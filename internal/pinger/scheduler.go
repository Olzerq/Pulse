package pinger

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/observability"
)

const monitorLoadTimeout = 5 * time.Second

type monitorSource interface {
	ListActive(context.Context) ([]monitor.Monitor, error)
}

type httpChecker interface {
	Check(context.Context, monitor.Monitor) check.Result
}

type resultPublisher interface {
	Publish(context.Context, check.Result) (event.CheckResult, error)
}

type monitorLocker interface {
	Acquire(context.Context, string, string, time.Duration) (bool, error)
	Complete(context.Context, string, string, time.Duration) (bool, error)
}

type stateCounter interface {
	CountByStatus(context.Context, []string, monitorstate.Status) (int, error)
}

type SchedulerConfig struct {
	InstanceID           string
	PollInterval         time.Duration
	WorkerCount          int
	LockGrace            time.Duration
	PublishTimeout       time.Duration
	LockOperationTimeout time.Duration
	StateCounter         stateCounter
	Metrics              *observability.Metrics
}

// Scheduler loads active monitors, tracks their next run in memory, and sends
// due work to a bounded pool. Redis leases coordinate separate instances.
type Scheduler struct {
	source    monitorSource
	checker   httpChecker
	publisher resultPublisher
	locker    monitorLocker
	logger    *slog.Logger
	config    SchedulerConfig
}

func NewScheduler(
	source monitorSource,
	checker httpChecker,
	publisher resultPublisher,
	locker monitorLocker,
	logger *slog.Logger,
	config SchedulerConfig,
) *Scheduler {
	return &Scheduler{
		source:    source,
		checker:   checker,
		publisher: publisher,
		locker:    locker,
		logger:    logger,
		config:    config,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	jobs := make(chan monitor.Monitor)
	completed := make(chan completedJob, s.config.WorkerCount)

	var workers sync.WaitGroup
	for workerID := 1; workerID <= s.config.WorkerCount; workerID++ {
		workers.Add(1)
		go s.runWorker(ctx, workerID, jobs, completed, &workers)
	}

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	schedule := make(map[string]scheduleEntry)
	inFlight := make(map[string]struct{})
	loadFailed := false

	load := func(now time.Time) {
		loadCtx, cancel := context.WithTimeout(ctx, monitorLoadTimeout)
		items, err := s.source.ListActive(loadCtx)
		cancel()
		if err != nil {
			if !loadFailed && ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "load active monitors", "error", err)
			}
			loadFailed = true
			return
		}
		if loadFailed {
			s.logger.InfoContext(ctx, "active monitor loading recovered")
			loadFailed = false
		}
		if s.config.Metrics != nil {
			s.config.Metrics.SetMonitorsTotal(len(items))
			if s.config.StateCounter != nil {
				monitorIDs := make([]string, len(items))
				for index, item := range items {
					monitorIDs[index] = item.ID
				}
				metricsCtx, metricsCancel := context.WithTimeout(ctx, s.config.LockOperationTimeout)
				down, countErr := s.config.StateCounter.CountByStatus(
					metricsCtx,
					monitorIDs,
					monitorstate.StatusDown,
				)
				metricsCancel()
				if countErr != nil {
					s.logger.WarnContext(ctx, "count DOWN monitors for metrics", "error", countErr)
				} else {
					s.config.Metrics.SetMonitorsDown(down)
				}
			}
		}

		s.dispatchDue(now, items, schedule, inFlight, jobs)
	}

	load(time.Now())

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			s.logger.Info("pinger shutting down")
			return nil
		case completion := <-completed:
			delete(inFlight, completion.monitorID)
			if completion.retrySoon {
				entry, exists := schedule[completion.monitorID]
				if exists {
					entry.nextRun = time.Now().Add(s.config.PollInterval)
					schedule[completion.monitorID] = entry
				}
			}
		case now := <-ticker.C:
			load(now)
		}
	}
}

type scheduleEntry struct {
	nextRun  time.Time
	interval time.Duration
}

type completedJob struct {
	monitorID string
	retrySoon bool
}

func (s *Scheduler) dispatchDue(
	now time.Time,
	items []monitor.Monitor,
	schedule map[string]scheduleEntry,
	inFlight map[string]struct{},
	jobs chan<- monitor.Monitor,
) {
	active := make(map[string]struct{}, len(items))

	for _, item := range items {
		active[item.ID] = struct{}{}
		interval := time.Duration(item.IntervalSeconds) * time.Second
		entry, exists := schedule[item.ID]
		if !exists || entry.interval != interval {
			entry = scheduleEntry{nextRun: now, interval: interval}
			schedule[item.ID] = entry
		}

		if _, running := inFlight[item.ID]; running || now.Before(entry.nextRun) {
			continue
		}

		select {
		case jobs <- item:
			inFlight[item.ID] = struct{}{}
			entry.nextRun = now.Add(interval)
			schedule[item.ID] = entry
		default:
			// Every worker is busy. Keep nextRun unchanged so this monitor is
			// retried during the next polling cycle rather than queued without a
			// bound.
		}
	}

	for monitorID := range schedule {
		if _, exists := active[monitorID]; !exists {
			delete(schedule, monitorID)
		}
	}
}

func (s *Scheduler) runWorker(
	ctx context.Context,
	workerID int,
	jobs <-chan monitor.Monitor,
	completed chan<- completedJob,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	ownerID := fmt.Sprintf("%s:%d", s.config.InstanceID, workerID)

	for item := range jobs {
		lockTTL := time.Duration(item.TimeoutMS)*time.Millisecond +
			s.config.PublishTimeout + s.config.LockGrace
		lockCtx, cancel := context.WithTimeout(ctx, s.config.LockOperationTimeout)
		acquired, err := s.locker.Acquire(lockCtx, item.ID, ownerID, lockTTL)
		cancel()
		if err != nil {
			if s.config.Metrics != nil {
				s.config.Metrics.ObserveLock("error")
			}
			s.logger.ErrorContext(ctx, "acquire monitor lock",
				"instance_id", s.config.InstanceID,
				"worker_id", workerID,
				"monitor_id", item.ID,
				"lock_ttl", lockTTL,
				"error", err,
			)
			s.complete(ctx, completed, item.ID, true)
			continue
		}
		if !acquired {
			if s.config.Metrics != nil {
				s.config.Metrics.ObserveLock("contended")
			}
			s.logger.DebugContext(ctx, "monitor check owned by another pinger",
				"instance_id", s.config.InstanceID,
				"worker_id", workerID,
				"monitor_id", item.ID,
			)
			s.complete(ctx, completed, item.ID, true)
			continue
		}
		if s.config.Metrics != nil {
			s.config.Metrics.ObserveLock("acquired")
		}
		acquiredAt := time.Now()

		result := s.checker.Check(ctx, item)
		if s.config.Metrics != nil {
			s.config.Metrics.ObserveCheck(
				result.Success,
				string(result.ErrorKind),
				time.Duration(result.LatencyMS)*time.Millisecond,
			)
		}
		published, err := s.publisher.Publish(ctx, result)
		if err != nil {
			if s.config.Metrics != nil {
				s.config.Metrics.ObserveKafka("published", "error")
			}
			s.logPublishError(ctx, workerID, published.EventID, result, err)
		} else {
			if s.config.Metrics != nil {
				s.config.Metrics.ObserveKafka("published", "success")
			}
			s.logResult(workerID, published.EventID, result)
		}

		holdFor := time.Duration(item.IntervalSeconds)*time.Second - time.Since(acquiredAt)
		if ctx.Err() != nil {
			holdFor = 0
		}
		completeCtx, completeCancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			s.config.LockOperationTimeout,
		)
		completedLock, completeErr := s.locker.Complete(completeCtx, item.ID, ownerID, holdFor)
		completeCancel()
		if completeErr != nil {
			s.logger.Warn("complete monitor lock",
				"instance_id", s.config.InstanceID,
				"worker_id", workerID,
				"monitor_id", item.ID,
				"hold_for", holdFor,
				"error", completeErr,
			)
		} else if !completedLock {
			s.logger.Debug("monitor lock already expired",
				"instance_id", s.config.InstanceID,
				"worker_id", workerID,
				"monitor_id", item.ID,
			)
		}

		s.complete(ctx, completed, item.ID, false)
	}
}

func (s *Scheduler) complete(
	ctx context.Context,
	completed chan<- completedJob,
	monitorID string,
	retrySoon bool,
) {
	select {
	case completed <- completedJob{monitorID: monitorID, retrySoon: retrySoon}:
	case <-ctx.Done():
	}
}

func (s *Scheduler) logResult(workerID int, eventID string, result check.Result) {
	attributes := []any{
		"instance_id", s.config.InstanceID,
		"worker_id", workerID,
		"event_id", eventID,
		"monitor_id", result.MonitorID,
		"checked_at", result.CheckedAt,
		"success", result.Success,
		"status_code", result.StatusCode,
		"latency_ms", result.LatencyMS,
	}
	if result.ErrorKind != check.FailureNone {
		attributes = append(attributes,
			"error_kind", result.ErrorKind,
			"error", result.Error,
		)
	}

	switch {
	case result.Success:
		s.logger.Info("check result published", attributes...)
	case result.ErrorKind == check.FailureCanceled:
		s.logger.Debug("check canceled", attributes...)
	default:
		s.logger.Warn("failed check result published", attributes...)
	}
}

func (s *Scheduler) logPublishError(
	ctx context.Context,
	workerID int,
	eventID string,
	result check.Result,
	err error,
) {
	attributes := []any{
		"instance_id", s.config.InstanceID,
		"worker_id", workerID,
		"event_id", eventID,
		"monitor_id", result.MonitorID,
		"checked_at", result.CheckedAt,
		"error", err,
	}
	if ctx.Err() != nil {
		s.logger.Debug("Kafka publish canceled", attributes...)
		return
	}
	s.logger.Error("publish check result", attributes...)
}
