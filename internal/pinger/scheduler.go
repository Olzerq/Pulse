package pinger

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
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

// Scheduler loads active monitors, tracks their next run in memory, and sends
// due work to a bounded pool. Distributed coordination is added in Stage 8.
type Scheduler struct {
	source       monitorSource
	checker      httpChecker
	publisher    resultPublisher
	logger       *slog.Logger
	pollInterval time.Duration
	workerCount  int
}

func NewScheduler(
	source monitorSource,
	checker httpChecker,
	publisher resultPublisher,
	logger *slog.Logger,
	pollInterval time.Duration,
	workerCount int,
) *Scheduler {
	return &Scheduler{
		source:       source,
		checker:      checker,
		publisher:    publisher,
		logger:       logger,
		pollInterval: pollInterval,
		workerCount:  workerCount,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	jobs := make(chan monitor.Monitor)
	completed := make(chan string, s.workerCount)

	var workers sync.WaitGroup
	for workerID := 1; workerID <= s.workerCount; workerID++ {
		workers.Add(1)
		go s.runWorker(ctx, workerID, jobs, completed, &workers)
	}

	ticker := time.NewTicker(s.pollInterval)
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
		case monitorID := <-completed:
			delete(inFlight, monitorID)
		case now := <-ticker.C:
			load(now)
		}
	}
}

type scheduleEntry struct {
	nextRun  time.Time
	interval time.Duration
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
	completed chan<- string,
	workers *sync.WaitGroup,
) {
	defer workers.Done()

	for item := range jobs {
		result := s.checker.Check(ctx, item)
		published, err := s.publisher.Publish(ctx, result)
		if err != nil {
			s.logPublishError(ctx, workerID, published.EventID, result, err)
		} else {
			s.logResult(workerID, published.EventID, result)
		}

		select {
		case completed <- item.ID:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) logResult(workerID int, eventID string, result check.Result) {
	attributes := []any{
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
