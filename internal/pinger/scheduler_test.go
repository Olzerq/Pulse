package pinger

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
)

func TestSchedulerRespectsWorkerLimit(t *testing.T) {
	t.Parallel()

	items := make([]monitor.Monitor, 5)
	for index := range items {
		items[index] = monitor.Monitor{
			ID:                 string(rune('a' + index)),
			IntervalSeconds:    5,
			TimeoutMS:          1000,
			ExpectedStatusCode: 200,
		}
	}

	checker := &blockingChecker{started: make(chan struct{}, len(items))}
	publisher := &recordingPublisher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scheduler := NewScheduler(
		staticMonitorSource{items: items}, checker, publisher, newMemoryLocker(), logger,
		testSchedulerConfig("pinger-a", 2),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(ctx)
	}()

	for range 2 {
		select {
		case <-checker.started:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("workers did not start in time")
		}
	}

	select {
	case <-checker.started:
		cancel()
		t.Fatal("a third check started while both workers were busy")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop in time")
	}

	if got := checker.maximum.Load(); got != 2 {
		t.Errorf("maximum concurrent checks = %d, want 2", got)
	}
}

func TestSchedulerPublishesCheckResult(t *testing.T) {
	t.Parallel()

	item := monitor.Monitor{
		ID:                 "monitor-id",
		IntervalSeconds:    5,
		TimeoutMS:          1000,
		ExpectedStatusCode: 200,
	}
	want := check.Result{
		MonitorID:  item.ID,
		CheckedAt:  time.Now().UTC(),
		Success:    true,
		StatusCode: 200,
		LatencyMS:  42,
	}
	publisher := &recordingPublisher{published: make(chan check.Result, 1)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scheduler := NewScheduler(
		staticMonitorSource{items: []monitor.Monitor{item}},
		fixedChecker{result: want},
		publisher,
		newMemoryLocker(),
		logger,
		testSchedulerConfig("pinger-a", 1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	select {
	case got := <-publisher.published:
		if got != want {
			t.Errorf("published result = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("check result was not published in time")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop in time")
	}
}

func TestDistributedLockPreventsDuplicateCheck(t *testing.T) {
	t.Parallel()

	item := monitor.Monitor{
		ID:                 "monitor-id",
		IntervalSeconds:    5,
		TimeoutMS:          1000,
		ExpectedStatusCode: 200,
	}
	checker := &signalingChecker{started: make(chan struct{}, 2)}
	locker := newMemoryLocker()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first := NewScheduler(
		staticMonitorSource{items: []monitor.Monitor{item}}, checker, &recordingPublisher{}, locker, logger,
		testSchedulerConfig("pinger-a", 1),
	)
	second := NewScheduler(
		staticMonitorSource{items: []monitor.Monitor{item}}, checker, &recordingPublisher{}, locker, logger,
		testSchedulerConfig("pinger-b", 1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { done <- first.Run(ctx) }()
	go func() { done <- second.Run(ctx) }()

	select {
	case <-checker.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("no check started")
	}
	select {
	case <-checker.started:
		cancel()
		t.Fatal("both Pinger instances checked the same monitor")
	case <-time.After(150 * time.Millisecond):
	}

	cancel()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("scheduler did not stop in time")
		}
	}
}

type staticMonitorSource struct {
	items []monitor.Monitor
}

func (s staticMonitorSource) ListActive(context.Context) ([]monitor.Monitor, error) {
	return s.items, nil
}

type blockingChecker struct {
	started chan struct{}
	current atomic.Int32
	maximum atomic.Int32
}

type fixedChecker struct {
	result check.Result
}

type signalingChecker struct {
	started chan struct{}
}

func (c *signalingChecker) Check(_ context.Context, item monitor.Monitor) check.Result {
	c.started <- struct{}{}
	return check.Result{
		MonitorID:  item.ID,
		CheckedAt:  time.Now().UTC(),
		Success:    true,
		StatusCode: item.ExpectedStatusCode,
	}
}

func (c fixedChecker) Check(context.Context, monitor.Monitor) check.Result {
	return c.result
}

type recordingPublisher struct {
	published chan check.Result
}

func testSchedulerConfig(instanceID string, workerCount int) SchedulerConfig {
	return SchedulerConfig{
		InstanceID:           instanceID,
		PollInterval:         20 * time.Millisecond,
		WorkerCount:          workerCount,
		LockGrace:            time.Second,
		PublishTimeout:       time.Second,
		LockOperationTimeout: time.Second,
	}
}

type memoryLocker struct {
	mu     sync.Mutex
	owners map[string]string
}

func newMemoryLocker() *memoryLocker {
	return &memoryLocker{owners: make(map[string]string)}
}

func (l *memoryLocker) Acquire(
	_ context.Context,
	monitorID string,
	owner string,
	_ time.Duration,
) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.owners[monitorID]; exists {
		return false, nil
	}
	l.owners[monitorID] = owner
	return true, nil
}

func (l *memoryLocker) Complete(
	_ context.Context,
	monitorID string,
	owner string,
	holdFor time.Duration,
) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owners[monitorID] != owner {
		return false, nil
	}
	if holdFor <= 0 {
		delete(l.owners, monitorID)
	}
	return true, nil
}

func (p *recordingPublisher) Publish(_ context.Context, result check.Result) (event.CheckResult, error) {
	if p.published != nil {
		p.published <- result
	}
	return event.NewCheckResult("event-id", result), nil
}

func (c *blockingChecker) Check(ctx context.Context, item monitor.Monitor) check.Result {
	current := c.current.Add(1)
	defer c.current.Add(-1)

	for {
		maximum := c.maximum.Load()
		if current <= maximum || c.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	c.started <- struct{}{}

	<-ctx.Done()
	return check.Result{
		MonitorID: item.ID,
		CheckedAt: time.Now().UTC(),
		ErrorKind: check.FailureCanceled,
		Error:     ctx.Err().Error(),
	}
}
