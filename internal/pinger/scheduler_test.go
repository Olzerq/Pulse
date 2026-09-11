package pinger

import (
	"context"
	"io"
	"log/slog"
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
	scheduler := NewScheduler(staticMonitorSource{items: items}, checker, publisher, logger, 20*time.Millisecond, 2)

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
		logger,
		20*time.Millisecond,
		1,
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

func (c fixedChecker) Check(context.Context, monitor.Monitor) check.Result {
	return c.result
}

type recordingPublisher struct {
	published chan check.Result
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
