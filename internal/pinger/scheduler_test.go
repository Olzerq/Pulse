package pinger

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scheduler := NewScheduler(staticMonitorSource{items: items}, checker, logger, 20*time.Millisecond, 2)

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
