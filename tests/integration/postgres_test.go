//go:build integration

package integration_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/postgres"
	"github.com/google/uuid"
)

func TestPostgresMonitorAndCheckStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, environment(
		"PULSE_INTEGRATION_POSTGRES_URL",
		"postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable",
	))
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	monitorStore := postgres.NewMonitorStore(pool)
	historyStore := postgres.NewCheckStore(pool)
	item, err := monitor.New(monitor.CreateParams{
		Name:            "Integration " + uuid.NewString(),
		URL:             "https://example.com/healthz",
		IntervalSeconds: 30,
		TimeoutMS:       2000,
	})
	if err != nil {
		t.Fatalf("construct monitor: %v", err)
	}
	created, err := monitorStore.Create(ctx, item)
	if err != nil {
		t.Fatalf("create monitor: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM checks WHERE monitor_id = $1`, created.ID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM status_transitions WHERE monitor_id = $1`, created.ID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM monitors WHERE id = $1`, created.ID)
	})

	loaded, err := monitorStore.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get monitor: %v", err)
	}
	if loaded.Name != item.Name || loaded.URL != item.URL || !loaded.Enabled {
		t.Fatalf("loaded monitor = %#v, want created values", loaded)
	}

	newName := "Updated " + uuid.NewString()
	loaded.Name = newName
	updated, err := monitorStore.Update(ctx, loaded)
	if err != nil {
		t.Fatalf("update monitor: %v", err)
	}
	if updated.Name != newName || updated.UpdatedAt.Before(created.UpdatedAt) {
		t.Errorf("updated monitor = %#v", updated)
	}

	items, err := monitorStore.List(ctx)
	if err != nil {
		t.Fatalf("list monitors: %v", err)
	}
	if !containsMonitor(items, created.ID) {
		t.Fatalf("created monitor %s is absent from List()", created.ID)
	}

	checkedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	first := event.NewCheckResult(uuid.NewString(), check.Result{
		MonitorID:  created.ID,
		CheckedAt:  checkedAt,
		Success:    true,
		StatusCode: 200,
		LatencyMS:  41,
	})
	inserted, err := historyStore.Insert(ctx, first)
	if err != nil || !inserted {
		t.Fatalf("insert first check = %v, %v; want true, nil", inserted, err)
	}
	inserted, err = historyStore.Insert(ctx, first)
	if err != nil || inserted {
		t.Fatalf("insert duplicate check = %v, %v; want false, nil", inserted, err)
	}

	second := event.NewCheckResult(uuid.NewString(), check.Result{
		MonitorID:  created.ID,
		CheckedAt:  checkedAt.Add(time.Second),
		Success:    true,
		StatusCode: 204,
		LatencyMS:  19,
	})
	if inserted, err := historyStore.Insert(ctx, second); err != nil || !inserted {
		t.Fatalf("insert second check = %v, %v; want true, nil", inserted, err)
	}

	recent, err := historyStore.ListRecent(ctx, created.ID, 1)
	if err != nil {
		t.Fatalf("list recent checks: %v", err)
	}
	if len(recent) != 1 || recent[0].EventID != second.EventID || recent[0].StatusCode != 204 {
		t.Fatalf("recent checks = %#v, want newest event %s", recent, second.EventID)
	}

	if err := monitorStore.Delete(ctx, created.ID); err != nil {
		t.Fatalf("delete monitor: %v", err)
	}
	if _, err := monitorStore.Get(ctx, created.ID); !errors.Is(err, monitor.ErrNotFound) {
		t.Fatalf("get deleted monitor error = %v, want monitor.ErrNotFound", err)
	}
}

func containsMonitor(items []monitor.Monitor, monitorID string) bool {
	for _, item := range items {
		if item.ID == monitorID {
			return true
		}
	}
	return false
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
