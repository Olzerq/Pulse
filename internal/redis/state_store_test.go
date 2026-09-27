package redis

import (
	"context"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/monitorstate"
)

func TestStateStoreRoundTrip(t *testing.T) {
	t.Parallel()

	client := &fakeKeyValueClient{values: make(map[string]string)}
	store := NewStateStore(client)
	checkedAt := time.Now().UTC()
	latencyMS := int64(42)
	statusCode := 200
	want := monitorstate.State{
		MonitorID:  "monitor-id",
		Status:     monitorstate.StatusUp,
		CheckedAt:  &checkedAt,
		StatusCode: &statusCode,
		LatencyMS:  &latencyMS,
	}

	if err := store.Set(context.Background(), want); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	got, err := store.Get(context.Background(), want.MonitorID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.MonitorID != want.MonitorID || got.Status != want.Status {
		t.Errorf("Get() = %#v, want identifiers and status from %#v", got, want)
	}
	if got.LatencyMS == nil || *got.LatencyMS != latencyMS {
		t.Errorf("LatencyMS = %v, want %d", got.LatencyMS, latencyMS)
	}
}

func TestStateStoreMissingKeyReturnsUnknown(t *testing.T) {
	t.Parallel()

	store := NewStateStore(&fakeKeyValueClient{getErr: errKeyNotFound})
	state, err := store.Get(context.Background(), "monitor-id")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.Status != monitorstate.StatusUnknown || state.MonitorID != "monitor-id" {
		t.Errorf("Get() = %#v, want UNKNOWN for monitor-id", state)
	}
}

func TestStateStoreRejectsCorruptValue(t *testing.T) {
	t.Parallel()

	client := &fakeKeyValueClient{values: map[string]string{
		stateKey("monitor-id"): `{"monitor_id":"another","status":"UP"}`,
	}}
	store := NewStateStore(client)
	if _, err := store.Get(context.Background(), "monitor-id"); err == nil {
		t.Fatal("Get() error = nil, want validation error")
	}
}

func TestStateStoreReadsStageSixValue(t *testing.T) {
	t.Parallel()

	client := &fakeKeyValueClient{values: map[string]string{
		stateKey("monitor-id"): `{"monitor_id":"monitor-id","status":"UP","checked_at":"2026-09-11T12:00:00Z","status_code":200,"latency_ms":42,"error":null}`,
	}}
	store := NewStateStore(client)
	state, err := store.Get(context.Background(), "monitor-id")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if state.LastStatusChangeAt == nil || !state.LastStatusChangeAt.Equal(*state.CheckedAt) {
		t.Errorf("LastStatusChangeAt = %v, want checked_at %v", state.LastStatusChangeAt, state.CheckedAt)
	}
}

type fakeKeyValueClient struct {
	values map[string]string
	getErr error
	setErr error
}

func (c *fakeKeyValueClient) Set(_ context.Context, key, value string) error {
	if c.setErr != nil {
		return c.setErr
	}
	if c.values == nil {
		c.values = make(map[string]string)
	}
	c.values[key] = value
	return nil
}

func (c *fakeKeyValueClient) Get(_ context.Context, key string) (string, error) {
	if c.getErr != nil {
		return "", c.getErr
	}
	value, ok := c.values[key]
	if !ok {
		return "", errKeyNotFound
	}
	return value, nil
}
