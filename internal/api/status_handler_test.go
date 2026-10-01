package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/observability"
)

const testMonitorID = "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748"

func TestGetMonitorStatus(t *testing.T) {
	t.Parallel()

	checkedAt := time.Now().UTC()
	statusCode := http.StatusOK
	latencyMS := int64(42)
	monitorStore := &fakeMonitorStore{item: monitor.Monitor{ID: testMonitorID}}
	states := &fakeStateStore{state: monitorstate.State{
		MonitorID:  testMonitorID,
		Status:     monitorstate.StatusUp,
		CheckedAt:  &checkedAt,
		StatusCode: &statusCode,
		LatencyMS:  &latencyMS,
	}}
	recorder := requestStatus(t, monitorStore, states)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		State monitorstate.State `json:"state"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.State.Status != monitorstate.StatusUp || response.State.MonitorID != testMonitorID {
		t.Errorf("state = %#v, want UP for %s", response.State, testMonitorID)
	}
}

func TestGetMonitorStatusReturnsUnknown(t *testing.T) {
	t.Parallel()

	monitorStore := &fakeMonitorStore{item: monitor.Monitor{ID: testMonitorID}}
	states := &fakeStateStore{state: monitorstate.Unknown(testMonitorID)}
	recorder := requestStatus(t, monitorStore, states)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		State monitorstate.State `json:"state"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.State.Status != monitorstate.StatusUnknown || response.State.CheckedAt != nil {
		t.Errorf("state = %#v, want UNKNOWN without check data", response.State)
	}
}

func TestGetMonitorStatusChecksMonitorExists(t *testing.T) {
	t.Parallel()

	monitorStore := &fakeMonitorStore{getErr: monitor.ErrNotFound}
	states := &fakeStateStore{state: monitorstate.Unknown(testMonitorID)}
	recorder := requestStatus(t, monitorStore, states)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
	if states.calls != 0 {
		t.Errorf("state store calls = %d, want 0", states.calls)
	}
}

func TestGetMonitorStatusHandlesRedisFailure(t *testing.T) {
	t.Parallel()

	monitorStore := &fakeMonitorStore{item: monitor.Monitor{ID: testMonitorID}}
	states := &fakeStateStore{err: errors.New("Redis unavailable")}
	recorder := requestStatus(t, monitorStore, states)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
	}
}

func requestStatus(t *testing.T, monitors monitorStore, states stateStore) *httptest.ResponseRecorder {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics("test")
	probes := observability.NewHandler(metrics, nil, time.Second)
	router := newRouter(logger, monitors, states, metrics, probes)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+testMonitorID+"/status", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

type fakeMonitorStore struct {
	item   monitor.Monitor
	getErr error
}

func (s *fakeMonitorStore) List(context.Context) ([]monitor.Monitor, error) {
	return nil, nil
}

func (s *fakeMonitorStore) Get(context.Context, string) (monitor.Monitor, error) {
	return s.item, s.getErr
}

func (s *fakeMonitorStore) Create(_ context.Context, item monitor.Monitor) (monitor.Monitor, error) {
	return item, nil
}

func (s *fakeMonitorStore) Update(_ context.Context, item monitor.Monitor) (monitor.Monitor, error) {
	return item, nil
}

func (s *fakeMonitorStore) Delete(context.Context, string) error {
	return nil
}

type fakeStateStore struct {
	state monitorstate.State
	err   error
	calls int
}

func (s *fakeStateStore) Get(context.Context, string) (monitorstate.State, error) {
	s.calls++
	return s.state, s.err
}
