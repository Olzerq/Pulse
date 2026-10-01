package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/observability"
)

func TestObservabilityRoutes(t *testing.T) {
	t.Parallel()

	metrics := observability.NewMetrics("api")
	probes := observability.NewHandler(metrics, nil, time.Second)
	router := newRouter(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&fakeMonitorStore{},
		&fakeStateStore{},
		&fakeHistoryStore{},
		metrics,
		probes,
	)

	for _, path := range []string{"/healthz", "/livez", "/readyz", "/metrics"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200; body = %s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestHTTPMetricsUseRoutePattern(t *testing.T) {
	t.Parallel()

	metrics := observability.NewMetrics("api")
	probes := observability.NewHandler(metrics, nil, time.Second)
	router := newRouter(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&fakeMonitorStore{},
		&fakeStateStore{},
		&fakeHistoryStore{},
		metrics,
		probes,
	)

	request := httptest.NewRequest(http.MethodGet, "/api/v1/monitors/"+testMonitorID, nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRecorder := httptest.NewRecorder()
	router.ServeHTTP(metricsRecorder, metricsRequest)
	body := metricsRecorder.Body.String()
	if !strings.Contains(body, `route="/api/v1/monitors/{monitorID}"`) {
		t.Fatalf("metrics do not contain route template; body = %s", body)
	}
	if strings.Contains(body, testMonitorID) {
		t.Fatalf("metrics contain monitor ID high-cardinality value; body = %s", body)
	}
}
