package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/observability"
)

func TestDashboardRendersMonitorStateAndEscapesData(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, time.September, 10, 16, 30, 0, 0, time.UTC)
	latency := int64(42)
	store := &fakeMonitorStore{items: []monitor.Monitor{{
		ID:              testMonitorID,
		Name:            "Main <script>alert(1)</script>",
		URL:             "https://example.com/health",
		IntervalSeconds: 60,
		Enabled:         true,
	}}}
	states := &fakeStateStore{states: map[string]monitorstate.State{
		testMonitorID: {
			MonitorID: testMonitorID,
			Status:    monitorstate.StatusUp,
			CheckedAt: &checkedAt,
			LatencyMS: &latency,
		},
	}}
	recorder := requestWeb(t, http.MethodGet, "/", "", store, states, &fakeHistoryStore{})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, expected := range []string{"Main &lt;script&gt;alert(1)&lt;/script&gt;", "Работает", "42 мс", "https://example.com/health"} {
		if !strings.Contains(body, expected) {
			t.Errorf("dashboard does not contain %q", expected)
		}
	}
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Error("dashboard rendered an unescaped monitor name")
	}
}

func TestCreateMonitorFromWebForm(t *testing.T) {
	t.Parallel()

	store := &fakeMonitorStore{}
	form := url.Values{
		"name":                 {"Главная"},
		"url":                  {"https://example.com/health"},
		"method":               {"HEAD"},
		"interval_seconds":     {"30"},
		"timeout_ms":           {"2000"},
		"expected_status_code": {"204"},
		"enabled":              {"on"},
	}.Encode()
	recorder := requestWeb(t, http.MethodPost, "/monitors", form, store, &fakeStateStore{}, &fakeHistoryStore{})

	if recorder.Code != http.StatusSeeOther {
		t.Fatalf("status code = %d, want 303; body = %s", recorder.Code, recorder.Body.String())
	}
	if location := recorder.Header().Get("Location"); location != "/monitors/"+testMonitorID {
		t.Errorf("Location = %q, want monitor detail page", location)
	}
	if store.created.Name != "Главная" || store.created.Method != "HEAD" || !store.created.Enabled {
		t.Errorf("created monitor = %#v", store.created)
	}
	if store.created.IntervalSeconds != 30 || store.created.TimeoutMS != 2000 || store.created.ExpectedStatusCode != 204 {
		t.Errorf("created monitor settings = %#v", store.created)
	}
}

func TestCreateMonitorFormShowsValidationErrors(t *testing.T) {
	t.Parallel()

	form := url.Values{
		"name":                 {""},
		"url":                  {"not-a-url"},
		"method":               {"GET"},
		"interval_seconds":     {"abc"},
		"timeout_ms":           {"5000"},
		"expected_status_code": {"200"},
	}.Encode()
	recorder := requestWeb(t, http.MethodPost, "/monitors", form, &fakeMonitorStore{}, &fakeStateStore{}, &fakeHistoryStore{})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want 400; body = %s", recorder.Code, recorder.Body.String())
	}
	for _, expected := range []string{"Введите название", "Укажите полный адрес", "Введите целое число"} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Errorf("validation response does not contain %q", expected)
		}
	}
}

func TestMonitorDetailsRendersRecentHistory(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, time.September, 10, 16, 30, 0, 0, time.UTC)
	statusCode := http.StatusOK
	latency := int64(37)
	store := &fakeMonitorStore{item: monitor.Monitor{
		ID:                 testMonitorID,
		Name:               "API",
		URL:                "https://example.com/healthz",
		Method:             "GET",
		IntervalSeconds:    60,
		TimeoutMS:          5000,
		ExpectedStatusCode: 200,
		Enabled:            true,
	}}
	states := &fakeStateStore{state: monitorstate.State{
		MonitorID:  testMonitorID,
		Status:     monitorstate.StatusUp,
		CheckedAt:  &checkedAt,
		StatusCode: &statusCode,
		LatencyMS:  &latency,
	}}
	history := &fakeHistoryStore{checks: []event.CheckResult{{
		EventID:    "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
		MonitorID:  testMonitorID,
		CheckedAt:  checkedAt,
		Success:    true,
		StatusCode: http.StatusOK,
		LatencyMS:  latency,
	}}}
	recorder := requestWeb(t, http.MethodGet, "/monitors/"+testMonitorID, "", store, states, history)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}
	for _, expected := range []string{"API", "Последние проверки", "Успешно", "37 мс"} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Errorf("details response does not contain %q", expected)
		}
	}
	if history.monitorID != testMonitorID || history.limit != webHistoryLimit {
		t.Errorf("history query = (%q, %d), want (%q, %d)", history.monitorID, history.limit, testMonitorID, webHistoryLimit)
	}
}

func TestChecksAPIValidatesAndPassesLimit(t *testing.T) {
	t.Parallel()

	store := &fakeMonitorStore{item: monitor.Monitor{ID: testMonitorID}}
	history := &fakeHistoryStore{checks: []event.CheckResult{}}
	recorder := requestWeb(t, http.MethodGet, "/api/v1/monitors/"+testMonitorID+"/checks?limit=12", "", store, &fakeStateStore{}, history)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}
	if history.limit != 12 {
		t.Errorf("history limit = %d, want 12", history.limit)
	}

	recorder = requestWeb(t, http.MethodGet, "/api/v1/monitors/"+testMonitorID+"/checks?limit=201", "", store, &fakeStateStore{}, history)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit status code = %d, want 400", recorder.Code)
	}
}

func requestWeb(
	t *testing.T,
	method string,
	path string,
	body string,
	monitors monitorStore,
	states stateStore,
	history historyStore,
) *httptest.ResponseRecorder {
	t.Helper()

	metrics := observability.NewMetrics("test")
	probes := observability.NewHandler(metrics, nil, time.Second)
	router := newRouter(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		monitors,
		states,
		history,
		metrics,
		probes,
	)
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}
