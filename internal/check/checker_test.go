package check

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/monitor"
)

func TestCheckerSuccess(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "Pulse/test" {
			t.Errorf("User-Agent = %q, want %q", got, "Pulse/test")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	checker := NewChecker(1, "Pulse/test")
	defer checker.CloseIdleConnections()

	result := checker.Check(context.Background(), testMonitor(server.URL, http.StatusNoContent, 1000))
	if !result.Success {
		t.Fatalf("Success = false, error = %q", result.Error)
	}
	if result.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %d, want %d", result.StatusCode, http.StatusNoContent)
	}
	if result.ErrorKind != FailureNone {
		t.Errorf("ErrorKind = %q, want empty", result.ErrorKind)
	}
}

func TestCheckerUnexpectedStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	checker := NewChecker(1, "Pulse/test")
	defer checker.CloseIdleConnections()

	result := checker.Check(context.Background(), testMonitor(server.URL, http.StatusOK, 1000))
	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want %d", result.StatusCode, http.StatusNotFound)
	}
	if result.ErrorKind != FailureUnexpectedStatus {
		t.Errorf("ErrorKind = %q, want %q", result.ErrorKind, FailureUnexpectedStatus)
	}
}

func TestCheckerTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	checker := NewChecker(1, "Pulse/test")
	defer checker.CloseIdleConnections()

	result := checker.Check(context.Background(), testMonitor(server.URL, http.StatusOK, 10))
	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorKind != FailureTimeout {
		t.Errorf("ErrorKind = %q, want %q; error = %q", result.ErrorKind, FailureTimeout, result.Error)
	}
}

func TestCheckerNetworkError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	target := server.URL
	server.Close()

	checker := NewChecker(1, "Pulse/test")
	defer checker.CloseIdleConnections()

	result := checker.Check(context.Background(), testMonitor(target, http.StatusOK, 1000))
	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorKind != FailureNetwork {
		t.Errorf("ErrorKind = %q, want %q; error = %q", result.ErrorKind, FailureNetwork, result.Error)
	}
}

func testMonitor(target string, expectedStatus, timeoutMS int) monitor.Monitor {
	return monitor.Monitor{
		ID:                 "monitor-id",
		Name:               "test",
		URL:                target,
		Method:             http.MethodGet,
		IntervalSeconds:    5,
		TimeoutMS:          timeoutMS,
		Enabled:            true,
		ExpectedStatusCode: expectedStatus,
	}
}
