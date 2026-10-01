package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadinessHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		check      Check
		wantStatus int
	}{
		{
			name:       "ready",
			check:      func(context.Context) error { return nil },
			wantStatus: http.StatusOK,
		},
		{
			name:       "dependency unavailable",
			check:      func(context.Context) error { return errors.New("unavailable") },
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			handler := NewHandler(NewMetrics("test"), map[string]Check{"dependency": test.check}, time.Second)
			request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
		})
	}
}

func TestLivenessDoesNotRunDependencies(t *testing.T) {
	t.Parallel()

	called := false
	handler := NewHandler(NewMetrics("test"), map[string]Check{
		"dependency": func(context.Context) error {
			called = true
			return nil
		},
	}, time.Second)
	request := httptest.NewRequest(http.MethodGet, "/livez", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || called {
		t.Fatalf("status = %d, dependency called = %v", recorder.Code, called)
	}
}
