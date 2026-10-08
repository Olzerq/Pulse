//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
)

func TestMonitoringFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	apiURL := strings.TrimRight(e2eEnvironment("PULSE_E2E_API_URL", "http://localhost:8080"), "/")
	client := &http.Client{Timeout: 5 * time.Second}
	if err := waitForAPI(ctx, client, apiURL); err != nil {
		t.Fatalf("wait for API: %v", err)
	}

	pool, err := postgres.Open(ctx, e2eEnvironment(
		"PULSE_E2E_POSTGRES_URL",
		"postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable",
	))
	if err != nil {
		t.Fatalf("open PostgreSQL for cleanup: %v", err)
	}
	t.Cleanup(pool.Close)

	redisClient := goredis.NewClient(&goredis.Options{
		Addr: e2eEnvironment("PULSE_E2E_REDIS_ADDR", "localhost:6379"),
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to Redis for cleanup: %v", err)
	}
	t.Cleanup(func() { _ = redisClient.Close() })

	enabled := true
	name := "E2E " + uuid.NewString()
	request := monitor.CreateParams{
		Name:               name,
		URL:                e2eEnvironment("PULSE_E2E_TARGET_URL", "http://api:8080/healthz"),
		Method:             "GET",
		IntervalSeconds:    5,
		TimeoutMS:          2000,
		Enabled:            &enabled,
		ExpectedStatusCode: http.StatusOK,
	}
	var created struct {
		Monitor monitor.Monitor `json:"monitor"`
	}
	if err := doJSON(ctx, client, http.MethodPost, apiURL+"/api/v1/monitors", request, http.StatusCreated, &created); err != nil {
		t.Fatalf("create monitor: %v", err)
	}
	if _, err := uuid.Parse(created.Monitor.ID); err != nil {
		t.Fatalf("created monitor ID %q is not a UUID: %v", created.Monitor.ID, err)
	}
	monitorID := created.Monitor.ID
	t.Cleanup(func() {
		cleanupE2EMonitor(t, client, apiURL, pool, redisClient, monitorID)
	})

	var state monitorstate.State
	var checks []event.CheckResult
	if err := waitForResult(ctx, client, apiURL, monitorID, &state, &checks); err != nil {
		t.Fatalf("wait for monitoring result: %v", err)
	}
	if state.Status != monitorstate.StatusUp || state.StatusCode == nil || *state.StatusCode != http.StatusOK {
		t.Fatalf("current state = %#v, want UP with HTTP 200", state)
	}
	if state.CheckedAt == nil || state.LatencyMS == nil {
		t.Fatalf("current state does not contain check time and latency: %#v", state)
	}
	if len(checks) == 0 || !checks[0].Success || checks[0].MonitorID != monitorID {
		t.Fatalf("check history = %#v, want a successful check for %s", checks, monitorID)
	}

	detailResponse, err := client.Get(apiURL + "/monitors/" + monitorID)
	if err != nil {
		t.Fatalf("open monitor page: %v", err)
	}
	detailBody, readErr := io.ReadAll(io.LimitReader(detailResponse.Body, 2<<20))
	_ = detailResponse.Body.Close()
	if readErr != nil {
		t.Fatalf("read monitor page: %v", readErr)
	}
	if detailResponse.StatusCode != http.StatusOK {
		t.Fatalf("monitor page status = %d, want 200", detailResponse.StatusCode)
	}
	if !bytes.Contains(detailBody, []byte(name)) || !bytes.Contains(detailBody, []byte("Работает")) {
		t.Fatalf("monitor page does not contain the E2E monitor name and UP status")
	}
}

func waitForAPI(ctx context.Context, client *http.Client, apiURL string) error {
	return poll(ctx, 250*time.Millisecond, func() (bool, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/readyz", nil)
		if err != nil {
			return false, err
		}
		response, err := client.Do(request)
		if err != nil {
			return false, nil
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK, nil
	})
}

func waitForResult(
	ctx context.Context,
	client *http.Client,
	apiURL string,
	monitorID string,
	state *monitorstate.State,
	checks *[]event.CheckResult,
) error {
	return poll(ctx, 500*time.Millisecond, func() (bool, error) {
		var stateEnvelope struct {
			State monitorstate.State `json:"state"`
		}
		if err := doJSON(ctx, client, http.MethodGet, apiURL+"/api/v1/monitors/"+monitorID+"/status", nil, http.StatusOK, &stateEnvelope); err != nil {
			return false, nil
		}
		var historyEnvelope struct {
			Checks []event.CheckResult `json:"checks"`
		}
		if err := doJSON(ctx, client, http.MethodGet, apiURL+"/api/v1/monitors/"+monitorID+"/checks?limit=10", nil, http.StatusOK, &historyEnvelope); err != nil {
			return false, nil
		}
		*state = stateEnvelope.State
		*checks = historyEnvelope.Checks
		return stateEnvelope.State.Status == monitorstate.StatusUp && len(historyEnvelope.Checks) > 0, nil
	})
}

func poll(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		complete, err := check()
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func doJSON(
	ctx context.Context,
	client *http.Client,
	method string,
	requestURL string,
	requestBody any,
	wantStatus int,
	responseBody any,
) error {
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return fmt.Errorf("response status %d, want %d: %s", response.StatusCode, wantStatus, payload)
	}
	if responseBody == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func cleanupE2EMonitor(
	t *testing.T,
	client *http.Client,
	apiURL string,
	pool *pgxpool.Pool,
	redisClient *goredis.Client,
	monitorID string,
) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	disabled := false
	_ = doJSON(cleanupCtx, client, http.MethodPatch, apiURL+"/api/v1/monitors/"+monitorID, monitor.Patch{
		Enabled: &disabled,
	}, http.StatusOK, &struct {
		Monitor monitor.Monitor `json:"monitor"`
	}{})
	time.Sleep(2 * time.Second)
	_ = doJSON(cleanupCtx, client, http.MethodDelete, apiURL+"/api/v1/monitors/"+monitorID, nil, http.StatusNoContent, nil)

	for attempt := 0; attempt < 4; attempt++ {
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM checks WHERE monitor_id = $1`, monitorID); err != nil {
			t.Logf("cleanup checks for %s: %v", monitorID, err)
		}
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM status_transitions WHERE monitor_id = $1`, monitorID); err != nil {
			t.Logf("cleanup transitions for %s: %v", monitorID, err)
		}
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM monitors WHERE id = $1`, monitorID); err != nil {
			t.Logf("cleanup monitor %s: %v", monitorID, err)
		}
		if err := redisClient.Del(
			cleanupCtx,
			"monitor:"+monitorID+":status",
			"monitor:"+monitorID+":lock",
		).Err(); err != nil {
			t.Logf("cleanup Redis keys for %s: %v", monitorID, err)
		}
		if attempt < 3 {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func e2eEnvironment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
