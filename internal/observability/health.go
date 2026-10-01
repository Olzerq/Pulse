package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Check func(context.Context) error

func NewHandler(metrics *Metrics, checks map[string]Check, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	ready := readinessHandler(checks, timeout)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/livez", liveHandler)
	mux.Handle("/readyz", ready)
	mux.Handle("/healthz", ready)
	return mux
}

func liveHandler(w http.ResponseWriter, _ *http.Request) {
	writeProbeJSON(w, http.StatusOK, probeResponse{Status: "ok"})
}

func readinessHandler(checks map[string]Check, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type result struct {
			name string
			err  error
		}

		results := make(chan result, len(checks))
		for name, check := range checks {
			go func(name string, check Check) {
				ctx, cancel := context.WithTimeout(r.Context(), timeout)
				defer cancel()
				results <- result{name: name, err: check(ctx)}
			}(name, check)
		}

		response := probeResponse{Status: "ok", Checks: make(map[string]string, len(checks))}
		status := http.StatusOK
		for range checks {
			item := <-results
			if item.err != nil {
				response.Status = "unavailable"
				response.Checks[item.name] = "unavailable"
				status = http.StatusServiceUnavailable
			} else {
				response.Checks[item.name] = "ok"
			}
		}
		writeProbeJSON(w, status, response)
	})
}

type probeResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func writeProbeJSON(w http.ResponseWriter, status int, response probeResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
