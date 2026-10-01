package api

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/Olzerq/Pulse/internal/observability"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func newRouter(
	logger *slog.Logger,
	store monitorStore,
	states stateStore,
	history historyStore,
	metrics *observability.Metrics,
	observabilityHandler http.Handler,
) http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(requestObserver(logger, metrics))
	router.Use(recoverer(logger))

	handler := newMonitorHandler(logger, store, states, history)
	web := newWebHandler(logger, store, states, history)

	router.Handle("/metrics", observabilityHandler)
	router.Handle("/healthz", observabilityHandler)
	router.Handle("/livez", observabilityHandler)
	router.Handle("/readyz", observabilityHandler)

	router.Get("/api/v1/monitors", handler.list)
	router.Post("/api/v1/monitors", handler.create)
	router.Get("/api/v1/monitors/{monitorID}", handler.get)
	router.Get("/api/v1/monitors/{monitorID}/status", handler.getStatus)
	router.Get("/api/v1/monitors/{monitorID}/checks", handler.getChecks)
	router.Patch("/api/v1/monitors/{monitorID}", handler.update)
	router.Delete("/api/v1/monitors/{monitorID}", handler.delete)

	router.Handle("/static/*", web.static())
	router.Get("/", web.dashboard)
	router.Get("/monitors/new", web.newMonitor)
	router.Post("/monitors", web.createMonitor)
	router.Get("/monitors/{monitorID}", web.monitorDetails)
	router.Post("/monitors/{monitorID}/toggle", web.toggleMonitor)
	router.Post("/monitors/{monitorID}/delete", web.deleteMonitor)

	router.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", nil)
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
	})

	return router
}

func requestObserver(logger *slog.Logger, metrics *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(wrapped, r)

			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			duration := time.Since(startedAt)
			metrics.ObserveHTTP(r.Method, route, wrapped.Status(), duration)
			attributes := []any{
				"request_id", middleware.GetReqID(r.Context()),
				"method", r.Method,
				"route", route,
				"status", wrapped.Status(),
				"bytes", wrapped.BytesWritten(),
				"duration_ms", duration.Milliseconds(),
			}
			if route == "/metrics" || route == "/healthz" || route == "/livez" || route == "/readyz" {
				logger.DebugContext(r.Context(), "http probe completed", attributes...)
			} else {
				logger.InfoContext(r.Context(), "http request completed", attributes...)
			}
		})
	}
}

func recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.ErrorContext(r.Context(), "panic while handling request",
						"request_id", middleware.GetReqID(r.Context()),
						"method", r.Method,
						"path", r.URL.Path,
						"panic", recovered,
						"stack", string(debug.Stack()),
					)
					writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
