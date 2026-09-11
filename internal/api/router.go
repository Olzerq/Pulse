package api

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func newRouter(logger *slog.Logger, store monitorStore) http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(recoverer(logger))

	handler := newMonitorHandler(logger, store)

	router.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	router.Get("/api/v1/monitors", handler.list)
	router.Post("/api/v1/monitors", handler.create)
	router.Get("/api/v1/monitors/{monitorID}", handler.get)
	router.Patch("/api/v1/monitors/{monitorID}", handler.update)
	router.Delete("/api/v1/monitors/{monitorID}", handler.delete)

	router.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", nil)
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
	})

	return router
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
