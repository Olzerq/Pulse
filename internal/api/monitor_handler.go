package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

const maxRequestBodyBytes = 1 << 20

var errUnsupportedMediaType = errors.New("Content-Type must be application/json")

type monitorStore interface {
	List(context.Context) ([]monitor.Monitor, error)
	Get(context.Context, string) (monitor.Monitor, error)
	Create(context.Context, monitor.Monitor) (monitor.Monitor, error)
	Update(context.Context, monitor.Monitor) (monitor.Monitor, error)
	Delete(context.Context, string) error
}

type monitorHandler struct {
	logger *slog.Logger
	store  monitorStore
}

func newMonitorHandler(logger *slog.Logger, store monitorStore) *monitorHandler {
	return &monitorHandler{logger: logger, store: store}
}

func (h *monitorHandler) list(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.List(r.Context())
	if err != nil {
		h.internalError(w, r, "list monitors", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"monitors": items})
}

func (h *monitorHandler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}

	item, err := h.store.Get(r.Context(), id)
	if h.handleStoreError(w, r, "get monitor", err) {
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"monitor": item})
}

func (h *monitorHandler) create(w http.ResponseWriter, r *http.Request) {
	var params monitor.CreateParams
	if err := decodeJSON(w, r, &params); err != nil {
		h.handleDecodeError(w, err)
		return
	}

	item, err := monitor.New(params)
	if err != nil {
		h.handleValidationError(w, err)
		return
	}

	created, err := h.store.Create(r.Context(), item)
	if err != nil {
		h.internalError(w, r, "create monitor", err)
		return
	}

	w.Header().Set("Location", "/api/v1/monitors/"+created.ID)
	writeJSON(w, http.StatusCreated, map[string]any{"monitor": created})
}

func (h *monitorHandler) update(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}

	var patch monitor.Patch
	if err := decodeJSON(w, r, &patch); err != nil {
		h.handleDecodeError(w, err)
		return
	}
	if patch.Empty() {
		writeError(w, http.StatusBadRequest, "validation_error", "request must contain at least one monitor field", nil)
		return
	}

	current, err := h.store.Get(r.Context(), id)
	if h.handleStoreError(w, r, "get monitor for update", err) {
		return
	}

	updated, err := patch.Apply(current)
	if err != nil {
		h.handleValidationError(w, err)
		return
	}

	updated, err = h.store.Update(r.Context(), updated)
	if h.handleStoreError(w, r, "update monitor", err) {
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"monitor": updated})
}

func (h *monitorHandler) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}

	err := h.store.Delete(r.Context(), id)
	if h.handleStoreError(w, r, "delete monitor", err) {
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *monitorHandler) handleDecodeError(w http.ResponseWriter, err error) {
	if errors.Is(err, errUnsupportedMediaType) {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), nil)
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_json", err.Error(), nil)
}

func (h *monitorHandler) handleValidationError(w http.ResponseWriter, err error) {
	var validationErr *monitor.ValidationError
	if errors.As(err, &validationErr) {
		writeError(w, http.StatusBadRequest, "validation_error", validationErr.Error(), validationErr.Fields)
		return
	}
	writeError(w, http.StatusBadRequest, "validation_error", err.Error(), nil)
}

// handleStoreError writes known storage errors and reports whether a response
// was written. A nil error means the caller can continue normally.
func (h *monitorHandler) handleStoreError(w http.ResponseWriter, r *http.Request, operation string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, monitor.ErrNotFound) {
		writeError(w, http.StatusNotFound, "monitor_not_found", "monitor not found", nil)
		return true
	}

	h.internalError(w, r, operation, err)
	return true
}

func (h *monitorHandler) internalError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	h.logger.ErrorContext(r.Context(), "request failed",
		"operation", operation,
		"request_id", middleware.GetReqID(r.Context()),
		"method", r.Method,
		"path", r.URL.Path,
		"error", err,
	)
	writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
}

func monitorID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "monitorID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_monitor_id", "monitor id must be a valid UUID", nil)
		return "", false
	}
	return id.String(), true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		return errUnsupportedMediaType
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("request body must contain valid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message string, fields map[string]string) {
	writeJSON(w, status, errorEnvelope{Error: apiError{
		Code:    code,
		Message: message,
		Fields:  fields,
	}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
