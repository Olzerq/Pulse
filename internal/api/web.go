package api

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

const webHistoryLimit = 50

//go:embed templates/*.html static/*
var webFiles embed.FS

var webTemplates = template.Must(template.New("").Funcs(template.FuncMap{
	"statusClass": func(status monitorstate.Status) string {
		switch status {
		case monitorstate.StatusUp:
			return "up"
		case monitorstate.StatusDown:
			return "down"
		default:
			return "unknown"
		}
	},
	"statusText": func(status monitorstate.Status) string {
		switch status {
		case monitorstate.StatusUp:
			return "Работает"
		case monitorstate.StatusDown:
			return "Недоступен"
		default:
			return "Нет данных"
		}
	},
	"formatTime": func(value *time.Time) string {
		if value == nil {
			return "Ещё не проверялся"
		}
		return value.UTC().Format("02.01.2006 15:04:05 UTC")
	},
	"formatCheckTime": func(value time.Time) string {
		return value.UTC().Format("02.01.2006 15:04:05")
	},
	"formatLatency": func(value *int64) string {
		if value == nil {
			return "Нет данных"
		}
		return fmt.Sprintf("%d мс", *value)
	},
	"checkStatus": func(value event.CheckResult) string {
		if value.Success {
			return "Успешно"
		}
		return "Ошибка"
	},
	"checkClass": func(value event.CheckResult) string {
		if value.Success {
			return "up"
		}
		return "down"
	},
	"checkCode": func(value event.CheckResult) string {
		if value.StatusCode == 0 {
			return "Нет ответа"
		}
		return strconv.Itoa(value.StatusCode)
	},
	"checkError": func(value event.CheckResult) string {
		if value.Error == nil {
			return "Нет"
		}
		return *value.Error
	},
}).ParseFS(webFiles, "templates/*.html"))

type webHandler struct {
	logger  *slog.Logger
	store   monitorStore
	states  stateStore
	history historyStore
}

type dashboardMonitor struct {
	Monitor monitor.Monitor
	State   monitorstate.State
}

type dashboardData struct {
	Monitors []dashboardMonitor
	Total    int
	Up       int
	Down     int
	Unknown  int
}

type monitorDetailsData struct {
	Monitor monitor.Monitor
	State   monitorstate.State
	Checks  []event.CheckResult
}

type monitorFormValues struct {
	Name               string
	URL                string
	Method             string
	IntervalSeconds    string
	TimeoutMS          string
	ExpectedStatusCode string
	Enabled            bool
}

type monitorFormData struct {
	Values monitorFormValues
	Errors map[string]string
}

type webErrorData struct {
	Code    int
	Title   string
	Message string
}

func newWebHandler(logger *slog.Logger, store monitorStore, states stateStore, history historyStore) *webHandler {
	return &webHandler{logger: logger, store: store, states: states, history: history}
}

func (h *webHandler) static() http.Handler {
	staticFiles, err := fs.Sub(webFiles, "static")
	if err != nil {
		panic(fmt.Sprintf("load embedded static files: %v", err))
	}
	fileServer := http.StripPrefix("/static/", http.FileServer(http.FS(staticFiles)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		fileServer.ServeHTTP(w, r)
	})
}

func (h *webHandler) dashboard(w http.ResponseWriter, r *http.Request) {
	items, err := h.store.List(r.Context())
	if err != nil {
		h.internalError(w, r, "загрузить список monitors", err)
		return
	}

	data := dashboardData{
		Monitors: make([]dashboardMonitor, 0, len(items)),
		Total:    len(items),
	}
	for _, item := range items {
		state, err := h.states.Get(r.Context(), item.ID)
		if err != nil {
			h.internalError(w, r, "загрузить текущий статус", err)
			return
		}
		data.Monitors = append(data.Monitors, dashboardMonitor{Monitor: item, State: state})
		switch state.Status {
		case monitorstate.StatusUp:
			data.Up++
		case monitorstate.StatusDown:
			data.Down++
		default:
			data.Unknown++
		}
	}

	h.render(w, r, http.StatusOK, "dashboard.html", data)
}

func (h *webHandler) newMonitor(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, http.StatusOK, "monitor_form.html", monitorFormData{
		Values: monitorFormValues{
			Method:             monitor.DefaultMethod,
			IntervalSeconds:    strconv.Itoa(monitor.DefaultIntervalSeconds),
			TimeoutMS:          strconv.Itoa(monitor.DefaultTimeoutMS),
			ExpectedStatusCode: strconv.Itoa(monitor.DefaultExpectedStatusCode),
			Enabled:            true,
		},
		Errors: map[string]string{},
	})
}

func (h *webHandler) createMonitor(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	if err := r.ParseForm(); err != nil {
		h.renderError(w, r, http.StatusBadRequest, "Некорректная форма", "Не удалось прочитать отправленные данные.")
		return
	}

	values := monitorFormValues{
		Name:               r.FormValue("name"),
		URL:                r.FormValue("url"),
		Method:             r.FormValue("method"),
		IntervalSeconds:    r.FormValue("interval_seconds"),
		TimeoutMS:          r.FormValue("timeout_ms"),
		ExpectedStatusCode: r.FormValue("expected_status_code"),
		Enabled:            r.FormValue("enabled") == "on",
	}
	errorsByField := make(map[string]string)
	interval := formInteger(values.IntervalSeconds, "interval_seconds", errorsByField)
	timeout := formInteger(values.TimeoutMS, "timeout_ms", errorsByField)
	expectedStatus := formInteger(values.ExpectedStatusCode, "expected_status_code", errorsByField)

	params := monitor.CreateParams{
		Name:               values.Name,
		URL:                values.URL,
		Method:             values.Method,
		IntervalSeconds:    interval,
		TimeoutMS:          timeout,
		Enabled:            &values.Enabled,
		ExpectedStatusCode: expectedStatus,
	}
	item, err := monitor.New(params)
	if err != nil {
		var validationErr *monitor.ValidationError
		if errors.As(err, &validationErr) {
			for field := range validationErr.Fields {
				if _, exists := errorsByField[field]; !exists {
					errorsByField[field] = validationMessage(field)
				}
			}
		}
		if len(errorsByField) == 0 {
			h.internalError(w, r, "проверить новый monitor", err)
			return
		}
	}
	if len(errorsByField) > 0 {
		h.render(w, r, http.StatusBadRequest, "monitor_form.html", monitorFormData{
			Values: values,
			Errors: errorsByField,
		})
		return
	}

	created, err := h.store.Create(r.Context(), item)
	if err != nil {
		h.internalError(w, r, "создать monitor", err)
		return
	}
	h.redirect(w, r, "/monitors/"+created.ID)
}

func (h *webHandler) monitorDetails(w http.ResponseWriter, r *http.Request) {
	id, ok := h.monitorID(w, r)
	if !ok {
		return
	}
	item, err := h.store.Get(r.Context(), id)
	if h.handleStoreError(w, r, "загрузить monitor", err) {
		return
	}
	state, err := h.states.Get(r.Context(), id)
	if err != nil {
		h.internalError(w, r, "загрузить текущий статус", err)
		return
	}
	checks, err := h.history.ListRecent(r.Context(), id, webHistoryLimit)
	if err != nil {
		h.internalError(w, r, "загрузить историю проверок", err)
		return
	}

	h.render(w, r, http.StatusOK, "monitor_details.html", monitorDetailsData{
		Monitor: item,
		State:   state,
		Checks:  checks,
	})
}

func (h *webHandler) toggleMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := h.monitorID(w, r)
	if !ok {
		return
	}
	item, err := h.store.Get(r.Context(), id)
	if h.handleStoreError(w, r, "загрузить monitor для изменения", err) {
		return
	}

	enabled := !item.Enabled
	updated, err := (monitor.Patch{Enabled: &enabled}).Apply(item)
	if err != nil {
		h.internalError(w, r, "изменить monitor", err)
		return
	}
	if _, err := h.store.Update(r.Context(), updated); h.handleStoreError(w, r, "сохранить monitor", err) {
		return
	}
	h.redirect(w, r, "/monitors/"+id)
}

func (h *webHandler) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := h.monitorID(w, r)
	if !ok {
		return
	}
	if err := h.store.Delete(r.Context(), id); h.handleStoreError(w, r, "удалить monitor", err) {
		return
	}
	h.redirect(w, r, "/")
}

func (h *webHandler) monitorID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "monitorID"))
	if err != nil {
		h.renderError(w, r, http.StatusNotFound, "Monitor не найден", "Проверьте адрес страницы.")
		return "", false
	}
	return id.String(), true
}

func (h *webHandler) handleStoreError(w http.ResponseWriter, r *http.Request, operation string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, monitor.ErrNotFound) {
		h.renderError(w, r, http.StatusNotFound, "Monitor не найден", "Возможно, он уже был удалён.")
		return true
	}
	h.internalError(w, r, operation, err)
	return true
}

func (h *webHandler) internalError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	h.logger.ErrorContext(r.Context(), "web request failed",
		"operation", operation,
		"request_id", middleware.GetReqID(r.Context()),
		"method", r.Method,
		"path", r.URL.Path,
		"error", err,
	)
	h.renderError(w, r, http.StatusInternalServerError, "Что-то пошло не так", "Pulse не смог выполнить запрос. Попробуйте ещё раз.")
}

func (h *webHandler) renderError(w http.ResponseWriter, r *http.Request, status int, title, message string) {
	h.render(w, r, status, "error.html", webErrorData{Code: status, Title: title, Message: message})
}

func (h *webHandler) render(w http.ResponseWriter, r *http.Request, status int, name string, data any) {
	var body bytes.Buffer
	if err := webTemplates.ExecuteTemplate(&body, name, data); err != nil {
		h.logger.ErrorContext(r.Context(), "render web template",
			"template", name,
			"request_id", middleware.GetReqID(r.Context()),
			"error", err,
		)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.WriteHeader(status)
	_, _ = body.WriteTo(w)
}

func (h *webHandler) redirect(w http.ResponseWriter, r *http.Request, location string) {
	http.Redirect(w, r, location, http.StatusSeeOther)
}

func formInteger(value, field string, errorsByField map[string]string) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		errorsByField[field] = "Введите целое число"
		return 0
	}
	return parsed
}

func validationMessage(field string) string {
	switch field {
	case "name":
		return "Введите название длиной до 200 символов"
	case "url":
		return "Укажите полный адрес с http:// или https://"
	case "method":
		return "Выберите GET или HEAD"
	case "interval_seconds":
		return "Интервал должен быть от 5 до 86400 секунд"
	case "timeout_ms":
		return "Timeout должен быть от 100 до 60000 мс и не больше интервала"
	case "expected_status_code":
		return "Код ответа должен быть от 100 до 599"
	default:
		return "Проверьте значение"
	}
}
