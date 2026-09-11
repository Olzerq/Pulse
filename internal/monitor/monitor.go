// Package monitor contains the monitor domain model and its validation rules.
package monitor

import (
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	DefaultMethod             = "GET"
	DefaultIntervalSeconds    = 60
	DefaultTimeoutMS          = 5000
	DefaultExpectedStatusCode = 200

	minIntervalSeconds = 5
	maxIntervalSeconds = 24 * 60 * 60
	minTimeoutMS       = 100
	maxTimeoutMS       = 60 * 1000
	maxNameLength      = 200
	maxURLLength       = 2048
)

var ErrNotFound = errors.New("monitor not found")

// Monitor describes an HTTP endpoint that Pulse checks periodically.
type Monitor struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	URL                string    `json:"url"`
	Method             string    `json:"method"`
	IntervalSeconds    int       `json:"interval_seconds"`
	TimeoutMS          int       `json:"timeout_ms"`
	Enabled            bool      `json:"enabled"`
	ExpectedStatusCode int       `json:"expected_status_code"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// CreateParams is the input accepted when a monitor is created. Pointer fields
// distinguish an omitted value from an explicit zero value.
type CreateParams struct {
	Name               string `json:"name"`
	URL                string `json:"url"`
	Method             string `json:"method"`
	IntervalSeconds    int    `json:"interval_seconds"`
	TimeoutMS          int    `json:"timeout_ms"`
	Enabled            *bool  `json:"enabled"`
	ExpectedStatusCode int    `json:"expected_status_code"`
}

// Patch contains optional updates for an existing monitor.
type Patch struct {
	Name               *string `json:"name"`
	URL                *string `json:"url"`
	Method             *string `json:"method"`
	IntervalSeconds    *int    `json:"interval_seconds"`
	TimeoutMS          *int    `json:"timeout_ms"`
	Enabled            *bool   `json:"enabled"`
	ExpectedStatusCode *int    `json:"expected_status_code"`
}

// ValidationError maps invalid API fields to human-readable explanations.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	return "monitor validation failed"
}

// New constructs a monitor and applies defaults used by the API.
func New(params CreateParams) (Monitor, error) {
	method := params.Method
	if strings.TrimSpace(method) == "" {
		method = DefaultMethod
	}

	intervalSeconds := params.IntervalSeconds
	if intervalSeconds == 0 {
		intervalSeconds = DefaultIntervalSeconds
	}

	timeoutMS := params.TimeoutMS
	if timeoutMS == 0 {
		timeoutMS = DefaultTimeoutMS
	}

	expectedStatusCode := params.ExpectedStatusCode
	if expectedStatusCode == 0 {
		expectedStatusCode = DefaultExpectedStatusCode
	}

	enabled := true
	if params.Enabled != nil {
		enabled = *params.Enabled
	}

	result := Monitor{
		Name:               params.Name,
		URL:                params.URL,
		Method:             method,
		IntervalSeconds:    intervalSeconds,
		TimeoutMS:          timeoutMS,
		Enabled:            enabled,
		ExpectedStatusCode: expectedStatusCode,
	}
	result.normalize()

	if err := result.Validate(); err != nil {
		return Monitor{}, err
	}
	return result, nil
}

// Apply returns a validated copy with the requested fields changed.
func (p Patch) Apply(current Monitor) (Monitor, error) {
	if p.Name != nil {
		current.Name = *p.Name
	}
	if p.URL != nil {
		current.URL = *p.URL
	}
	if p.Method != nil {
		current.Method = *p.Method
	}
	if p.IntervalSeconds != nil {
		current.IntervalSeconds = *p.IntervalSeconds
	}
	if p.TimeoutMS != nil {
		current.TimeoutMS = *p.TimeoutMS
	}
	if p.Enabled != nil {
		current.Enabled = *p.Enabled
	}
	if p.ExpectedStatusCode != nil {
		current.ExpectedStatusCode = *p.ExpectedStatusCode
	}

	current.normalize()
	if err := current.Validate(); err != nil {
		return Monitor{}, err
	}
	return current, nil
}

// Empty reports whether a PATCH request contains no supported fields.
func (p Patch) Empty() bool {
	return p.Name == nil &&
		p.URL == nil &&
		p.Method == nil &&
		p.IntervalSeconds == nil &&
		p.TimeoutMS == nil &&
		p.Enabled == nil &&
		p.ExpectedStatusCode == nil
}

// Validate enforces rules shared by API input and persisted monitors.
func (m Monitor) Validate() error {
	fields := make(map[string]string)

	if m.Name == "" {
		fields["name"] = "must not be empty"
	} else if utf8.RuneCountInString(m.Name) > maxNameLength {
		fields["name"] = "must contain at most 200 characters"
	}

	if m.URL == "" {
		fields["url"] = "must not be empty"
	} else if len(m.URL) > maxURLLength {
		fields["url"] = "must contain at most 2048 bytes"
	} else if err := validateURL(m.URL); err != "" {
		fields["url"] = err
	}

	if m.Method != "GET" && m.Method != "HEAD" {
		fields["method"] = "must be GET or HEAD"
	}
	if m.IntervalSeconds < minIntervalSeconds || m.IntervalSeconds > maxIntervalSeconds {
		fields["interval_seconds"] = "must be between 5 and 86400"
	}
	if m.TimeoutMS < minTimeoutMS || m.TimeoutMS > maxTimeoutMS {
		fields["timeout_ms"] = "must be between 100 and 60000"
	} else if m.IntervalSeconds > 0 && m.TimeoutMS > m.IntervalSeconds*1000 {
		fields["timeout_ms"] = "must not exceed interval_seconds"
	}
	if m.ExpectedStatusCode < 100 || m.ExpectedStatusCode > 599 {
		fields["expected_status_code"] = "must be between 100 and 599"
	}

	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

func (m *Monitor) normalize() {
	m.Name = strings.TrimSpace(m.Name)
	m.URL = strings.TrimSpace(m.URL)
	m.Method = strings.ToUpper(strings.TrimSpace(m.Method))
}

func validateURL(value string) string {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" {
		return "must be a valid absolute HTTP URL"
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "scheme must be http or https"
	}
	if parsed.User != nil {
		return "must not contain credentials"
	}
	return ""
}
