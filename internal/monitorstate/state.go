// Package monitorstate defines the latest known runtime state of a monitor.
package monitorstate

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
)

type Status string

const (
	StatusUnknown Status = "UNKNOWN"
	StatusUp      Status = "UP"
	StatusDown    Status = "DOWN"
)

// State is the latest check result exposed by the API. Pointer fields preserve
// the distinction between a real zero value and data that does not exist yet.
type State struct {
	MonitorID          string            `json:"monitor_id"`
	Status             Status            `json:"status"`
	CheckedAt          *time.Time        `json:"checked_at"`
	LastStatusChangeAt *time.Time        `json:"last_status_change_at"`
	StatusCode         *int              `json:"status_code"`
	LatencyMS          *int64            `json:"latency_ms"`
	ErrorKind          check.FailureKind `json:"error_kind,omitempty"`
	Error              *string           `json:"error"`
}

// Transition is an alert-worthy change between known states. Initial changes
// from UNKNOWN establish the baseline and deliberately do not create one.
type Transition struct {
	EventID       string            `json:"event_id"`
	MonitorID     string            `json:"monitor_id"`
	Previous      Status            `json:"previous_status"`
	Current       Status            `json:"new_status"`
	ChangedAt     time.Time         `json:"changed_at"`
	StatusCode    *int              `json:"status_code"`
	LatencyMS     int64             `json:"latency_ms"`
	FailureKind   check.FailureKind `json:"error_kind,omitempty"`
	FailureReason *string           `json:"error"`
}

func Unknown(monitorID string) State {
	return State{
		MonitorID: monitorID,
		Status:    StatusUnknown,
	}
}

func FromCheck(result event.CheckResult) State {
	checkedAt := result.CheckedAt.UTC()
	latencyMS := result.LatencyMS

	var statusCode *int
	if result.StatusCode != 0 {
		value := result.StatusCode
		statusCode = &value
	}

	status := StatusDown
	if result.Success {
		status = StatusUp
	}

	return State{
		MonitorID:          result.MonitorID,
		Status:             status,
		CheckedAt:          &checkedAt,
		LastStatusChangeAt: &checkedAt,
		StatusCode:         statusCode,
		LatencyMS:          &latencyMS,
		ErrorKind:          result.ErrorKind,
		Error:              result.Error,
	}
}

// Advance applies a check result to the previous state. Only UP to DOWN and
// DOWN to UP are alert-worthy transitions. UNKNOWN establishes a baseline.
func Advance(previous State, result event.CheckResult) (State, *Transition) {
	current := FromCheck(result)
	if previous.Status == current.Status && previous.LastStatusChangeAt != nil {
		current.LastStatusChangeAt = previous.LastStatusChangeAt
	}

	if !isKnown(previous.Status) || previous.Status == current.Status {
		return current, nil
	}

	return current, &Transition{
		EventID:       result.EventID,
		MonitorID:     result.MonitorID,
		Previous:      previous.Status,
		Current:       current.Status,
		ChangedAt:     result.CheckedAt.UTC(),
		StatusCode:    current.StatusCode,
		LatencyMS:     result.LatencyMS,
		FailureKind:   result.ErrorKind,
		FailureReason: result.Error,
	}
}

// Validate rejects corrupt Redis values rather than returning misleading
// status data to API clients.
func (s State) Validate(expectedMonitorID string) error {
	var errs []error

	if s.MonitorID != expectedMonitorID {
		errs = append(errs, fmt.Errorf("monitor_id %q does not match key monitor %q", s.MonitorID, expectedMonitorID))
	}
	if s.Status != StatusUp && s.Status != StatusDown {
		errs = append(errs, fmt.Errorf("stored status must be UP or DOWN, got %q", s.Status))
	}
	if s.CheckedAt == nil || s.CheckedAt.IsZero() {
		errs = append(errs, errors.New("checked_at is required"))
	}
	if s.LastStatusChangeAt == nil || s.LastStatusChangeAt.IsZero() {
		errs = append(errs, errors.New("last_status_change_at is required"))
	} else if s.CheckedAt != nil && s.LastStatusChangeAt.After(*s.CheckedAt) {
		errs = append(errs, errors.New("last_status_change_at must not be after checked_at"))
	}
	if s.LatencyMS == nil || *s.LatencyMS < 0 {
		errs = append(errs, errors.New("latency_ms must be present and non-negative"))
	}
	if s.StatusCode != nil && (*s.StatusCode < 100 || *s.StatusCode > 599) {
		errs = append(errs, errors.New("status_code must be null or between 100 and 599"))
	}
	if s.Status == StatusUp && (s.ErrorKind != check.FailureNone || s.Error != nil) {
		errs = append(errs, errors.New("UP state must not contain an error"))
	}
	if s.Status == StatusUp && s.StatusCode == nil {
		errs = append(errs, errors.New("UP state requires status_code"))
	}
	if s.Status == StatusDown && (s.Error == nil || strings.TrimSpace(*s.Error) == "") {
		errs = append(errs, errors.New("DOWN state requires an error"))
	}

	return errors.Join(errs...)
}

func isKnown(status Status) bool {
	return status == StatusUp || status == StatusDown
}
