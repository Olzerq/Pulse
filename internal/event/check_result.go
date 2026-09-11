// Package event contains the contracts exchanged through the Pulse event bus.
package event

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/google/uuid"
)

const CheckResultType = "check.result"

// CheckResult is the stable JSON contract published after every HTTP check.
// Error stays null for successful checks so consumers can distinguish an
// absent failure from an empty error description.
type CheckResult struct {
	EventID    string            `json:"event_id"`
	MonitorID  string            `json:"monitor_id"`
	CheckedAt  time.Time         `json:"checked_at"`
	Success    bool              `json:"success"`
	StatusCode int               `json:"status_code"`
	LatencyMS  int64             `json:"latency_ms"`
	ErrorKind  check.FailureKind `json:"error_kind,omitempty"`
	Error      *string           `json:"error"`
}

func NewCheckResult(eventID string, result check.Result) CheckResult {
	var resultError *string
	if result.Error != "" {
		value := result.Error
		resultError = &value
	}

	return CheckResult{
		EventID:    eventID,
		MonitorID:  result.MonitorID,
		CheckedAt:  result.CheckedAt.UTC(),
		Success:    result.Success,
		StatusCode: result.StatusCode,
		LatencyMS:  result.LatencyMS,
		ErrorKind:  result.ErrorKind,
		Error:      resultError,
	}
}

// Validate protects the database and the consumer loop from malformed or
// incomplete messages. Unknown JSON fields remain allowed for forward
// compatibility, while the fields required by this version are strict.
func (e CheckResult) Validate() error {
	var errs []error

	if _, err := uuid.Parse(e.EventID); err != nil {
		errs = append(errs, fmt.Errorf("event_id must be a UUID: %w", err))
	}
	if _, err := uuid.Parse(e.MonitorID); err != nil {
		errs = append(errs, fmt.Errorf("monitor_id must be a UUID: %w", err))
	}
	if e.CheckedAt.IsZero() {
		errs = append(errs, errors.New("checked_at is required"))
	}
	if e.StatusCode != 0 && (e.StatusCode < 100 || e.StatusCode > 599) {
		errs = append(errs, errors.New("status_code must be zero or between 100 and 599"))
	}
	if e.LatencyMS < 0 {
		errs = append(errs, errors.New("latency_ms must not be negative"))
	}

	if e.Success {
		if e.StatusCode == 0 {
			errs = append(errs, errors.New("a successful check requires status_code"))
		}
		if e.ErrorKind != check.FailureNone || e.Error != nil {
			errs = append(errs, errors.New("a successful check must not contain an error"))
		}
	} else {
		if !knownFailureKind(e.ErrorKind) {
			errs = append(errs, fmt.Errorf("unknown error_kind %q", e.ErrorKind))
		}
		if e.Error == nil || strings.TrimSpace(*e.Error) == "" {
			errs = append(errs, errors.New("a failed check requires an error"))
		}
	}

	return errors.Join(errs...)
}

func knownFailureKind(kind check.FailureKind) bool {
	switch kind {
	case check.FailureTimeout,
		check.FailureCanceled,
		check.FailureNetwork,
		check.FailureRequest,
		check.FailureUnexpectedStatus:
		return true
	default:
		return false
	}
}
