// Package event contains the contracts exchanged through the Pulse event bus.
package event

import (
	"time"

	"github.com/Olzerq/Pulse/internal/check"
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
