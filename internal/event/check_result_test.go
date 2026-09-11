package event

import (
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
)

func TestNewCheckResult(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, time.September, 10, 16, 30, 0, 123, time.FixedZone("test", 3*60*60))
	result := check.Result{
		MonitorID:  "monitor-id",
		CheckedAt:  checkedAt,
		Success:    false,
		StatusCode: 503,
		LatencyMS:  142,
		ErrorKind:  check.FailureUnexpectedStatus,
		Error:      "expected status 200, got 503",
	}

	got := NewCheckResult("event-id", result)

	if got.EventID != "event-id" {
		t.Errorf("EventID = %q, want event-id", got.EventID)
	}
	if got.MonitorID != result.MonitorID {
		t.Errorf("MonitorID = %q, want %q", got.MonitorID, result.MonitorID)
	}
	if !got.CheckedAt.Equal(checkedAt) || got.CheckedAt.Location() != time.UTC {
		t.Errorf("CheckedAt = %v, want the same instant in UTC", got.CheckedAt)
	}
	if got.Error == nil || *got.Error != result.Error {
		t.Errorf("Error = %v, want %q", got.Error, result.Error)
	}
}

func TestNewCheckResultLeavesSuccessfulErrorNull(t *testing.T) {
	t.Parallel()

	got := NewCheckResult("event-id", check.Result{Success: true})
	if got.Error != nil {
		t.Errorf("Error = %q, want nil", *got.Error)
	}
}
