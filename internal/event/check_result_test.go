package event

import (
	"strings"
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

func TestCheckResultValidate(t *testing.T) {
	t.Parallel()

	valid := CheckResult{
		EventID:    "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
		MonitorID:  "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		CheckedAt:  time.Now().UTC(),
		Success:    true,
		StatusCode: 200,
		LatencyMS:  42,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCheckResultValidateRejectsMalformedEvent(t *testing.T) {
	t.Parallel()

	emptyError := " "
	invalid := CheckResult{
		EventID:    "not-a-uuid",
		MonitorID:  "also-not-a-uuid",
		Success:    false,
		StatusCode: 700,
		LatencyMS:  -1,
		Error:      &emptyError,
	}

	err := invalid.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want validation errors")
	}
	for _, message := range []string{
		"event_id must be a UUID",
		"monitor_id must be a UUID",
		"checked_at is required",
		"status_code must be zero or between 100 and 599",
		"latency_ms must not be negative",
		"unknown error_kind",
		"a failed check requires an error",
	} {
		if !strings.Contains(err.Error(), message) {
			t.Errorf("Validate() error = %q, want %q", err, message)
		}
	}
}
