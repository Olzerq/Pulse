package monitorstate

import (
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
)

func TestUnknown(t *testing.T) {
	t.Parallel()

	state := Unknown("monitor-id")
	if state.Status != StatusUnknown {
		t.Errorf("Status = %q, want UNKNOWN", state.Status)
	}
	if state.CheckedAt != nil || state.StatusCode != nil || state.LatencyMS != nil || state.Error != nil {
		t.Errorf("unknown state contains check data: %#v", state)
	}
}

func TestFromCheck(t *testing.T) {
	t.Parallel()

	checkedAt := time.Now().UTC()
	result := event.CheckResult{
		MonitorID:  "monitor-id",
		CheckedAt:  checkedAt,
		Success:    true,
		StatusCode: 204,
		LatencyMS:  17,
	}

	state := FromCheck(result)
	if state.Status != StatusUp {
		t.Errorf("Status = %q, want UP", state.Status)
	}
	if state.CheckedAt == nil || !state.CheckedAt.Equal(checkedAt) {
		t.Errorf("CheckedAt = %v, want %v", state.CheckedAt, checkedAt)
	}
	if state.StatusCode == nil || *state.StatusCode != 204 {
		t.Errorf("StatusCode = %v, want 204", state.StatusCode)
	}
	if state.LatencyMS == nil || *state.LatencyMS != 17 {
		t.Errorf("LatencyMS = %v, want 17", state.LatencyMS)
	}
	if state.LastStatusChangeAt == nil || !state.LastStatusChangeAt.Equal(checkedAt) {
		t.Errorf("LastStatusChangeAt = %v, want %v", state.LastStatusChangeAt, checkedAt)
	}
}

func TestDownStateValidate(t *testing.T) {
	t.Parallel()

	description := "request timed out"
	state := FromCheck(event.CheckResult{
		MonitorID: "monitor-id",
		CheckedAt: time.Now().UTC(),
		Success:   false,
		LatencyMS: 1000,
		ErrorKind: check.FailureTimeout,
		Error:     &description,
	})

	if state.Status != StatusDown {
		t.Errorf("Status = %q, want DOWN", state.Status)
	}
	if err := state.Validate("monitor-id"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAdvanceStateMachine(t *testing.T) {
	t.Parallel()

	upAt := time.Now().UTC().Add(-time.Minute)
	up := event.CheckResult{
		EventID:    "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
		MonitorID:  "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		CheckedAt:  upAt,
		Success:    true,
		StatusCode: 200,
		LatencyMS:  10,
	}

	baseline, transition := Advance(Unknown(up.MonitorID), up)
	if transition != nil {
		t.Fatalf("UNKNOWN to UP transition = %#v, want nil", transition)
	}
	if baseline.Status != StatusUp {
		t.Fatalf("baseline status = %q, want UP", baseline.Status)
	}

	secondUp := up
	secondUp.EventID = "5df75599-fea3-45b5-b54a-99df074b7a4c"
	secondUp.CheckedAt = upAt.Add(30 * time.Second)
	unchanged, transition := Advance(baseline, secondUp)
	if transition != nil {
		t.Fatalf("UP to UP transition = %#v, want nil", transition)
	}
	if !unchanged.LastStatusChangeAt.Equal(upAt) {
		t.Errorf("UP to UP changed at = %v, want %v", unchanged.LastStatusChangeAt, upAt)
	}

	reason := "expected HTTP status 200, got 503"
	down := event.CheckResult{
		EventID:    "b4a22989-d25e-4a58-a394-dd92c8869906",
		MonitorID:  up.MonitorID,
		CheckedAt:  upAt.Add(time.Minute),
		Success:    false,
		StatusCode: 503,
		LatencyMS:  15,
		ErrorKind:  check.FailureUnexpectedStatus,
		Error:      &reason,
	}
	downState, transition := Advance(unchanged, down)
	if transition == nil || transition.Previous != StatusUp || transition.Current != StatusDown {
		t.Fatalf("UP to DOWN transition = %#v", transition)
	}
	if !downState.LastStatusChangeAt.Equal(down.CheckedAt) {
		t.Errorf("UP to DOWN changed at = %v, want %v", downState.LastStatusChangeAt, down.CheckedAt)
	}

	recovery := secondUp
	recovery.EventID = "3a9715fb-eb63-429d-9270-eade2b27ba05"
	recovery.CheckedAt = down.CheckedAt.Add(time.Minute)
	_, transition = Advance(downState, recovery)
	if transition == nil || transition.Previous != StatusDown || transition.Current != StatusUp {
		t.Fatalf("DOWN to UP transition = %#v", transition)
	}
}
