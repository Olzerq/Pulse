package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/notification"
)

func TestConsumeOneStoresBeforeCommit(t *testing.T) {
	t.Parallel()

	order := make([]string, 0, 4)
	message := validMessage(t)
	reader := &fakeReader{message: message, order: &order}
	history := &fakeHistoryStore{inserted: true, order: &order}
	states := &fakeStateStore{order: &order}
	service := newTestService(reader, history, states)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	if len(order) != 4 || order[0] != "history" || order[1] != "state_get" || order[2] != "state_set" || order[3] != "commit" {
		t.Fatalf("operation order = %v, want [history state_get state_set commit]", order)
	}
	if reader.committed.Offset != message.Offset {
		t.Errorf("committed offset = %d, want %d", reader.committed.Offset, message.Offset)
	}
	if states.state.Status != monitorstate.StatusUp || states.state.MonitorID != string(message.Key) {
		t.Errorf("stored state = %#v, want UP for %s", states.state, message.Key)
	}
}

func TestConsumeOneCommitsDuplicate(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{message: validMessage(t)}
	history := &fakeHistoryStore{inserted: false}
	states := &fakeStateStore{}
	service := newTestService(reader, history, states)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	if reader.commitCalls != 1 {
		t.Errorf("commit calls = %d, want 1", reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitStoreFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("database unavailable")
	reader := &fakeReader{message: validMessage(t)}
	history := &fakeHistoryStore{err: want}
	states := &fakeStateStore{}
	service := newTestService(reader, history, states)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if reader.commitCalls != 0 {
		t.Errorf("commit calls = %d, want 0", reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitStateFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("Redis unavailable")
	reader := &fakeReader{message: validMessage(t)}
	history := &fakeHistoryStore{inserted: true}
	states := &fakeStateStore{getErr: want}
	service := newTestService(reader, history, states)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if history.calls != 1 || states.getCalls != 1 || states.setCalls != 0 {
		t.Errorf("history calls = %d, state gets = %d, state sets = %d", history.calls, states.getCalls, states.setCalls)
	}
	if reader.commitCalls != 0 {
		t.Errorf("commit calls = %d, want 0", reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitStateUpdateFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("Redis unavailable")
	reader := &fakeReader{message: validMessage(t)}
	history := &fakeHistoryStore{inserted: true}
	states := &fakeStateStore{setErr: want}
	service := newTestService(reader, history, states)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if states.getCalls != 1 || states.setCalls != 1 || reader.commitCalls != 0 {
		t.Errorf("state gets = %d, state sets = %d, commits = %d", states.getCalls, states.setCalls, reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitCommitFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("commit unavailable")
	reader := &fakeReader{message: validMessage(t), commitErr: want}
	history := &fakeHistoryStore{inserted: true}
	states := &fakeStateStore{}
	service := newTestService(reader, history, states)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if reader.commitCalls != 1 {
		t.Errorf("commit calls = %d, want 1 attempt", reader.commitCalls)
	}
}

func TestConsumeOneRejectsInvalidMessageWithoutCommit(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{message: kafkago.Message{Value: []byte("not-json")}}
	history := &fakeHistoryStore{}
	states := &fakeStateStore{}
	service := newTestService(reader, history, states)

	if err := service.consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want decoding error")
	}
	if history.calls != 0 || states.getCalls != 0 || states.setCalls != 0 {
		t.Errorf("history calls = %d, state gets = %d, state sets = %d, want all zero", history.calls, states.getCalls, states.setCalls)
	}
	if reader.commitCalls != 0 {
		t.Errorf("commit calls = %d, want 0", reader.commitCalls)
	}
}

func TestConsumeOneRejectsMismatchedKeyWithoutCommit(t *testing.T) {
	t.Parallel()

	message := validMessage(t)
	message.Key = []byte("another-monitor")
	reader := &fakeReader{message: message}
	history := &fakeHistoryStore{}
	states := &fakeStateStore{}
	service := newTestService(reader, history, states)

	if err := service.consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want key validation error")
	}
	if history.calls != 0 || states.getCalls != 0 || states.setCalls != 0 || reader.commitCalls != 0 {
		t.Errorf(
			"history calls = %d, state gets = %d, state sets = %d, commit calls = %d, want all zero",
			history.calls,
			states.getCalls,
			states.setCalls,
			reader.commitCalls,
		)
	}
}

func TestConsumeOneSendsTransitionBeforeStateAndCommit(t *testing.T) {
	t.Parallel()

	order := make([]string, 0, 7)
	message := downMessage(t)
	reader := &fakeReader{message: message, order: &order}
	history := &fakeHistoryStore{inserted: true, order: &order}
	states := &fakeStateStore{current: upState(string(message.Key)), order: &order}
	transitions := &fakeTransitionStore{status: notification.DeliveryPending, order: &order}
	sender := &fakeSender{messageID: 42, order: &order}
	service := NewService(
		reader,
		history,
		states,
		transitions,
		sender,
		true,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
	)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	want := []string{"history", "state_get", "transition", "send", "mark_sent", "state_set", "commit"}
	if len(order) != len(want) {
		t.Fatalf("operation order = %v, want %v", order, want)
	}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("operation order = %v, want %v", order, want)
		}
	}
	if transitions.prepared.Previous != monitorstate.StatusUp || transitions.prepared.Current != monitorstate.StatusDown {
		t.Errorf("prepared transition = %#v", transitions.prepared)
	}
}

func TestConsumeOneDoesNotCommitTelegramFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("Telegram unavailable")
	message := downMessage(t)
	reader := &fakeReader{message: message}
	history := &fakeHistoryStore{inserted: true}
	states := &fakeStateStore{current: upState(string(message.Key))}
	transitions := &fakeTransitionStore{status: notification.DeliveryPending}
	sender := &fakeSender{err: want}
	service := NewService(
		reader,
		history,
		states,
		transitions,
		sender,
		true,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
	)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if transitions.failureCalls != 1 || states.setCalls != 0 || reader.commitCalls != 0 {
		t.Errorf("failure calls = %d, state sets = %d, commits = %d", transitions.failureCalls, states.setCalls, reader.commitCalls)
	}
}

func TestConsumeOneDoesNotResendDeliveredTransition(t *testing.T) {
	t.Parallel()

	message := downMessage(t)
	reader := &fakeReader{message: message}
	history := &fakeHistoryStore{inserted: false}
	states := &fakeStateStore{current: upState(string(message.Key))}
	transitions := &fakeTransitionStore{status: notification.DeliverySent}
	sender := &fakeSender{}
	service := NewService(
		reader,
		history,
		states,
		transitions,
		sender,
		true,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
	)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	if sender.calls != 0 || transitions.sentCalls != 0 || reader.commitCalls != 1 {
		t.Errorf("sender calls = %d, mark sent calls = %d, commits = %d", sender.calls, transitions.sentCalls, reader.commitCalls)
	}
}

func TestConsumeOneMarksTransitionSkippedWhenTelegramDisabled(t *testing.T) {
	t.Parallel()

	message := downMessage(t)
	reader := &fakeReader{message: message}
	history := &fakeHistoryStore{inserted: true}
	states := &fakeStateStore{current: upState(string(message.Key))}
	transitions := &fakeTransitionStore{status: notification.DeliveryPending}
	service := NewService(
		reader,
		history,
		states,
		transitions,
		nil,
		false,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
	)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	if transitions.skippedCalls != 1 || states.setCalls != 1 || reader.commitCalls != 1 {
		t.Errorf("skipped = %d, state sets = %d, commits = %d", transitions.skippedCalls, states.setCalls, reader.commitCalls)
	}
}

func validMessage(t *testing.T) kafkago.Message {
	t.Helper()

	result := event.CheckResult{
		EventID:    "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
		MonitorID:  "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		CheckedAt:  time.Now().UTC(),
		Success:    true,
		StatusCode: 200,
		LatencyMS:  42,
	}
	value, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal test event: %v", err)
	}
	return kafkago.Message{
		Topic:     "check.result",
		Partition: 2,
		Offset:    7,
		Key:       []byte(result.MonitorID),
		Value:     value,
	}
}

func downMessage(t *testing.T) kafkago.Message {
	t.Helper()

	reason := "expected HTTP status 200, got 503"
	result := event.CheckResult{
		EventID:    "b4a22989-d25e-4a58-a394-dd92c8869906",
		MonitorID:  "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		CheckedAt:  time.Now().UTC(),
		Success:    false,
		StatusCode: 503,
		LatencyMS:  42,
		ErrorKind:  check.FailureUnexpectedStatus,
		Error:      &reason,
	}
	value, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal test event: %v", err)
	}
	return kafkago.Message{
		Topic:     "check.result",
		Partition: 2,
		Offset:    8,
		Key:       []byte(result.MonitorID),
		Value:     value,
	}
}

func upState(monitorID string) monitorstate.State {
	checkedAt := time.Now().UTC().Add(-time.Minute)
	statusCode := 200
	latencyMS := int64(10)
	return monitorstate.State{
		MonitorID:          monitorID,
		Status:             monitorstate.StatusUp,
		CheckedAt:          &checkedAt,
		LastStatusChangeAt: &checkedAt,
		StatusCode:         &statusCode,
		LatencyMS:          &latencyMS,
	}
}

func newTestService(reader messageReader, history historyStore, states stateStore) *Service {
	return NewService(
		reader,
		history,
		states,
		&fakeTransitionStore{status: notification.DeliveryPending},
		nil,
		false,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		time.Second,
	)
}

type fakeReader struct {
	message     kafkago.Message
	fetchErr    error
	commitErr   error
	committed   kafkago.Message
	commitCalls int
	order       *[]string
}

func (r *fakeReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return r.message, r.fetchErr
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafkago.Message) error {
	r.commitCalls++
	if len(messages) > 0 {
		r.committed = messages[len(messages)-1]
	}
	if r.order != nil {
		*r.order = append(*r.order, "commit")
	}
	return r.commitErr
}

func (r *fakeReader) Close() error { return nil }

type fakeHistoryStore struct {
	inserted bool
	err      error
	calls    int
	order    *[]string
}

func (s *fakeHistoryStore) Insert(context.Context, event.CheckResult) (bool, error) {
	s.calls++
	if s.order != nil {
		*s.order = append(*s.order, "history")
	}
	return s.inserted, s.err
}

type fakeStateStore struct {
	getErr   error
	setErr   error
	getCalls int
	setCalls int
	current  monitorstate.State
	state    monitorstate.State
	order    *[]string
}

func (s *fakeStateStore) Get(_ context.Context, monitorID string) (monitorstate.State, error) {
	s.getCalls++
	if s.order != nil {
		*s.order = append(*s.order, "state_get")
	}
	if s.getErr != nil {
		return monitorstate.State{}, s.getErr
	}
	if s.current.Status == "" {
		return monitorstate.Unknown(monitorID), nil
	}
	return s.current, nil
}

func (s *fakeStateStore) Set(_ context.Context, state monitorstate.State) error {
	s.setCalls++
	s.state = state
	if s.order != nil {
		*s.order = append(*s.order, "state_set")
	}
	return s.setErr
}

type fakeTransitionStore struct {
	status       notification.DeliveryStatus
	prepareErr   error
	markErr      error
	failureErr   error
	prepared     monitorstate.Transition
	sentCalls    int
	skippedCalls int
	failureCalls int
	order        *[]string
}

func (s *fakeTransitionStore) Prepare(_ context.Context, transition monitorstate.Transition) (notification.Delivery, error) {
	s.prepared = transition
	if s.order != nil {
		*s.order = append(*s.order, "transition")
	}
	return notification.Delivery{Transition: transition, Status: s.status}, s.prepareErr
}

func (s *fakeTransitionStore) MarkSent(context.Context, string, int64) error {
	s.sentCalls++
	if s.order != nil {
		*s.order = append(*s.order, "mark_sent")
	}
	return s.markErr
}

func (s *fakeTransitionStore) MarkSkipped(context.Context, string) error {
	s.skippedCalls++
	if s.order != nil {
		*s.order = append(*s.order, "mark_skipped")
	}
	return s.markErr
}

func (s *fakeTransitionStore) RecordFailure(context.Context, string, string) error {
	s.failureCalls++
	return s.failureErr
}

type fakeSender struct {
	messageID int64
	err       error
	calls     int
	order     *[]string
}

func (s *fakeSender) Send(context.Context, monitorstate.Transition) (int64, error) {
	s.calls++
	if s.order != nil {
		*s.order = append(*s.order, "send")
	}
	return s.messageID, s.err
}
