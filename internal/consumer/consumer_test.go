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

	"github.com/Olzerq/Pulse/internal/event"
	"github.com/Olzerq/Pulse/internal/monitorstate"
)

func TestConsumeOneStoresBeforeCommit(t *testing.T) {
	t.Parallel()

	order := make([]string, 0, 3)
	message := validMessage(t)
	reader := &fakeReader{message: message, order: &order}
	history := &fakeHistoryStore{inserted: true, order: &order}
	states := &fakeStateStore{order: &order}
	service := newTestService(reader, history, states)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	want := []string{"history", "state", "commit"}
	for index := range want {
		if order[index] != want[index] {
			t.Fatalf("operation order = %v, want %v", order, want)
		}
	}
	if states.state.Status != monitorstate.StatusUp || states.state.MonitorID != string(message.Key) {
		t.Errorf("stored state = %#v, want UP for %s", states.state, message.Key)
	}
}

func TestConsumeOneCommitsDuplicate(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{message: validMessage(t)}
	service := newTestService(reader, &fakeHistoryStore{}, &fakeStateStore{})
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
	err := newTestService(reader, &fakeHistoryStore{err: want}, &fakeStateStore{}).consumeOne(context.Background())
	if !errors.Is(err, want) || reader.commitCalls != 0 {
		t.Fatalf("error = %v, commit calls = %d", err, reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitStateFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("Redis unavailable")
	reader := &fakeReader{message: validMessage(t)}
	err := newTestService(reader, &fakeHistoryStore{inserted: true}, &fakeStateStore{err: want}).consumeOne(context.Background())
	if !errors.Is(err, want) || reader.commitCalls != 0 {
		t.Fatalf("error = %v, commit calls = %d", err, reader.commitCalls)
	}
}

func TestConsumeOneRejectsInvalidMessageWithoutCommit(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{message: kafkago.Message{Value: []byte("not-json")}}
	history := &fakeHistoryStore{}
	states := &fakeStateStore{}
	if err := newTestService(reader, history, states).consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want decoding error")
	}
	if history.calls != 0 || states.calls != 0 || reader.commitCalls != 0 {
		t.Fatalf("history = %d, state = %d, commit = %d", history.calls, states.calls, reader.commitCalls)
	}
}

func TestConsumeOneRejectsMismatchedKeyWithoutCommit(t *testing.T) {
	t.Parallel()

	message := validMessage(t)
	message.Key = []byte("another-monitor")
	reader := &fakeReader{message: message}
	history := &fakeHistoryStore{}
	states := &fakeStateStore{}
	if err := newTestService(reader, history, states).consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want key validation error")
	}
	if history.calls != 0 || states.calls != 0 || reader.commitCalls != 0 {
		t.Fatalf("history = %d, state = %d, commit = %d", history.calls, states.calls, reader.commitCalls)
	}
}

func validMessage(t *testing.T) kafkago.Message {
	t.Helper()
	result := event.CheckResult{
		EventID: "2efad0fa-47c9-4ff5-b2c8-a61735ac1251", MonitorID: "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		CheckedAt: time.Now().UTC(), Success: true, StatusCode: 200, LatencyMS: 42,
	}
	value, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return kafkago.Message{Topic: "check.result", Partition: 2, Offset: 7, Key: []byte(result.MonitorID), Value: value}
}

func newTestService(reader messageReader, history historyStore, states stateStore) *Service {
	return NewService(reader, history, states, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)
}

type fakeReader struct {
	message     kafkago.Message
	commitErr   error
	commitCalls int
	order       *[]string
}

func (r *fakeReader) FetchMessage(context.Context) (kafkago.Message, error) { return r.message, nil }
func (r *fakeReader) CommitMessages(context.Context, ...kafkago.Message) error {
	r.commitCalls++
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
	err   error
	calls int
	state monitorstate.State
	order *[]string
}

func (s *fakeStateStore) Set(_ context.Context, state monitorstate.State) error {
	s.calls++
	s.state = state
	if s.order != nil {
		*s.order = append(*s.order, "state")
	}
	return s.err
}
