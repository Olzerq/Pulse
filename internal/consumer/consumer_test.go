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
)

func TestConsumeOneStoresBeforeCommit(t *testing.T) {
	t.Parallel()

	order := make([]string, 0, 2)
	message := validMessage(t)
	reader := &fakeReader{message: message, order: &order}
	store := &fakeStore{inserted: true, order: &order}
	service := newTestService(reader, store)

	if err := service.consumeOne(context.Background()); err != nil {
		t.Fatalf("consumeOne() error = %v", err)
	}
	if len(order) != 2 || order[0] != "store" || order[1] != "commit" {
		t.Fatalf("operation order = %v, want [store commit]", order)
	}
	if reader.committed.Offset != message.Offset {
		t.Errorf("committed offset = %d, want %d", reader.committed.Offset, message.Offset)
	}
}

func TestConsumeOneCommitsDuplicate(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{message: validMessage(t)}
	store := &fakeStore{inserted: false}
	service := newTestService(reader, store)

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
	store := &fakeStore{err: want}
	service := newTestService(reader, store)

	err := service.consumeOne(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("consumeOne() error = %v, want wrapped %v", err, want)
	}
	if reader.commitCalls != 0 {
		t.Errorf("commit calls = %d, want 0", reader.commitCalls)
	}
}

func TestConsumeOneDoesNotCommitCommitFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("commit unavailable")
	reader := &fakeReader{message: validMessage(t), commitErr: want}
	store := &fakeStore{inserted: true}
	service := newTestService(reader, store)

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
	store := &fakeStore{}
	service := newTestService(reader, store)

	if err := service.consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want decoding error")
	}
	if store.calls != 0 {
		t.Errorf("store calls = %d, want 0", store.calls)
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
	store := &fakeStore{}
	service := newTestService(reader, store)

	if err := service.consumeOne(context.Background()); err == nil {
		t.Fatal("consumeOne() error = nil, want key validation error")
	}
	if store.calls != 0 || reader.commitCalls != 0 {
		t.Errorf("store calls = %d, commit calls = %d, want both zero", store.calls, reader.commitCalls)
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

func newTestService(reader messageReader, store historyStore) *Service {
	return NewService(
		reader,
		store,
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

type fakeStore struct {
	inserted bool
	err      error
	calls    int
	order    *[]string
}

func (s *fakeStore) Insert(context.Context, event.CheckResult) (bool, error) {
	s.calls++
	if s.order != nil {
		*s.order = append(*s.order, "store")
	}
	return s.inserted, s.err
}
