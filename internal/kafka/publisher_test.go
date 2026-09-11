package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
)

func TestCheckResultPublisherWritesKeyedJSONEvent(t *testing.T) {
	t.Parallel()

	writer := &recordingWriter{}
	publisher := newCheckResultPublisher(writer, time.Second, func() string { return "event-id" })
	checkedAt := time.Date(2026, time.September, 10, 16, 30, 0, 0, time.UTC)
	result := check.Result{
		MonitorID:  "monitor-id",
		CheckedAt:  checkedAt,
		Success:    true,
		StatusCode: 200,
		LatencyMS:  142,
	}

	published, err := publisher.Publish(context.Background(), result)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if published.EventID != "event-id" {
		t.Errorf("EventID = %q, want event-id", published.EventID)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("message count = %d, want 1", len(writer.messages))
	}

	message := writer.messages[0]
	if got := string(message.Key); got != result.MonitorID {
		t.Errorf("message key = %q, want %q", got, result.MonitorID)
	}
	if !message.Time.Equal(checkedAt) {
		t.Errorf("message time = %v, want %v", message.Time, checkedAt)
	}

	var decoded event.CheckResult
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if decoded.EventID != "event-id" || decoded.MonitorID != result.MonitorID {
		t.Errorf("decoded event identifiers = %#v", decoded)
	}
	if decoded.Error != nil {
		t.Errorf("decoded error = %q, want nil", *decoded.Error)
	}
	if got := headerValue(message.Headers, "event-type"); got != event.CheckResultType {
		t.Errorf("event-type header = %q, want %q", got, event.CheckResultType)
	}
}

func TestCheckResultPublisherReturnsWriterError(t *testing.T) {
	t.Parallel()

	want := errors.New("kafka unavailable")
	writer := &recordingWriter{err: want}
	publisher := newCheckResultPublisher(writer, time.Second, func() string { return "event-id" })

	published, err := publisher.Publish(context.Background(), check.Result{MonitorID: "monitor-id"})
	if !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want wrapped %v", err, want)
	}
	if published.EventID != "event-id" {
		t.Errorf("EventID = %q, want event-id", published.EventID)
	}
}

type recordingWriter struct {
	messages []kafkago.Message
	err      error
}

func (w *recordingWriter) WriteMessages(_ context.Context, messages ...kafkago.Message) error {
	w.messages = append(w.messages, messages...)
	return w.err
}

func (w *recordingWriter) Close() error { return nil }

func headerValue(headers []kafkago.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
