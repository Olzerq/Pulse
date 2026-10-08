//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	pulsekafka "github.com/Olzerq/Pulse/internal/kafka"
	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

func TestKafkaPublisherAndGroupReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	broker := environment("PULSE_INTEGRATION_KAFKA_BROKER", "localhost:9092")
	topic := "pulse-integration-" + uuid.NewString()
	groupID := "pulse-integration-" + uuid.NewString()
	admin := &kafkago.Client{Addr: kafkago.TCP(broker), Timeout: 10 * time.Second}

	created, err := admin.CreateTopics(ctx, &kafkago.CreateTopicsRequest{Topics: []kafkago.TopicConfig{{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}}})
	if err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}
	if topicErr := created.Errors[topic]; topicErr != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, topicErr)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.DeleteTopics(cleanupCtx, &kafkago.DeleteTopicsRequest{Topics: []string{topic}})
	})

	publisher := pulsekafka.NewCheckResultPublisher([]string{broker}, topic, 10*time.Second)
	t.Cleanup(func() { _ = publisher.Close() })
	reader := pulsekafka.NewCheckResultReader([]string{broker}, topic, groupID)
	t.Cleanup(func() { _ = reader.Close() })

	result := check.Result{
		MonitorID:  uuid.NewString(),
		CheckedAt:  time.Now().UTC().Truncate(time.Millisecond),
		Success:    true,
		StatusCode: 200,
		LatencyMS:  27,
	}
	published, err := publisher.Publish(ctx, result)
	if err != nil {
		t.Fatalf("publish check result: %v", err)
	}

	message, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("fetch check result: %v", err)
	}
	if string(message.Key) != result.MonitorID {
		t.Errorf("Kafka key = %q, want %q", message.Key, result.MonitorID)
	}
	if header(message.Headers, "event-type") != event.CheckResultType {
		t.Errorf("event-type header = %q, want %q", header(message.Headers, "event-type"), event.CheckResultType)
	}

	var decoded event.CheckResult
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatalf("decode check result: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("validate check result: %v", err)
	}
	if decoded.EventID != published.EventID || decoded.MonitorID != result.MonitorID || decoded.LatencyMS != result.LatencyMS {
		t.Fatalf("decoded event = %#v, want published event %#v", decoded, published)
	}
	if err := reader.CommitMessages(ctx, message); err != nil {
		t.Fatalf("commit Kafka message: %v", err)
	}
}

func header(headers []kafkago.Header, name string) string {
	for _, item := range headers {
		if item.Key == name {
			return string(item.Value)
		}
	}
	return ""
}
