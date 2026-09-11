// Package kafka publishes Pulse domain events to Apache Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
)

const batchTimeout = 10 * time.Millisecond

type messageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// CheckResultPublisher serializes HTTP check results and writes them to one
// Kafka topic. The underlying kafka-go writer is safe for concurrent use by
// all Pinger workers.
type CheckResultPublisher struct {
	writer         messageWriter
	publishTimeout time.Duration
	newEventID     func() string
}

func NewCheckResultPublisher(
	brokers []string,
	topic string,
	publishTimeout time.Duration,
) *CheckResultPublisher {
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		BatchTimeout:           batchTimeout,
		ReadTimeout:            publishTimeout,
		WriteTimeout:           publishTimeout,
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}

	return newCheckResultPublisher(writer, publishTimeout, uuid.NewString)
}

func newCheckResultPublisher(
	writer messageWriter,
	publishTimeout time.Duration,
	newEventID func() string,
) *CheckResultPublisher {
	return &CheckResultPublisher{
		writer:         writer,
		publishTimeout: publishTimeout,
		newEventID:     newEventID,
	}
}

// Publish writes one synchronous message and returns only after Kafka has
// acknowledged it or the publish timeout has elapsed.
func (p *CheckResultPublisher) Publish(
	ctx context.Context,
	result check.Result,
) (event.CheckResult, error) {
	payload := event.NewCheckResult(p.newEventID(), result)
	value, err := json.Marshal(payload)
	if err != nil {
		return payload, fmt.Errorf("marshal %s event: %w", event.CheckResultType, err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
	defer cancel()

	message := kafkago.Message{
		Key:   []byte(payload.MonitorID),
		Value: value,
		Time:  payload.CheckedAt,
		Headers: []kafkago.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "event-type", Value: []byte(event.CheckResultType)},
		},
	}
	if err := p.writer.WriteMessages(publishCtx, message); err != nil {
		return payload, fmt.Errorf("write %s event: %w", event.CheckResultType, err)
	}

	return payload, nil
}

func (p *CheckResultPublisher) Close() error {
	return p.writer.Close()
}
