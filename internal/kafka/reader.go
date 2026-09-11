package kafka

import (
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const maxCheckResultBytes = 1024 * 1024

// NewCheckResultReader creates a group reader with explicit synchronous
// commits. The Consumer uses FetchMessage and only commits after PostgreSQL has
// accepted the event.
func NewCheckResultReader(brokers []string, topic, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:               brokers,
		GroupID:               groupID,
		Topic:                 topic,
		QueueCapacity:         1,
		MinBytes:              1,
		MaxBytes:              maxCheckResultBytes,
		MaxWait:               time.Second,
		CommitInterval:        0,
		WatchPartitionChanges: true,
		StartOffset:           kafkago.FirstOffset,
	})
}
