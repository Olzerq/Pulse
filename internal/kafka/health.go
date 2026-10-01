package kafka

import (
	"context"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// Ping succeeds when at least one configured broker accepts a connection.
func Ping(ctx context.Context, brokers []string) error {
	var errs []error
	dialer := &kafkago.Dialer{}
	for _, broker := range brokers {
		connection, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			errs = append(errs, fmt.Errorf("connect to Kafka broker %s: %w", broker, err))
			continue
		}
		if err := connection.Close(); err != nil {
			return fmt.Errorf("close Kafka health connection: %w", err)
		}
		return nil
	}
	return errors.Join(errs...)
}
