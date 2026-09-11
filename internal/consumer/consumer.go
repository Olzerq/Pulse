// Package consumer owns the Kafka consumer process.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Olzerq/Pulse/internal/config"
	"github.com/Olzerq/Pulse/internal/event"
	pulsekafka "github.com/Olzerq/Pulse/internal/kafka"
	"github.com/Olzerq/Pulse/internal/postgres"
)

type messageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type historyStore interface {
	Insert(context.Context, event.CheckResult) (bool, error)
}

// Service processes one message at a time. This makes the commit boundary
// unambiguous: a later offset can never be committed past an event that failed
// to reach PostgreSQL.
type Service struct {
	reader           messageReader
	store            historyStore
	logger           *slog.Logger
	operationTimeout time.Duration
}

func NewService(
	reader messageReader,
	store historyStore,
	logger *slog.Logger,
	operationTimeout time.Duration,
) *Service {
	return &Service{
		reader:           reader,
		store:            store,
		logger:           logger,
		operationTimeout: operationTimeout,
	}
}

// Run starts the Stage 5 Kafka to PostgreSQL pipeline.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	reader := pulsekafka.NewCheckResultReader(
		cfg.KafkaBrokers,
		cfg.CheckResultTopic,
		cfg.KafkaConsumerGroup,
	)
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Warn("close Kafka reader", "error", err)
		}
	}()

	service := NewService(
		reader,
		postgres.NewCheckStore(pool),
		logger,
		cfg.ConsumerProcessTimeout,
	)
	logger.InfoContext(ctx, "consumer ready",
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
		"consumer_group", cfg.KafkaConsumerGroup,
		"operation_timeout", cfg.ConsumerProcessTimeout,
	)
	return service.Run(ctx)
}

func (s *Service) Run(ctx context.Context) error {
	for {
		if err := s.consumeOne(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				s.logger.Info("consumer shutting down")
				return nil
			}
			return err
		}
	}
}

func (s *Service) consumeOne(ctx context.Context) error {
	message, err := s.reader.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("fetch Kafka message: %w", err)
	}

	result, inserted, err := s.storeMessage(ctx, message)
	if err != nil {
		return fmt.Errorf(
			"process Kafka message topic=%s partition=%d offset=%d: %w",
			message.Topic,
			message.Partition,
			message.Offset,
			err,
		)
	}

	commitCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	err = s.reader.CommitMessages(commitCtx, message)
	cancel()
	if err != nil {
		return fmt.Errorf(
			"commit Kafka message topic=%s partition=%d offset=%d: %w",
			message.Topic,
			message.Partition,
			message.Offset,
			err,
		)
	}

	attributes := []any{
		"event_id", result.EventID,
		"monitor_id", result.MonitorID,
		"topic", message.Topic,
		"partition", message.Partition,
		"offset", message.Offset,
	}
	if inserted {
		s.logger.Info("check result stored", attributes...)
	} else {
		s.logger.Info("duplicate check result ignored", attributes...)
	}

	return nil
}

func (s *Service) storeMessage(
	ctx context.Context,
	message kafkago.Message,
) (event.CheckResult, bool, error) {
	var result event.CheckResult
	if err := json.Unmarshal(message.Value, &result); err != nil {
		return result, false, fmt.Errorf("decode check.result: %w", err)
	}
	if err := result.Validate(); err != nil {
		return result, false, fmt.Errorf("validate check.result: %w", err)
	}
	if string(message.Key) != result.MonitorID {
		return result, false, fmt.Errorf(
			"Kafka key %q does not match monitor_id %q",
			message.Key,
			result.MonitorID,
		)
	}

	processCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	inserted, err := s.store.Insert(processCtx, result)
	cancel()
	if err != nil {
		return result, false, err
	}
	return result, inserted, nil
}
