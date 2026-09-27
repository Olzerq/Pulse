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
	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/postgres"
	pulseredis "github.com/Olzerq/Pulse/internal/redis"
)

type messageReader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type historyStore interface {
	Insert(context.Context, event.CheckResult) (bool, error)
}

type stateStore interface {
	Set(context.Context, monitorstate.State) error
}

// Service processes one message at a time. This makes the commit boundary
// unambiguous: a later offset can never be committed past an event that failed
// to reach PostgreSQL or Redis.
type Service struct {
	reader           messageReader
	history          historyStore
	states           stateStore
	logger           *slog.Logger
	operationTimeout time.Duration
}

func NewService(
	reader messageReader,
	history historyStore,
	states stateStore,
	logger *slog.Logger,
	operationTimeout time.Duration,
) *Service {
	return &Service{
		reader:           reader,
		history:          history,
		states:           states,
		logger:           logger,
		operationTimeout: operationTimeout,
	}
}

// Run starts the Kafka to PostgreSQL and Redis pipeline.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	redisClient, err := pulseredis.Open(ctx, cfg.RedisAddr, cfg.RedisOperationTimeout)
	if err != nil {
		return err
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Warn("close Redis client", "error", err)
		}
	}()

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
		pulseredis.NewStateStore(redisClient),
		logger,
		cfg.ConsumerProcessTimeout,
	)
	logger.InfoContext(ctx, "consumer ready",
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
		"consumer_group", cfg.KafkaConsumerGroup,
		"operation_timeout", cfg.ConsumerProcessTimeout,
		"redis_operation_timeout", cfg.RedisOperationTimeout,
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

	result, state, inserted, err := s.storeMessage(ctx, message)
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
		"status", state.Status,
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
) (event.CheckResult, monitorstate.State, bool, error) {
	var result event.CheckResult
	if err := json.Unmarshal(message.Value, &result); err != nil {
		return result, monitorstate.State{}, false, fmt.Errorf("decode check.result: %w", err)
	}
	if err := result.Validate(); err != nil {
		return result, monitorstate.State{}, false, fmt.Errorf("validate check.result: %w", err)
	}
	if string(message.Key) != result.MonitorID {
		return result, monitorstate.State{}, false, fmt.Errorf(
			"Kafka key %q does not match monitor_id %q",
			message.Key,
			result.MonitorID,
		)
	}

	processCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	inserted, err := s.history.Insert(processCtx, result)
	cancel()
	if err != nil {
		return result, monitorstate.State{}, false, err
	}

	state := monitorstate.FromCheck(result)
	stateCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	err = s.states.Set(stateCtx, state)
	cancel()
	if err != nil {
		return result, state, inserted, fmt.Errorf("update monitor state: %w", err)
	}

	return result, state, inserted, nil
}
