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
	"github.com/Olzerq/Pulse/internal/notification"
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
	Get(context.Context, string) (monitorstate.State, error)
	Set(context.Context, monitorstate.State) error
}

type transitionStore interface {
	Prepare(context.Context, monitorstate.Transition) (notification.Delivery, error)
	MarkSent(context.Context, string, int64) error
	MarkSkipped(context.Context, string) error
	RecordFailure(context.Context, string, string) error
}

// Service processes one message at a time. This makes the commit boundary
// unambiguous: a later offset can never be committed past an event that failed
// to reach PostgreSQL.
type Service struct {
	reader           messageReader
	history          historyStore
	states           stateStore
	transitions      transitionStore
	sender           notification.Sender
	notificationsOn  bool
	logger           *slog.Logger
	operationTimeout time.Duration
}

func NewService(
	reader messageReader,
	history historyStore,
	states stateStore,
	transitions transitionStore,
	sender notification.Sender,
	notificationsOn bool,
	logger *slog.Logger,
	operationTimeout time.Duration,
) *Service {
	return &Service{
		reader:           reader,
		history:          history,
		states:           states,
		transitions:      transitions,
		sender:           sender,
		notificationsOn:  notificationsOn,
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

	var sender notification.Sender
	if cfg.TelegramEnabled() {
		sender = notification.NewTelegramSender(
			cfg.TelegramBotToken,
			cfg.TelegramChatID,
			cfg.TelegramAPIURL,
			cfg.TelegramRequestTimeout,
		)
	} else {
		logger.InfoContext(ctx, "Telegram notifications disabled")
	}

	service := NewService(
		reader,
		postgres.NewCheckStore(pool),
		pulseredis.NewStateStore(redisClient),
		postgres.NewTransitionStore(pool),
		sender,
		cfg.TelegramEnabled(),
		logger,
		cfg.ConsumerProcessTimeout,
	)
	logger.InfoContext(ctx, "consumer ready",
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
		"consumer_group", cfg.KafkaConsumerGroup,
		"operation_timeout", cfg.ConsumerProcessTimeout,
		"redis_operation_timeout", cfg.RedisOperationTimeout,
		"telegram_notifications", cfg.TelegramEnabled(),
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

	result, state, inserted, transition, err := s.storeMessage(ctx, message)
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
	if transition != nil {
		s.logger.Info("status transition processed",
			"event_id", transition.EventID,
			"monitor_id", transition.MonitorID,
			"previous_status", transition.Previous,
			"new_status", transition.Current,
			"telegram_notifications", s.notificationsOn,
		)
	}

	return nil
}

func (s *Service) storeMessage(
	ctx context.Context,
	message kafkago.Message,
) (event.CheckResult, monitorstate.State, bool, *monitorstate.Transition, error) {
	var result event.CheckResult
	if err := json.Unmarshal(message.Value, &result); err != nil {
		return result, monitorstate.State{}, false, nil, fmt.Errorf("decode check.result: %w", err)
	}
	if err := result.Validate(); err != nil {
		return result, monitorstate.State{}, false, nil, fmt.Errorf("validate check.result: %w", err)
	}
	if string(message.Key) != result.MonitorID {
		return result, monitorstate.State{}, false, nil, fmt.Errorf(
			"Kafka key %q does not match monitor_id %q",
			message.Key,
			result.MonitorID,
		)
	}

	processCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	inserted, err := s.history.Insert(processCtx, result)
	cancel()
	if err != nil {
		return result, monitorstate.State{}, false, nil, err
	}

	stateCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	previous, err := s.states.Get(stateCtx, result.MonitorID)
	cancel()
	if err != nil {
		return result, monitorstate.State{}, inserted, nil, fmt.Errorf("load monitor state: %w", err)
	}

	state, transition := monitorstate.Advance(previous, result)
	if transition != nil {
		if err := s.processTransition(ctx, *transition); err != nil {
			return result, state, inserted, transition, err
		}
	}

	stateCtx, cancel = context.WithTimeout(ctx, s.operationTimeout)
	err = s.states.Set(stateCtx, state)
	cancel()
	if err != nil {
		return result, state, inserted, transition, fmt.Errorf("update monitor state: %w", err)
	}

	return result, state, inserted, transition, nil
}

func (s *Service) processTransition(ctx context.Context, transition monitorstate.Transition) error {
	operationCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	delivery, err := s.transitions.Prepare(operationCtx, transition)
	cancel()
	if err != nil {
		return err
	}
	if delivery.Status != notification.DeliveryPending {
		return nil
	}

	if !s.notificationsOn {
		operationCtx, cancel = context.WithTimeout(ctx, s.operationTimeout)
		err = s.transitions.MarkSkipped(operationCtx, transition.EventID)
		cancel()
		if err != nil {
			return fmt.Errorf("mark Telegram notification skipped: %w", err)
		}
		return nil
	}
	if s.sender == nil {
		return errors.New("Telegram notifications are enabled without a sender")
	}

	operationCtx, cancel = context.WithTimeout(ctx, s.operationTimeout)
	messageID, sendErr := s.sender.Send(operationCtx, transition)
	cancel()
	if sendErr != nil {
		failureCtx, failureCancel := context.WithTimeout(ctx, s.operationTimeout)
		recordErr := s.transitions.RecordFailure(failureCtx, transition.EventID, sendErr.Error())
		failureCancel()
		if recordErr != nil {
			return errors.Join(fmt.Errorf("send Telegram notification: %w", sendErr), recordErr)
		}
		return fmt.Errorf("send Telegram notification: %w", sendErr)
	}

	operationCtx, cancel = context.WithTimeout(ctx, s.operationTimeout)
	err = s.transitions.MarkSent(operationCtx, transition.EventID, messageID)
	cancel()
	if err != nil {
		return err
	}
	return nil
}
