// Package pinger owns the HTTP checking worker process.
package pinger

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/config"
	pulsekafka "github.com/Olzerq/Pulse/internal/kafka"
	"github.com/Olzerq/Pulse/internal/observability"
	"github.com/Olzerq/Pulse/internal/postgres"
	pulseredis "github.com/Olzerq/Pulse/internal/redis"
	"github.com/google/uuid"
)

// Run starts the distributed scheduler, HTTP worker pool, and Kafka publisher.
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

	checker := check.NewChecker(cfg.PingerWorkers, cfg.PingerUserAgent)
	defer checker.CloseIdleConnections()
	publisher := pulsekafka.NewCheckResultPublisher(
		cfg.KafkaBrokers,
		cfg.CheckResultTopic,
		cfg.KafkaPublishTimeout,
	)
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Warn("close Kafka publisher", "error", err)
		}
	}()

	store := postgres.NewMonitorStore(pool)
	stateStore := pulseredis.NewStateStore(redisClient)
	metrics := observability.NewMetrics(cfg.Service)
	instanceID := newInstanceID()
	scheduler := NewScheduler(
		store,
		checker,
		publisher,
		pulseredis.NewLockStore(redisClient),
		logger,
		SchedulerConfig{
			InstanceID:           instanceID,
			PollInterval:         cfg.PingerPoll,
			WorkerCount:          cfg.PingerWorkers,
			LockGrace:            cfg.PingerLockGrace,
			PublishTimeout:       cfg.KafkaPublishTimeout,
			LockOperationTimeout: cfg.RedisOperationTimeout,
			StateCounter:         stateStore,
			Metrics:              metrics,
		},
	)

	logger.InfoContext(ctx, "pinger ready",
		"instance_id", instanceID,
		"poll_interval", cfg.PingerPoll,
		"max_concurrency", cfg.PingerWorkers,
		"lock_grace", cfg.PingerLockGrace,
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
		"publish_timeout", cfg.KafkaPublishTimeout,
	)
	healthHandler := observability.NewHandler(metrics, map[string]observability.Check{
		"postgres": pool.Ping,
		"redis":    redisClient.Ping,
		"kafka": func(checkCtx context.Context) error {
			return pulsekafka.Ping(checkCtx, cfg.KafkaBrokers)
		},
	}, cfg.HealthCheckTimeout)
	return observability.RunTogether(
		ctx,
		scheduler.Run,
		func(serverCtx context.Context) error {
			return observability.Serve(
				serverCtx,
				cfg.ObservabilityAddr,
				cfg.ShutdownTimeout,
				healthHandler,
				logger,
			)
		},
	)
}

func newInstanceID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "pinger"
	}
	return hostname + "-" + uuid.NewString()
}
