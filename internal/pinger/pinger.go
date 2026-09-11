// Package pinger owns the HTTP checking worker process.
package pinger

import (
	"context"
	"log/slog"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/config"
	pulsekafka "github.com/Olzerq/Pulse/internal/kafka"
	"github.com/Olzerq/Pulse/internal/postgres"
)

// Run starts the single-instance scheduler, HTTP worker pool, and Kafka
// publisher.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	checker := check.NewChecker(cfg.PingerWorkers, cfg.PingerUserAgent)
	defer checker.CloseIdleConnections()
	publisher := pulsekafka.NewCheckResultPublisher(
		cfg.KafkaBrokers,
		cfg.CheckResultTopic,
		cfg.KafkaPublishTime,
	)
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Warn("close Kafka publisher", "error", err)
		}
	}()

	store := postgres.NewMonitorStore(pool)
	scheduler := NewScheduler(store, checker, publisher, logger, cfg.PingerPoll, cfg.PingerWorkers)

	logger.InfoContext(ctx, "pinger ready",
		"poll_interval", cfg.PingerPoll,
		"max_concurrency", cfg.PingerWorkers,
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
		"publish_timeout", cfg.KafkaPublishTime,
	)
	return scheduler.Run(ctx)
}
