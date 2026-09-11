// Package consumer owns the Kafka consumer process.
package consumer

import (
	"context"
	"log/slog"

	"pulse/internal/config"
)

// Run keeps the Stage 1 consumer alive until shutdown. Kafka consumption is
// introduced in Stage 5.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	logger.InfoContext(ctx, "consumer ready",
		"kafka_broker_count", len(cfg.KafkaBrokers),
		"topic", cfg.CheckResultTopic,
	)
	<-ctx.Done()
	logger.Info("consumer shutting down")
	return nil
}
