// Package pinger owns the HTTP checking worker process.
package pinger

import (
	"context"
	"log/slog"

	"github.com/Olzerq/Pulse/internal/config"
)

// Run keeps the Stage 1 worker alive until shutdown. Scheduling and HTTP
// checks are introduced in Stage 3.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	logger.InfoContext(ctx, "pinger ready",
		"kafka_broker_count", len(cfg.KafkaBrokers),
	)
	<-ctx.Done()
	logger.Info("pinger shutting down")
	return nil
}
