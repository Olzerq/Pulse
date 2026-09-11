// Package pinger owns the HTTP checking worker process.
package pinger

import (
	"context"
	"log/slog"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/config"
	"github.com/Olzerq/Pulse/internal/postgres"
)

// Run starts the single-instance Stage 3 scheduler and HTTP worker pool.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	checker := check.NewChecker(cfg.PingerWorkers, cfg.PingerUserAgent)
	defer checker.CloseIdleConnections()

	store := postgres.NewMonitorStore(pool)
	scheduler := NewScheduler(store, checker, logger, cfg.PingerPoll, cfg.PingerWorkers)

	logger.InfoContext(ctx, "pinger ready",
		"poll_interval", cfg.PingerPoll,
		"max_concurrency", cfg.PingerWorkers,
	)
	return scheduler.Run(ctx)
}
