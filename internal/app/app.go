// Package app contains the shared process bootstrap used by Pulse services.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Olzerq/Pulse/internal/config"
	"github.com/Olzerq/Pulse/internal/logging"
)

// Runner is the service-specific part of a Pulse process. It must return after
// ctx is cancelled so the process can shut down gracefully.
type Runner func(context.Context, config.Config, *slog.Logger) error

// Run loads shared dependencies and executes a service until it stops. The
// returned value is suitable for os.Exit.
func Run(service string, runner Runner) int {
	cfg, err := config.Load(service)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "load configuration: %v\n", err)
		return 1
	}

	logger := logging.New(cfg.Service, cfg.Environment, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.InfoContext(ctx, "service starting")
	if err := runner(ctx, cfg, logger); err != nil {
		logger.Error("service stopped with error", "error", err)
		return 1
	}

	logger.Info("service stopped")
	return 0
}
