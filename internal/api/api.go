// Package api owns the HTTP process for Pulse.
package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Olzerq/Pulse/internal/config"
	"github.com/Olzerq/Pulse/internal/postgres"
)

// Run connects the API to PostgreSQL, starts the HTTP server, and drains it
// when the process context is cancelled.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.InfoContext(ctx, "connected to PostgreSQL")

	store := postgres.NewMonitorStore(pool)
	router := newRouter(logger, store)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "http server listening", "address", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Info("http server shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shut down HTTP server: %w", err)
	}

	if err := <-errCh; err != nil {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}
