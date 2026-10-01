package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

func Serve(
	ctx context.Context,
	address string,
	shutdownTimeout time.Duration,
	handler http.Handler,
	logger *slog.Logger,
) error {
	server := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "observability server listening", "address", address)
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve observability HTTP: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shut down observability HTTP: %w", err)
	}
	if err := <-errCh; err != nil {
		return fmt.Errorf("serve observability HTTP: %w", err)
	}
	return nil
}

// RunTogether ties a worker and its observability server to one lifecycle. If
// either side fails, the other is cancelled and both are allowed to drain.
func RunTogether(
	ctx context.Context,
	worker func(context.Context) error,
	server func(context.Context) error,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan error, 2)
	go func() { results <- worker(runCtx) }()
	go func() { results <- server(runCtx) }()

	first := <-results
	cancel()
	second := <-results
	if ctx.Err() != nil {
		return nil
	}
	return errors.Join(first, second)
}
