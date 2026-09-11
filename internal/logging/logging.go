// Package logging constructs the structured logger used by Pulse services.
package logging

import (
	"log/slog"
	"os"
)

// New returns a JSON logger with stable fields that identify the process.
func New(service, environment, level string) *slog.Logger {
	options := &slog.HandlerOptions{Level: parseLevel(level)}
	handler := slog.NewJSONHandler(os.Stdout, options)

	return slog.New(handler).With(
		"service", service,
		"environment", environment,
	)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
