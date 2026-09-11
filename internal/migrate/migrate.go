// Package migrate applies embedded PostgreSQL schema migrations.
package migrate

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Olzerq/Pulse/internal/config"
	"github.com/Olzerq/Pulse/internal/postgres"
	"github.com/Olzerq/Pulse/migrations"
	"github.com/jackc/pgx/v5"
)

const advisoryLockID int64 = 735_756_523_002_051

// Run applies every migration not yet recorded in schema_migrations.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.Open(ctx, cfg.PostgresURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	connection, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire PostgreSQL connection: %w", err)
	}
	defer connection.Release()

	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := connection.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, advisoryLockID); err != nil {
			logger.Error("release migration lock", "error", err)
		}
	}()

	if _, err := connection.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY,
			name text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	items, err := migrations.All()
	if err != nil {
		return err
	}
	for _, item := range items {
		applied, err := migrationApplied(ctx, connection, item.Version)
		if err != nil {
			return err
		}
		if applied {
			logger.DebugContext(ctx, "migration already applied", "version", item.Version, "name", item.Name)
			continue
		}

		if err := applyMigration(ctx, connection, item); err != nil {
			return err
		}
		logger.InfoContext(ctx, "migration applied", "version", item.Version, "name", item.Name)
	}

	return nil
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

func migrationApplied(ctx context.Context, connection queryer, version int64) (bool, error) {
	var applied bool
	err := connection.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		version,
	).Scan(&applied)
	if err != nil {
		return false, fmt.Errorf("check migration %d: %w", version, err)
	}
	return applied, nil
}

func applyMigration(ctx context.Context, connection beginner, item migrations.Migration) error {
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", item.Version, err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()

	if _, err := transaction.Exec(ctx, item.SQL); err != nil {
		return fmt.Errorf("apply migration %d: %w", item.Version, err)
	}
	if _, err := transaction.Exec(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
		item.Version,
		item.Name,
	); err != nil {
		return fmt.Errorf("record migration %d: %w", item.Version, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", item.Version, err)
	}
	return nil
}
