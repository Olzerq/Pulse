package postgres

import (
	"context"
	"fmt"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/event"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckStore appends check history to PostgreSQL. Duplicate event IDs are a
// successful no-op so at-least-once Kafka delivery remains idempotent.
type CheckStore struct {
	pool *pgxpool.Pool
}

func NewCheckStore(pool *pgxpool.Pool) *CheckStore {
	return &CheckStore{pool: pool}
}

// Insert returns true when a new row was written and false when event_id was
// already present.
func (s *CheckStore) Insert(ctx context.Context, result event.CheckResult) (bool, error) {
	var statusCode any
	if result.StatusCode != 0 {
		statusCode = result.StatusCode
	}

	var errorKind any
	if result.ErrorKind != check.FailureNone {
		errorKind = string(result.ErrorKind)
	}

	var errorDescription any
	if result.Error != nil {
		errorDescription = *result.Error
	}

	commandTag, err := s.pool.Exec(ctx, `
		INSERT INTO checks (
			event_id,
			monitor_id,
			checked_at,
			success,
			status_code,
			latency_ms,
			error_kind,
			error
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (event_id) DO NOTHING`,
		result.EventID,
		result.MonitorID,
		result.CheckedAt,
		result.Success,
		statusCode,
		result.LatencyMS,
		errorKind,
		errorDescription,
	)
	if err != nil {
		return false, fmt.Errorf("insert check result: %w", err)
	}

	return commandTag.RowsAffected() == 1, nil
}
