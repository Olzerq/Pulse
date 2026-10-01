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

// ListRecent returns the newest check results for one monitor. History is kept
// in PostgreSQL even after a monitor is deleted, so this method deliberately
// does not join the monitors table.
func (s *CheckStore) ListRecent(ctx context.Context, monitorID string, limit int) ([]event.CheckResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			event_id::text,
			monitor_id::text,
			checked_at,
			success,
			status_code,
			latency_ms,
			error_kind,
			error
		FROM checks
		WHERE monitor_id = $1
		ORDER BY checked_at DESC, event_id DESC
		LIMIT $2`, monitorID, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent check results: %w", err)
	}
	defer rows.Close()

	results := make([]event.CheckResult, 0, limit)
	for rows.Next() {
		var result event.CheckResult
		var statusCode *int
		var errorKind *string
		if err := rows.Scan(
			&result.EventID,
			&result.MonitorID,
			&result.CheckedAt,
			&result.Success,
			&statusCode,
			&result.LatencyMS,
			&errorKind,
			&result.Error,
		); err != nil {
			return nil, fmt.Errorf("scan recent check result: %w", err)
		}
		if statusCode != nil {
			result.StatusCode = *statusCode
		}
		if errorKind != nil {
			result.ErrorKind = check.FailureKind(*errorKind)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent check results: %w", err)
	}

	return results, nil
}
