package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Olzerq/Pulse/internal/monitor"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const monitorColumns = `
	id::text,
	name,
	url,
	method,
	interval_seconds,
	timeout_ms,
	enabled,
	expected_status_code,
	created_at,
	updated_at`

// MonitorStore persists monitors in PostgreSQL.
type MonitorStore struct {
	pool *pgxpool.Pool
}

func NewMonitorStore(pool *pgxpool.Pool) *MonitorStore {
	return &MonitorStore{pool: pool}
}

func (s *MonitorStore) List(ctx context.Context) ([]monitor.Monitor, error) {
	return s.list(ctx, `SELECT `+monitorColumns+` FROM monitors ORDER BY created_at DESC, id DESC`)
}

// ListActive returns only monitors that should be scheduled by the Pinger.
func (s *MonitorStore) ListActive(ctx context.Context) ([]monitor.Monitor, error) {
	return s.list(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE enabled = true ORDER BY id`)
}

func (s *MonitorStore) list(ctx context.Context, query string) ([]monitor.Monitor, error) {
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query monitors: %w", err)
	}
	defer rows.Close()

	monitors := make([]monitor.Monitor, 0)
	for rows.Next() {
		item, err := scanMonitor(rows)
		if err != nil {
			return nil, fmt.Errorf("scan monitor: %w", err)
		}
		monitors = append(monitors, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate monitors: %w", err)
	}

	return monitors, nil
}

func (s *MonitorStore) Get(ctx context.Context, id string) (monitor.Monitor, error) {
	item, err := scanMonitor(s.pool.QueryRow(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return monitor.Monitor{}, monitor.ErrNotFound
	}
	if err != nil {
		return monitor.Monitor{}, fmt.Errorf("query monitor: %w", err)
	}
	return item, nil
}

func (s *MonitorStore) Create(ctx context.Context, item monitor.Monitor) (monitor.Monitor, error) {
	created, err := scanMonitor(s.pool.QueryRow(ctx, `
		INSERT INTO monitors (
			name,
			url,
			method,
			interval_seconds,
			timeout_ms,
			enabled,
			expected_status_code
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+monitorColumns,
		item.Name,
		item.URL,
		item.Method,
		item.IntervalSeconds,
		item.TimeoutMS,
		item.Enabled,
		item.ExpectedStatusCode,
	))
	if err != nil {
		return monitor.Monitor{}, fmt.Errorf("insert monitor: %w", err)
	}
	return created, nil
}

func (s *MonitorStore) Update(ctx context.Context, item monitor.Monitor) (monitor.Monitor, error) {
	updated, err := scanMonitor(s.pool.QueryRow(ctx, `
		UPDATE monitors
		SET name = $2,
			url = $3,
			method = $4,
			interval_seconds = $5,
			timeout_ms = $6,
			enabled = $7,
			expected_status_code = $8,
			updated_at = now()
		WHERE id = $1
		RETURNING `+monitorColumns,
		item.ID,
		item.Name,
		item.URL,
		item.Method,
		item.IntervalSeconds,
		item.TimeoutMS,
		item.Enabled,
		item.ExpectedStatusCode,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return monitor.Monitor{}, monitor.ErrNotFound
	}
	if err != nil {
		return monitor.Monitor{}, fmt.Errorf("update monitor: %w", err)
	}
	return updated, nil
}

func (s *MonitorStore) Delete(ctx context.Context, id string) error {
	commandTag, err := s.pool.Exec(ctx, `DELETE FROM monitors WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete monitor: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return monitor.ErrNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMonitor(row rowScanner) (monitor.Monitor, error) {
	var item monitor.Monitor
	err := row.Scan(
		&item.ID,
		&item.Name,
		&item.URL,
		&item.Method,
		&item.IntervalSeconds,
		&item.TimeoutMS,
		&item.Enabled,
		&item.ExpectedStatusCode,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	return item, err
}
