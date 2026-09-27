package postgres

import (
	"context"
	"fmt"

	"github.com/Olzerq/Pulse/internal/monitorstate"
	"github.com/Olzerq/Pulse/internal/notification"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransitionStore struct {
	pool *pgxpool.Pool
}

func NewTransitionStore(pool *pgxpool.Pool) *TransitionStore {
	return &TransitionStore{pool: pool}
}

func (s *TransitionStore) Prepare(
	ctx context.Context,
	transition monitorstate.Transition,
) (notification.Delivery, error) {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO status_transitions (
			event_id,
			monitor_id,
			previous_status,
			new_status,
			changed_at
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		transition.EventID,
		transition.MonitorID,
		transition.Previous,
		transition.Current,
		transition.ChangedAt,
	)
	if err != nil {
		return notification.Delivery{}, fmt.Errorf("insert status transition: %w", err)
	}

	var status notification.DeliveryStatus
	err = s.pool.QueryRow(ctx, `
		SELECT notification_status
		FROM status_transitions
		WHERE event_id = $1`,
		transition.EventID,
	).Scan(&status)
	if err != nil {
		return notification.Delivery{}, fmt.Errorf("query status transition: %w", err)
	}

	return notification.Delivery{Transition: transition, Status: status}, nil
}

func (s *TransitionStore) MarkSent(ctx context.Context, eventID string, messageID int64) error {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE status_transitions
		SET notification_status = 'sent',
			notification_attempts = notification_attempts + 1,
			telegram_message_id = $2,
			notified_at = now(),
			last_error = NULL
		WHERE event_id = $1`,
		eventID,
		messageID,
	)
	if err != nil {
		return fmt.Errorf("mark status transition sent: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("mark status transition sent: event %s not found", eventID)
	}
	return nil
}

func (s *TransitionStore) MarkSkipped(ctx context.Context, eventID string) error {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE status_transitions
		SET notification_status = 'skipped',
			last_error = NULL
		WHERE event_id = $1`,
		eventID,
	)
	if err != nil {
		return fmt.Errorf("mark status transition skipped: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("mark status transition skipped: event %s not found", eventID)
	}
	return nil
}

func (s *TransitionStore) RecordFailure(ctx context.Context, eventID, description string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE status_transitions
		SET notification_attempts = notification_attempts + 1,
			last_error = $2
		WHERE event_id = $1`,
		eventID,
		description,
	)
	if err != nil {
		return fmt.Errorf("record notification failure: %w", err)
	}
	return nil
}
