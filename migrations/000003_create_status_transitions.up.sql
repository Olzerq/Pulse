CREATE TABLE status_transitions (
    event_id uuid PRIMARY KEY REFERENCES checks(event_id) ON DELETE CASCADE,
    monitor_id uuid NOT NULL,
    previous_status text NOT NULL,
    new_status text NOT NULL,
    changed_at timestamptz NOT NULL,
    notification_status text NOT NULL DEFAULT 'pending',
    notification_attempts integer NOT NULL DEFAULT 0,
    telegram_message_id bigint,
    notified_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT status_transitions_previous_status CHECK (previous_status IN ('UP', 'DOWN')),
    CONSTRAINT status_transitions_new_status CHECK (new_status IN ('UP', 'DOWN')),
    CONSTRAINT status_transitions_status_changed CHECK (previous_status <> new_status),
    CONSTRAINT status_transitions_notification_status CHECK (
        notification_status IN ('pending', 'sent', 'skipped')
    ),
    CONSTRAINT status_transitions_attempts_non_negative CHECK (notification_attempts >= 0)
);

-- A monitor may be deleted while its result is still in Kafka, so this table
-- deliberately has no foreign key to monitors.
CREATE INDEX status_transitions_monitor_changed_at_idx
    ON status_transitions (monitor_id, changed_at DESC, event_id);
