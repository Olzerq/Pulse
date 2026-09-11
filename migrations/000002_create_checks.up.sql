CREATE TABLE checks (
    event_id uuid PRIMARY KEY,
    monitor_id uuid NOT NULL,
    checked_at timestamptz NOT NULL,
    success boolean NOT NULL,
    status_code smallint,
    latency_ms integer NOT NULL,
    error_kind text,
    error text,
    received_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT checks_status_code_range CHECK (
        status_code IS NULL OR status_code BETWEEN 100 AND 599
    ),
    CONSTRAINT checks_latency_non_negative CHECK (latency_ms >= 0),
    CONSTRAINT checks_success_has_status CHECK (NOT success OR status_code IS NOT NULL),
    CONSTRAINT checks_success_has_no_error CHECK (
        NOT success OR (error_kind IS NULL AND error IS NULL)
    )
);

-- There is deliberately no foreign key to monitors. A result can already be
-- in Kafka when its monitor is deleted, and history must still be consumable.
CREATE INDEX checks_monitor_checked_at_idx
    ON checks (monitor_id, checked_at DESC, event_id);
