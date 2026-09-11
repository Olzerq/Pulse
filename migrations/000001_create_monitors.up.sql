CREATE TABLE monitors (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    url text NOT NULL,
    method varchar(8) NOT NULL DEFAULT 'GET',
    interval_seconds integer NOT NULL DEFAULT 60,
    timeout_ms integer NOT NULL DEFAULT 5000,
    enabled boolean NOT NULL DEFAULT true,
    expected_status_code smallint NOT NULL DEFAULT 200,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT monitors_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT monitors_method_supported CHECK (method IN ('GET', 'HEAD')),
    CONSTRAINT monitors_interval_range CHECK (interval_seconds BETWEEN 5 AND 86400),
    CONSTRAINT monitors_timeout_range CHECK (timeout_ms BETWEEN 100 AND 60000),
    CONSTRAINT monitors_timeout_within_interval CHECK (timeout_ms <= interval_seconds * 1000),
    CONSTRAINT monitors_expected_status_range CHECK (expected_status_code BETWEEN 100 AND 599)
);

CREATE INDEX monitors_enabled_idx ON monitors (id) WHERE enabled = true;
