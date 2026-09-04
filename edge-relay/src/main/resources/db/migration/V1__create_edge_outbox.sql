CREATE TABLE outbox_event (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    machine_boot_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number >= 0),
    schema_version INTEGER NOT NULL,
    source_payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'SENT', 'REJECTED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TEXT NOT NULL,
    last_attempt_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    rejected_at TEXT,
    UNIQUE (tenant_id, site_id, machine_id, machine_boot_id, sequence_number)
);

CREATE INDEX idx_outbox_dispatch ON outbox_event (state, next_attempt_at, row_id);
CREATE INDEX idx_outbox_machine_order ON outbox_event (tenant_id, site_id, machine_id, state, row_id);

CREATE TABLE edge_rejected_message (
    rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_excerpt TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_message TEXT NOT NULL
);
