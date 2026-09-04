CREATE TABLE cycle_event (
    event_id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    site_id VARCHAR(64) NOT NULL,
    machine_id VARCHAR(64) NOT NULL,
    machine_boot_id UUID NOT NULL,
    sequence_number BIGINT NOT NULL CHECK (sequence_number >= 0),
    schema_version INTEGER NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    edge_received_at TIMESTAMPTZ NOT NULL,
    cloud_received_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    cycle_number BIGINT NOT NULL,
    total_duration_ms BIGINT NOT NULL,
    plasticizing_duration_ms BIGINT NOT NULL,
    injection_duration_ms BIGINT NOT NULL,
    cooling_duration_ms BIGINT NOT NULL,
    demolding_duration_ms BIGINT NOT NULL,
    peak_injection_pressure_bar DOUBLE PRECISION NOT NULL,
    melt_temperature_c DOUBLE PRECISION NOT NULL,
    good_parts BIGINT NOT NULL,
    rejected_parts BIGINT NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    UNIQUE (tenant_id, machine_id, machine_boot_id, sequence_number)
);

CREATE INDEX idx_cycle_history ON cycle_event (tenant_id, machine_id, occurred_at DESC, event_id);
CREATE UNIQUE INDEX idx_cycle_kafka_position ON cycle_event (kafka_partition, kafka_offset);

CREATE TABLE machine_data_quality (
    tenant_id VARCHAR(64) NOT NULL,
    machine_id VARCHAR(64) NOT NULL,
    current_boot_id UUID NOT NULL,
    last_sequence_number BIGINT NOT NULL,
    detected_gap_count BIGINT NOT NULL DEFAULT 0,
    last_detected_gap BIGINT NOT NULL DEFAULT 0,
    duplicate_event_count BIGINT NOT NULL DEFAULT 0,
    last_cloud_ingestion_at TIMESTAMPTZ NOT NULL,
    latest_event_id UUID NOT NULL REFERENCES cycle_event(event_id),
    PRIMARY KEY (tenant_id, machine_id)
);
