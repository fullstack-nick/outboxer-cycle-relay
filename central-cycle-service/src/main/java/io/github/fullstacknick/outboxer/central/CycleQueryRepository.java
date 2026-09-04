package io.github.fullstacknick.outboxer.central;

import io.r2dbc.spi.Row;
import java.time.Instant;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
class CycleQueryRepository {

    private static final String CYCLE_COLUMNS = """
            SELECT schema_version, event_id, tenant_id, site_id, machine_id, machine_boot_id,
                   sequence_number, occurred_at, edge_received_at, cloud_received_at, ingested_at,
                   cycle_number, total_duration_ms, plasticizing_duration_ms, injection_duration_ms,
                   cooling_duration_ms, demolding_duration_ms, peak_injection_pressure_bar,
                   melt_temperature_c, good_parts, rejected_parts, energy_consumption_wh
            FROM cycle_event
            """;

    private final DatabaseClient database;

    CycleQueryRepository(DatabaseClient database) {
        this.database = database;
    }

    Mono<CycleResponse> latest(String tenantId, String machineId) {
        return database.sql(CYCLE_COLUMNS
                        + " WHERE tenant_id = :tenantId AND machine_id = :machineId"
                        + " ORDER BY occurred_at DESC, event_id DESC LIMIT 1")
                .bind("tenantId", tenantId)
                .bind("machineId", machineId)
                .map((row, metadata) -> cycle(row))
                .one();
    }

    Flux<CycleResponse> recent(String tenantId, String machineId, int limit) {
        return database.sql(CYCLE_COLUMNS
                        + " WHERE tenant_id = :tenantId AND machine_id = :machineId"
                        + " ORDER BY occurred_at DESC, event_id DESC LIMIT :limit")
                .bind("tenantId", tenantId)
                .bind("machineId", machineId)
                .bind("limit", limit)
                .map((row, metadata) -> cycle(row))
                .all();
    }

    Mono<DataQualityResponse> quality(String tenantId, String machineId) {
        return database.sql("""
                        SELECT machine_id, last_sequence_number, detected_gap_count,
                               duplicate_event_count, last_cloud_ingestion_at
                        FROM machine_data_quality
                        WHERE tenant_id = :tenantId AND machine_id = :machineId
                        """)
                .bind("tenantId", tenantId)
                .bind("machineId", machineId)
                .map((row, metadata) -> new DataQualityResponse(
                        row.get("machine_id", String.class),
                        required(row, "last_sequence_number", Long.class),
                        required(row, "detected_gap_count", Long.class),
                        required(row, "duplicate_event_count", Long.class),
                        required(row, "last_cloud_ingestion_at", Instant.class)))
                .one();
    }

    private static CycleResponse cycle(Row row) {
        return new CycleResponse(
                required(row, "schema_version", Integer.class),
                required(row, "event_id", UUID.class),
                row.get("tenant_id", String.class),
                row.get("site_id", String.class),
                row.get("machine_id", String.class),
                required(row, "machine_boot_id", UUID.class),
                required(row, "sequence_number", Long.class),
                required(row, "occurred_at", Instant.class),
                required(row, "edge_received_at", Instant.class),
                required(row, "cloud_received_at", Instant.class),
                required(row, "ingested_at", Instant.class),
                required(row, "cycle_number", Long.class),
                required(row, "total_duration_ms", Long.class),
                required(row, "plasticizing_duration_ms", Long.class),
                required(row, "injection_duration_ms", Long.class),
                required(row, "cooling_duration_ms", Long.class),
                required(row, "demolding_duration_ms", Long.class),
                required(row, "peak_injection_pressure_bar", Double.class),
                required(row, "melt_temperature_c", Double.class),
                required(row, "good_parts", Long.class),
                required(row, "rejected_parts", Long.class),
                row.get("energy_consumption_wh", Double.class));
    }

    private static <T> T required(Row row, String column, Class<T> type) {
        T value = row.get(column, type);
        if (value == null) {
            throw new IllegalStateException("Database returned null for " + column);
        }
        return value;
    }
}
