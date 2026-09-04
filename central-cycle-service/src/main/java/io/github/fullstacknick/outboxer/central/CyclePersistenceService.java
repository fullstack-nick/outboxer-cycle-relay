package io.github.fullstacknick.outboxer.central;

import io.github.fullstacknick.outboxer.contract.CycleEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class CyclePersistenceService {

    enum Result {
        INSERTED,
        DUPLICATE,
        CONFLICT
    }

    record Outcome(Result result, long detectedGaps) {}

    record PersistCommand(
            CycleEvent event, String canonicalJson, int partition, long offset, Instant cloudReceivedAt) {}

    private static final String CYCLE_COLUMNS = """
            event_id, tenant_id, site_id, machine_id, machine_boot_id, sequence_number,
            schema_version, occurred_at, edge_received_at, cloud_received_at, ingested_at,
            cycle_number, total_duration_ms, plasticizing_duration_ms, injection_duration_ms,
            cooling_duration_ms, demolding_duration_ms, peak_injection_pressure_bar,
            melt_temperature_c, good_parts, rejected_parts, energy_consumption_wh,
            payload_sha256, kafka_partition, kafka_offset
            """;

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    CyclePersistenceService(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    @Transactional(transactionManager = "transactionManager")
    public Outcome persist(
            CycleEvent event,
            String canonicalJson,
            int partition,
            long offset,
            Instant cloudReceivedAt) {
        return persistBatch(List.of(new PersistCommand(event, canonicalJson, partition, offset, cloudReceivedAt)))
                .getFirst();
    }

    /**
     * Persists one Kafka poll as a set-based transaction. The usual all-new path has three database
     * round trips regardless of poll size: cycle insert, quality-state read, and quality-state write.
     * Duplicate accounting and conflict classification are also set-based when required.
     */
    @Transactional(transactionManager = "transactionManager")
    public List<Outcome> persistBatch(List<PersistCommand> commands) {
        if (commands.isEmpty()) {
            return List.of();
        }

        Instant ingestedAt = clock.instant();
        List<PreparedCommand> prepared = new ArrayList<>(commands.size());
        for (int index = 0; index < commands.size(); index++) {
            PersistCommand command = commands.get(index);
            prepared.add(new PreparedCommand(index, command, sha256(command.canonicalJson())));
        }

        Set<InsertedKey> inserted = bulkInsertCycles(prepared, ingestedAt);
        Outcome[] outcomes = new Outcome[prepared.size()];
        List<PreparedCommand> insertedCommands = new ArrayList<>(inserted.size());
        List<PreparedCommand> existingCommands = new ArrayList<>(prepared.size() - inserted.size());
        for (PreparedCommand command : prepared) {
            InsertedKey key = InsertedKey.from(command.command().event());
            if (inserted.remove(key)) {
                insertedCommands.add(command);
            } else {
                existingCommands.add(command);
            }
        }
        if (!inserted.isEmpty()) {
            throw new IllegalStateException("Bulk insert returned an event that could not be matched to its source");
        }

        updateQuality(insertedCommands, outcomes, ingestedAt);
        classifyExisting(existingCommands, outcomes, ingestedAt);

        List<Outcome> result = new ArrayList<>(outcomes.length);
        for (Outcome outcome : outcomes) {
            if (outcome == null) {
                throw new IllegalStateException("A batch persistence outcome was not assigned");
            }
            result.add(outcome);
        }
        return List.copyOf(result);
    }

    private Set<InsertedKey> bulkInsertCycles(List<PreparedCommand> commands, Instant ingestedAt) {
        String sql = "INSERT INTO cycle_event (" + CYCLE_COLUMNS + ") VALUES "
                + valueRows(commands.size(), 25)
                + " ON CONFLICT DO NOTHING RETURNING "
                + "event_id, tenant_id, machine_id, machine_boot_id, sequence_number";
        return jdbcTemplate.query(
                sql,
                statement -> {
                    int parameter = 1;
                    for (PreparedCommand command : commands) {
                        parameter = bindCycle(statement, parameter, command, ingestedAt);
                    }
                },
                resultSet -> {
                    Set<InsertedKey> keys = new HashSet<>();
                    while (resultSet.next()) {
                        keys.add(new InsertedKey(
                                resultSet.getObject("event_id", UUID.class),
                                new NaturalKey(
                                        resultSet.getString("tenant_id"),
                                        resultSet.getString("machine_id"),
                                        resultSet.getObject("machine_boot_id", UUID.class),
                                        resultSet.getLong("sequence_number"))));
                    }
                    return keys;
                });
    }

    private void updateQuality(List<PreparedCommand> inserted, Outcome[] outcomes, Instant ingestedAt) {
        if (inserted.isEmpty()) {
            return;
        }

        LinkedHashSet<MachineKey> requested = new LinkedHashSet<>();
        for (PreparedCommand command : inserted) {
            requested.add(MachineKey.from(command.command().event()));
        }
        Map<MachineKey, QualityAccumulator> states = readQuality(requested);

        for (PreparedCommand command : inserted) {
            CycleEvent event = command.command().event();
            MachineKey machine = MachineKey.from(event);
            QualityAccumulator state = states.get(machine);
            if (state == null) {
                state = QualityAccumulator.initial(event, ingestedAt);
                states.put(machine, state);
            }
            long gaps = state.advance(event, ingestedAt);
            outcomes[command.ordinal()] = new Outcome(Result.INSERTED, gaps);
        }
        upsertQuality(states);
    }

    private Map<MachineKey, QualityAccumulator> readQuality(Set<MachineKey> requested) {
        String row = "(?::varchar, ?::varchar)";
        String sql = """
                WITH requested(tenant_id, machine_id) AS (VALUES %s)
                SELECT quality.tenant_id, quality.machine_id, quality.current_boot_id,
                       quality.last_sequence_number, quality.detected_gap_count,
                       quality.last_detected_gap, quality.last_cloud_ingestion_at,
                       quality.latest_event_id
                FROM machine_data_quality quality
                JOIN requested USING (tenant_id, machine_id)
                """.formatted(repeatedRows(requested.size(), row));
        return jdbcTemplate.query(
                sql,
                statement -> {
                    int parameter = 1;
                    for (MachineKey machine : requested) {
                        statement.setString(parameter++, machine.tenantId());
                        statement.setString(parameter++, machine.machineId());
                    }
                },
                resultSet -> {
                    Map<MachineKey, QualityAccumulator> states = new HashMap<>();
                    while (resultSet.next()) {
                        MachineKey machine = new MachineKey(
                                resultSet.getString("tenant_id"), resultSet.getString("machine_id"));
                        states.put(
                                machine,
                                new QualityAccumulator(
                                        machine,
                                        resultSet.getObject("current_boot_id", UUID.class),
                                        resultSet.getLong("last_sequence_number"),
                                        resultSet.getLong("detected_gap_count"),
                                        resultSet.getLong("last_detected_gap"),
                                        resultSet.getObject("last_cloud_ingestion_at", OffsetDateTime.class).toInstant(),
                                        resultSet.getObject("latest_event_id", UUID.class),
                                        false));
                    }
                    return states;
                });
    }

    private void upsertQuality(Map<MachineKey, QualityAccumulator> states) {
        String sql = """
                INSERT INTO machine_data_quality (
                    tenant_id, machine_id, current_boot_id, last_sequence_number,
                    detected_gap_count, last_detected_gap, last_cloud_ingestion_at, latest_event_id
                ) VALUES %s
                ON CONFLICT (tenant_id, machine_id) DO UPDATE SET
                    current_boot_id = EXCLUDED.current_boot_id,
                    last_sequence_number = EXCLUDED.last_sequence_number,
                    detected_gap_count = EXCLUDED.detected_gap_count,
                    last_detected_gap = EXCLUDED.last_detected_gap,
                    last_cloud_ingestion_at = EXCLUDED.last_cloud_ingestion_at,
                    latest_event_id = EXCLUDED.latest_event_id
                """.formatted(valueRows(states.size(), 8));
        jdbcTemplate.update(sql, statement -> {
            int parameter = 1;
            for (QualityAccumulator state : states.values()) {
                statement.setString(parameter++, state.machine.tenantId());
                statement.setString(parameter++, state.machine.machineId());
                statement.setObject(parameter++, state.currentBootId);
                statement.setLong(parameter++, state.lastSequenceNumber);
                statement.setLong(parameter++, state.detectedGapCount);
                statement.setLong(parameter++, state.lastDetectedGap);
                statement.setObject(parameter++, sqlTimestamp(state.lastCloudIngestionAt));
                statement.setObject(parameter++, state.latestEventId);
            }
        });
    }

    private void classifyExisting(List<PreparedCommand> existing, Outcome[] outcomes, Instant ingestedAt) {
        if (existing.isEmpty()) {
            return;
        }
        Map<Integer, ExistingEvent> stored = readExisting(existing);
        Map<MachineKey, Long> duplicateCounts = new LinkedHashMap<>();
        for (PreparedCommand command : existing) {
            ExistingEvent candidate = stored.get(command.ordinal());
            if (candidate == null) {
                throw new IllegalStateException("A database uniqueness conflict could not be classified");
            }
            CycleEvent event = command.command().event();
            if (candidate.eventId().equals(event.eventId()) && candidate.payloadHash().equals(command.hash())) {
                outcomes[command.ordinal()] = new Outcome(Result.DUPLICATE, 0);
                duplicateCounts.merge(MachineKey.from(event), 1L, Long::sum);
            } else {
                outcomes[command.ordinal()] = new Outcome(Result.CONFLICT, 0);
            }
        }
        incrementDuplicates(duplicateCounts, ingestedAt);
    }

    private Map<Integer, ExistingEvent> readExisting(List<PreparedCommand> commands) {
        String row = "(?::integer, ?::uuid, ?::varchar, ?::varchar, ?::uuid, ?::bigint)";
        String sql = """
                WITH incoming(
                    ordinal, event_id, tenant_id, machine_id, machine_boot_id, sequence_number
                ) AS (VALUES %s)
                SELECT incoming.ordinal, stored.event_id, stored.payload_sha256
                FROM incoming
                JOIN LATERAL (
                    SELECT event.event_id, event.payload_sha256
                    FROM cycle_event event
                    WHERE event.event_id = incoming.event_id OR (
                        event.tenant_id = incoming.tenant_id
                        AND event.machine_id = incoming.machine_id
                        AND event.machine_boot_id = incoming.machine_boot_id
                        AND event.sequence_number = incoming.sequence_number
                    )
                    ORDER BY CASE WHEN event.event_id = incoming.event_id THEN 0 ELSE 1 END
                    LIMIT 1
                ) stored ON TRUE
                """.formatted(repeatedRows(commands.size(), row));
        return jdbcTemplate.query(
                sql,
                statement -> {
                    int parameter = 1;
                    for (PreparedCommand command : commands) {
                        CycleEvent event = command.command().event();
                        statement.setInt(parameter++, command.ordinal());
                        statement.setObject(parameter++, event.eventId());
                        statement.setString(parameter++, event.tenantId());
                        statement.setString(parameter++, event.machineId());
                        statement.setObject(parameter++, event.machineBootId());
                        statement.setLong(parameter++, event.sequenceNumber());
                    }
                },
                resultSet -> {
                    Map<Integer, ExistingEvent> events = new HashMap<>();
                    while (resultSet.next()) {
                        events.put(
                                resultSet.getInt("ordinal"),
                                new ExistingEvent(
                                        resultSet.getObject("event_id", UUID.class),
                                        resultSet.getString("payload_sha256")));
                    }
                    return events;
                });
    }

    private void incrementDuplicates(Map<MachineKey, Long> duplicateCounts, Instant ingestedAt) {
        if (duplicateCounts.isEmpty()) {
            return;
        }
        String row = "(?::varchar, ?::varchar, ?::bigint, ?::timestamptz)";
        String sql = """
                UPDATE machine_data_quality quality
                SET duplicate_event_count = quality.duplicate_event_count + incoming.duplicate_count,
                    last_cloud_ingestion_at = GREATEST(
                        quality.last_cloud_ingestion_at, incoming.last_cloud_ingestion_at
                    )
                FROM (VALUES %s) AS incoming(
                    tenant_id, machine_id, duplicate_count, last_cloud_ingestion_at
                )
                WHERE quality.tenant_id = incoming.tenant_id
                  AND quality.machine_id = incoming.machine_id
                """.formatted(repeatedRows(duplicateCounts.size(), row));
        jdbcTemplate.update(sql, statement -> {
            int parameter = 1;
            for (Map.Entry<MachineKey, Long> duplicate : duplicateCounts.entrySet()) {
                statement.setString(parameter++, duplicate.getKey().tenantId());
                statement.setString(parameter++, duplicate.getKey().machineId());
                statement.setLong(parameter++, duplicate.getValue());
                statement.setObject(parameter++, sqlTimestamp(ingestedAt));
            }
        });
    }

    private static int bindCycle(
            PreparedStatement statement, int parameter, PreparedCommand command, Instant ingestedAt)
            throws SQLException {
        CycleEvent event = command.command().event();
        var payload = event.payload();
        statement.setObject(parameter++, event.eventId());
        statement.setString(parameter++, event.tenantId());
        statement.setString(parameter++, event.siteId());
        statement.setString(parameter++, event.machineId());
        statement.setObject(parameter++, event.machineBootId());
        statement.setLong(parameter++, event.sequenceNumber());
        statement.setInt(parameter++, event.schemaVersion());
        statement.setObject(parameter++, sqlTimestamp(event.occurredAt()));
        statement.setObject(parameter++, sqlTimestamp(event.edgeReceivedAt()));
        statement.setObject(parameter++, sqlTimestamp(command.command().cloudReceivedAt()));
        statement.setObject(parameter++, sqlTimestamp(ingestedAt));
        statement.setLong(parameter++, payload.cycleNumber());
        statement.setLong(parameter++, payload.totalDurationMs());
        statement.setLong(parameter++, payload.plasticizingDurationMs());
        statement.setLong(parameter++, payload.injectionDurationMs());
        statement.setLong(parameter++, payload.coolingDurationMs());
        statement.setLong(parameter++, payload.demoldingDurationMs());
        statement.setDouble(parameter++, payload.peakInjectionPressureBar());
        statement.setDouble(parameter++, payload.meltTemperatureC());
        statement.setLong(parameter++, payload.goodParts());
        statement.setLong(parameter++, payload.rejectedParts());
        statement.setObject(parameter++, payload.energyConsumptionWh());
        statement.setString(parameter++, command.hash());
        statement.setInt(parameter++, command.command().partition());
        statement.setLong(parameter++, command.command().offset());
        return parameter;
    }

    private static String valueRows(int rowCount, int columnCount) {
        String row = "(" + String.join(", ", java.util.Collections.nCopies(columnCount, "?")) + ")";
        return repeatedRows(rowCount, row);
    }

    private static String repeatedRows(int rowCount, String row) {
        return String.join(", ", java.util.Collections.nCopies(rowCount, row));
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static OffsetDateTime sqlTimestamp(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private record PreparedCommand(int ordinal, PersistCommand command, String hash) {}

    private record NaturalKey(String tenantId, String machineId, UUID machineBootId, long sequenceNumber) {
        static NaturalKey from(CycleEvent event) {
            return new NaturalKey(
                    event.tenantId(), event.machineId(), event.machineBootId(), event.sequenceNumber());
        }
    }

    private record InsertedKey(UUID eventId, NaturalKey naturalKey) {
        static InsertedKey from(CycleEvent event) {
            return new InsertedKey(event.eventId(), NaturalKey.from(event));
        }
    }

    private record MachineKey(String tenantId, String machineId) {
        static MachineKey from(CycleEvent event) {
            return new MachineKey(event.tenantId(), event.machineId());
        }
    }

    private record ExistingEvent(UUID eventId, String payloadHash) {}

    private static final class QualityAccumulator {
        private final MachineKey machine;
        private UUID currentBootId;
        private long lastSequenceNumber;
        private long detectedGapCount;
        private long lastDetectedGap;
        private Instant lastCloudIngestionAt;
        private UUID latestEventId;
        private boolean needsInitialAdvance;

        private QualityAccumulator(
                MachineKey machine,
                UUID currentBootId,
                long lastSequenceNumber,
                long detectedGapCount,
                long lastDetectedGap,
                Instant lastCloudIngestionAt,
                UUID latestEventId,
                boolean needsInitialAdvance) {
            this.machine = machine;
            this.currentBootId = currentBootId;
            this.lastSequenceNumber = lastSequenceNumber;
            this.detectedGapCount = detectedGapCount;
            this.lastDetectedGap = lastDetectedGap;
            this.lastCloudIngestionAt = lastCloudIngestionAt;
            this.latestEventId = latestEventId;
            this.needsInitialAdvance = needsInitialAdvance;
        }

        static QualityAccumulator initial(CycleEvent event, Instant ingestedAt) {
            return new QualityAccumulator(
                    MachineKey.from(event),
                    event.machineBootId(),
                    event.sequenceNumber(),
                    0,
                    0,
                    ingestedAt,
                    event.eventId(),
                    true);
        }

        long advance(CycleEvent event, Instant ingestedAt) {
            long gaps;
            if (needsInitialAdvance) {
                gaps = event.sequenceNumber();
                needsInitialAdvance = false;
            } else if (!currentBootId.equals(event.machineBootId())) {
                gaps = event.sequenceNumber();
                currentBootId = event.machineBootId();
                lastSequenceNumber = event.sequenceNumber();
                latestEventId = event.eventId();
            } else if (event.sequenceNumber() > lastSequenceNumber) {
                gaps = Math.max(0, event.sequenceNumber() - lastSequenceNumber - 1);
                lastSequenceNumber = event.sequenceNumber();
                latestEventId = event.eventId();
            } else {
                gaps = 0;
            }
            detectedGapCount += gaps;
            lastDetectedGap = gaps;
            lastCloudIngestionAt = ingestedAt;
            return gaps;
        }
    }
}
