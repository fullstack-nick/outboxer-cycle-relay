package io.github.fullstacknick.outboxer.central;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.fullstacknick.outboxer.contract.CycleEvent;
import io.github.fullstacknick.outboxer.contract.CyclePayload;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

class CyclePersistenceServiceIntegrationTest {

    private static final Instant INGESTED_AT = Instant.parse("2026-09-04T12:00:03Z");
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine")
            .withDatabaseName("outboxer_test")
            .withUsername("outboxer_test")
            .withPassword("local-test-only");

    private static HikariDataSource dataSource;
    private JdbcTemplate jdbc;
    private CyclePersistenceService service;
    private TransactionTemplate transaction;

    @BeforeAll
    static void startDatabase() {
        POSTGRES.start();
        HikariConfig configuration = new HikariConfig();
        configuration.setJdbcUrl(POSTGRES.getJdbcUrl());
        configuration.setUsername(POSTGRES.getUsername());
        configuration.setPassword(POSTGRES.getPassword());
        dataSource = new HikariDataSource(configuration);
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
        POSTGRES.stop();
    }

    @BeforeEach
    void resetDatabase() {
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("TRUNCATE machine_data_quality, cycle_event");
        service = new CyclePersistenceService(jdbc, Clock.fixed(INGESTED_AT, ZoneOffset.UTC));
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Test
    void insertsOnceAndClassifiesAnExactReplay() {
        CycleEvent event = event(0, "de73fc96-87ab-460f-b24f-a81c51dfec6f");
        String json = "canonical-event-zero";
        Instant cloudReceived = Instant.parse("2026-09-04T12:00:02Z");

        assertThat(persist(event, json, 4, 7, cloudReceived).result())
                .isEqualTo(CyclePersistenceService.Result.INSERTED);
        assertThat(persist(event, json, 4, 8, cloudReceived).result())
                .isEqualTo(CyclePersistenceService.Result.DUPLICATE);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM cycle_event", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT duplicate_event_count FROM machine_data_quality WHERE machine_id = 'IMM-0042'",
                        Long.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT cloud_received_at = TIMESTAMPTZ '2026-09-04T12:00:02Z' FROM cycle_event",
                        Boolean.class))
                .isTrue();
    }

    @Test
    void rejectsAChangedPayloadForTheSameNaturalIdentity() {
        CycleEvent first = event(0, "de73fc96-87ab-460f-b24f-a81c51dfec6f");
        CycleEvent collision = event(0, "0e17e56c-dde9-4e54-870c-75dc37a03c35");

        assertThat(persist(first, "first", 1, 0, INGESTED_AT).result())
                .isEqualTo(CyclePersistenceService.Result.INSERTED);
        assertThat(persist(collision, "changed", 1, 1, INGESTED_AT).result())
                .isEqualTo(CyclePersistenceService.Result.CONFLICT);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM cycle_event", Long.class)).isEqualTo(1);
    }

    @Test
    void tracksSequenceGapsAndBootChanges() {
        CycleEvent first = event(0, "de73fc96-87ab-460f-b24f-a81c51dfec6f");
        CycleEvent third = event(2, "0e17e56c-dde9-4e54-870c-75dc37a03c35");

        assertThat(persist(first, "zero", 2, 0, INGESTED_AT).detectedGaps()).isZero();
        assertThat(persist(third, "two", 2, 1, INGESTED_AT).detectedGaps()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT detected_gap_count FROM machine_data_quality WHERE machine_id = 'IMM-0042'",
                        Long.class))
                .isEqualTo(1);
    }

    @Test
    void persistsABatchWhilePreservingIndividualOutcomesAndQualityAccounting() {
        CycleEvent first = event(0, "de73fc96-87ab-460f-b24f-a81c51dfec6f");
        CycleEvent third = event(2, "0e17e56c-dde9-4e54-870c-75dc37a03c35");
        CycleEvent conflictingThird = event(2, "c9bdc6b8-eb16-4244-a22d-d8ed8f999784");
        Instant cloudReceived = Instant.parse("2026-09-04T12:00:02Z");

        List<CyclePersistenceService.Outcome> outcomes = transaction.execute(status -> service.persistBatch(List.of(
                new CyclePersistenceService.PersistCommand(first, "first", 0, 0, cloudReceived),
                new CyclePersistenceService.PersistCommand(third, "third", 1, 0, cloudReceived),
                new CyclePersistenceService.PersistCommand(first, "first", 0, 1, cloudReceived),
                new CyclePersistenceService.PersistCommand(conflictingThird, "conflict", 1, 1, cloudReceived))));

        assertThat(outcomes).extracting(CyclePersistenceService.Outcome::result)
                .containsExactly(
                        CyclePersistenceService.Result.INSERTED,
                        CyclePersistenceService.Result.INSERTED,
                        CyclePersistenceService.Result.DUPLICATE,
                        CyclePersistenceService.Result.CONFLICT);
        assertThat(outcomes).extracting(CyclePersistenceService.Outcome::detectedGaps)
                .containsExactly(0L, 1L, 0L, 0L);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM cycle_event", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForMap(
                        """
                        SELECT last_sequence_number, detected_gap_count, duplicate_event_count
                        FROM machine_data_quality
                        WHERE machine_id = 'IMM-0042'
                        """))
                .containsEntry("last_sequence_number", 2L)
                .containsEntry("detected_gap_count", 1L)
                .containsEntry("duplicate_event_count", 1L);
    }

    private CyclePersistenceService.Outcome persist(
            CycleEvent event, String json, int partition, long offset, Instant cloudReceivedAt) {
        return transaction.execute(status -> service.persist(event, json, partition, offset, cloudReceivedAt));
    }

    private static CycleEvent event(long sequence, String eventId) {
        return new CycleEvent(
                1,
                UUID.fromString(eventId),
                "tenant-017",
                "site-north-01",
                "IMM-0042",
                UUID.fromString("f16fa35a-836c-4246-b707-f05e0ff491e2"),
                sequence,
                Instant.parse("2026-09-04T12:00:00Z").plusSeconds(sequence),
                Instant.parse("2026-09-04T12:00:01Z").plusSeconds(sequence),
                "CYCLE_COMPLETED",
                new CyclePayload(sequence, 20_000, 4_000, 700, 11_000, 2_000, 1_200, 230, 4, 0));
    }
}
