package io.github.fullstacknick.outboxer.edge

import io.github.fullstacknick.outboxer.contract.CycleEvent
import io.github.fullstacknick.outboxer.contract.CyclePayload
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.sqlite.SQLiteDataSource
import org.springframework.jdbc.core.JdbcTemplate

class OutboxRepositoryTest {
    @TempDir
    lateinit var directory: Path

    private lateinit var repository: OutboxRepository
    private lateinit var jdbcTemplate: JdbcTemplate
    private val now = Instant.parse("2026-09-04T12:00:00Z")

    @BeforeEach
    fun setUp() {
        val dataSource = SQLiteDataSource().apply {
            url = "jdbc:sqlite:${directory.resolve("edge.db")}?journal_mode=WAL&synchronous=FULL&foreign_keys=on&busy_timeout=5000"
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate()
        jdbcTemplate = JdbcTemplate(dataSource)
        repository = OutboxRepository(jdbcTemplate, Clock.fixed(now, ZoneOffset.UTC))
    }

    @Test
    fun `stores an event once and applies an idempotent receipt`() {
        val event = event()
        val source = "stable-source".toByteArray()
        val canonical = "{\"eventId\":\"${event.eventId()}\"}".toByteArray()

        assertThat(repository.store(event, source, canonical)).isEqualTo(StoreResult.INSERTED)
        assertThat(repository.store(event, source, canonical)).isEqualTo(StoreResult.DUPLICATE)
        assertThat(repository.pendingCount()).isEqualTo(1)

        assertThat(repository.markSent(event.eventId(), event.siteId())).isEqualTo(1)
        assertThat(repository.markSent(event.eventId(), event.siteId())).isZero()
        assertThat(repository.pendingCount()).isZero()
    }

    @Test
    fun `classifies changed content for the same identity as a conflict`() {
        val event = event()

        assertThat(repository.store(event, "first".toByteArray(), "canonical".toByteArray()))
            .isEqualTo(StoreResult.INSERTED)
        assertThat(repository.store(event, "changed".toByteArray(), "canonical".toByteArray()))
            .isEqualTo(StoreResult.CONFLICT)
    }

    @Test
    fun `retains pending payload and schedules application receipt retries`() {
        val event = event()
        val canonical = "canonical".toByteArray()
        repository.store(event, "source".toByteArray(), canonical)
        val initial = repository.findEligible(10, now).single()

        repository.recordTransportAccepted(initial.rowId, now.plusSeconds(30))

        assertThat(repository.findEligible(10, now.plusSeconds(29))).isEmpty()
        val retry = repository.findEligible(10, now.plusSeconds(30)).single()
        assertThat(retry.attemptCount).isEqualTo(1)
        assertThat(retry.lastErrorCode).isNull()
        assertThat(retry.payloadJson).isEqualTo("canonical")
    }

    @Test
    fun `allows only the oldest pending event for each machine`() {
        val first = event(sequenceNumber = 1, eventId = UUID.fromString("de73fc96-87ab-460f-b24f-a81c51dfec6f"))
        val second = event(sequenceNumber = 2, eventId = UUID.fromString("42171ac6-e008-40ba-af87-48517bba0465"))
        repository.store(first, "source-1".toByteArray(), "canonical-1".toByteArray())
        repository.store(second, "source-2".toByteArray(), "canonical-2".toByteArray())

        assertThat(repository.findEligible(10, now).map { it.eventId }).containsExactly(first.eventId())

        repository.markSent(first.eventId(), first.siteId())

        assertThat(repository.findEligible(10, now).map { it.eventId }).containsExactly(second.eventId())
    }

    @Test
    fun `records a bounded transient error without losing the row`() {
        val event = event()
        repository.store(event, "source".toByteArray(), "canonical".toByteArray())
        val initial = repository.findEligible(1, now).single()

        repository.recordTransportFailure(initial.rowId, now.plusSeconds(5), "x".repeat(100))

        assertThat(repository.pendingCount()).isEqualTo(1)
        val retry = repository.findEligible(1, now.plusSeconds(5)).single()
        assertThat(retry.attemptCount).isEqualTo(1)
        assertThat(retry.lastErrorCode).hasSize(64)
    }

    @Test
    fun `keeps terminal rejections for diagnosis`() {
        val event = event()
        repository.store(event, "source".toByteArray(), "canonical".toByteArray())

        assertThat(repository.markRejected(event.eventId(), event.siteId(), "INVALID_CANONICAL_EVENT"))
            .isEqualTo(1)
        assertThat(repository.pendingCount()).isZero()
        assertThat(repository.rejectedCount()).isEqualTo(1)
    }

    @Test
    fun `cleans only expired sent rows`() {
        val event = event()
        repository.store(event, "source".toByteArray(), "canonical".toByteArray())
        repository.markSent(event.eventId(), event.siteId())

        assertThat(repository.deleteSentBefore(now.minusSeconds(1))).isZero()
        assertThat(repository.deleteSentBefore(now.plusSeconds(1))).isEqualTo(1)
    }

    @Test
    fun `uses required durability pragmas`() {
        assertThat(jdbcTemplate.queryForObject("PRAGMA journal_mode", String::class.java)).isEqualToIgnoringCase("wal")
        assertThat(jdbcTemplate.queryForObject("PRAGMA synchronous", Int::class.java)).isEqualTo(2)
        assertThat(jdbcTemplate.queryForObject("PRAGMA foreign_keys", Int::class.java)).isEqualTo(1)
    }

    private fun event(
        sequenceNumber: Long = 1,
        eventId: UUID = UUID.fromString("de73fc96-87ab-460f-b24f-a81c51dfec6f"),
    ): CycleEvent = CycleEvent(
        1,
        eventId,
        "tenant-017",
        "site-north-01",
        "IMM-0042",
        UUID.fromString("f16fa35a-836c-4246-b707-f05e0ff491e2"),
        sequenceNumber,
        now.minusSeconds(1),
        now,
        "CYCLE_COMPLETED",
        CyclePayload(1, 20_000, 4_000, 700, 11_000, 2_000, 1_200.0, 230.0, 4, 0),
    )
}
