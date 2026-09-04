package io.github.fullstacknick.outboxer.edge

import io.github.fullstacknick.outboxer.contract.CycleEvent
import io.github.fullstacknick.outboxer.contract.ReceiptStatus
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Clock
import java.time.Instant
import java.util.UUID
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional

enum class StoreResult {
    INSERTED,
    DUPLICATE,
    CONFLICT,
}

data class OutboxRecord(
    val rowId: Long,
    val eventId: UUID,
    val siteId: String,
    val machineId: String,
    val payloadJson: String,
    val attemptCount: Int,
    val lastErrorCode: String?,
)

data class ReceiptSettlement(
    val eventId: UUID,
    val siteId: String,
    val status: ReceiptStatus,
    val errorCode: String?,
)

data class StoreCommand(
    val event: CycleEvent,
    val sourcePayload: ByteArray,
    val canonicalPayload: ByteArray,
)

data class QuarantineCommand(
    val topic: String,
    val payload: ByteArray,
    val reasonCode: String,
    val reasonMessage: String,
)

@Repository
class OutboxRepository(
    private val jdbcTemplate: JdbcTemplate,
    private val clock: Clock,
) {
    @Transactional
    fun store(event: CycleEvent, sourcePayload: ByteArray, canonicalPayload: ByteArray): StoreResult =
        storeInternal(StoreCommand(event, sourcePayload, canonicalPayload))

    @Transactional
    fun storeBatch(commands: List<StoreCommand>): List<StoreResult> = commands.map(::storeInternal)

    private fun storeInternal(command: StoreCommand): StoreResult {
        val event = command.event
        val now = clock.instant().toString()
        val sourceHash = sha256(command.sourcePayload)
        return try {
            jdbcTemplate.update(
                """
                INSERT INTO outbox_event (
                    event_id, tenant_id, site_id, machine_id, machine_boot_id, sequence_number,
                    schema_version, source_payload_sha256, payload_json, state, next_attempt_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
                """.trimIndent(),
                event.eventId().toString(),
                event.tenantId(),
                event.siteId(),
                event.machineId(),
                event.machineBootId().toString(),
                event.sequenceNumber(),
                event.schemaVersion(),
                sourceHash,
                String(command.canonicalPayload, StandardCharsets.UTF_8),
                now,
                now,
            )
            StoreResult.INSERTED
        } catch (exception: DataAccessException) {
            classifyExisting(event, sourceHash)
        }
    }

    fun findEligible(limit: Int, now: Instant): List<OutboxRecord> = jdbcTemplate.query(
        """
        SELECT candidate.row_id, candidate.event_id, candidate.site_id, candidate.machine_id,
               candidate.payload_json, candidate.attempt_count, candidate.last_error_code
        FROM outbox_event candidate
        WHERE candidate.state = 'PENDING' AND candidate.next_attempt_at <= ?
          AND NOT EXISTS (
              SELECT 1
              FROM outbox_event earlier
              WHERE earlier.tenant_id = candidate.tenant_id
                AND earlier.site_id = candidate.site_id
                AND earlier.machine_id = candidate.machine_id
                AND earlier.state = 'PENDING'
                AND earlier.row_id < candidate.row_id
          )
        ORDER BY candidate.row_id
        LIMIT ?
        """.trimIndent(),
        { statement ->
            statement.setString(1, now.toString())
            statement.setInt(2, limit)
        },
    ) { result, _ ->
        OutboxRecord(
            result.getLong("row_id"),
            UUID.fromString(result.getString("event_id")),
            result.getString("site_id"),
            result.getString("machine_id"),
            result.getString("payload_json"),
            result.getInt("attempt_count"),
            result.getString("last_error_code"),
        )
    }

    fun recordTransportAccepted(rowId: Long, nextAttemptAt: Instant): Int = jdbcTemplate.update(
        """
        UPDATE outbox_event
        SET attempt_count = attempt_count + 1, last_attempt_at = ?, next_attempt_at = ?, last_error_code = NULL
        WHERE row_id = ? AND state = 'PENDING'
        """.trimIndent(),
        clock.instant().toString(),
        nextAttemptAt.toString(),
        rowId,
    )

    fun recordTransportFailure(rowId: Long, nextAttemptAt: Instant, errorCode: String): Int = jdbcTemplate.update(
        """
        UPDATE outbox_event
        SET attempt_count = attempt_count + 1, last_attempt_at = ?, next_attempt_at = ?, last_error_code = ?
        WHERE row_id = ? AND state = 'PENDING'
        """.trimIndent(),
        clock.instant().toString(),
        nextAttemptAt.toString(),
        errorCode.take(64),
        rowId,
    )

    fun markSent(eventId: UUID, siteId: String): Int = jdbcTemplate.update(
        """
        UPDATE outbox_event
        SET state = 'SENT', sent_at = ?, last_error_code = NULL
        WHERE event_id = ? AND site_id = ? AND state = 'PENDING'
        """.trimIndent(),
        clock.instant().toString(),
        eventId.toString(),
        siteId,
    )

    fun markRejected(eventId: UUID, siteId: String, errorCode: String): Int = jdbcTemplate.update(
        """
        UPDATE outbox_event
        SET state = 'REJECTED', rejected_at = ?, last_error_code = ?
        WHERE event_id = ? AND site_id = ? AND state = 'PENDING'
        """.trimIndent(),
        clock.instant().toString(),
        errorCode.take(64),
        eventId.toString(),
        siteId,
    )

    @Transactional
    fun settleReceipts(settlements: List<ReceiptSettlement>): Int = settlements.sumOf { settlement ->
        when (settlement.status) {
            ReceiptStatus.ACCEPTED -> markSent(settlement.eventId, settlement.siteId)
            ReceiptStatus.REJECTED -> markRejected(
                settlement.eventId,
                settlement.siteId,
                settlement.errorCode ?: "CENTRAL_REJECTED",
            )
        }
    }

    @Transactional
    fun quarantine(topic: String, payload: ByteArray, reasonCode: String, reasonMessage: String) =
        quarantineInternal(QuarantineCommand(topic, payload, reasonCode, reasonMessage))

    @Transactional
    fun quarantineBatch(commands: List<QuarantineCommand>) = commands.forEach(::quarantineInternal)

    private fun quarantineInternal(command: QuarantineCommand) {
        val excerpt = String(
            command.payload.copyOfRange(0, minOf(command.payload.size, 4_096)),
            StandardCharsets.UTF_8,
        )
        jdbcTemplate.update(
            """
            INSERT INTO edge_rejected_message (
                received_at, source_topic, payload_sha256, payload_excerpt, reason_code, reason_message
            ) VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            clock.instant().toString(),
            command.topic.take(256),
            sha256(command.payload),
            excerpt,
            command.reasonCode.take(64),
            command.reasonMessage.take(256),
        )
    }

    fun pendingCount(): Long = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE state = 'PENDING'",
        Long::class.java,
    ) ?: 0

    fun rejectedCount(): Long = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox_event WHERE state = 'REJECTED'",
        Long::class.java,
    ) ?: 0

    fun oldestPendingAgeSeconds(): Long {
        val created = jdbcTemplate.queryForObject(
            "SELECT MIN(created_at) FROM outbox_event WHERE state = 'PENDING'",
            String::class.java,
        ) ?: return 0
        return java.time.Duration.between(Instant.parse(created), clock.instant()).seconds.coerceAtLeast(0)
    }

    fun deleteSentBefore(cutoff: Instant): Int = jdbcTemplate.update(
        "DELETE FROM outbox_event WHERE state = 'SENT' AND sent_at < ?",
        cutoff.toString(),
    )

    private fun classifyExisting(event: CycleEvent, sourceHash: String): StoreResult {
        val rows = jdbcTemplate.query(
            """
            SELECT event_id, source_payload_sha256
            FROM outbox_event
            WHERE event_id = ? OR (
                tenant_id = ? AND site_id = ? AND machine_id = ? AND machine_boot_id = ? AND sequence_number = ?
            )
            LIMIT 1
            """.trimIndent(),
            { statement ->
                statement.setString(1, event.eventId().toString())
                statement.setString(2, event.tenantId())
                statement.setString(3, event.siteId())
                statement.setString(4, event.machineId())
                statement.setString(5, event.machineBootId().toString())
                statement.setLong(6, event.sequenceNumber())
            },
        ) { result, _ -> result.getString("event_id") to result.getString("source_payload_sha256") }
        val existing = rows.firstOrNull() ?: throw IllegalStateException("Unique constraint failed without an existing row")
        return if (existing.first == event.eventId().toString() && existing.second == sourceHash) {
            StoreResult.DUPLICATE
        } else {
            StoreResult.CONFLICT
        }
    }

    companion object {
        fun sha256(payload: ByteArray): String =
            MessageDigest.getInstance("SHA-256").digest(payload).joinToString("") { "%02x".format(it) }
    }
}
