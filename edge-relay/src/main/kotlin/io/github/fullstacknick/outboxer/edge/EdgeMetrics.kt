package io.github.fullstacknick.outboxer.edge

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicLong
import org.springframework.stereotype.Component

@Component
class EdgeMetrics(registry: MeterRegistry, repository: OutboxRepository) {
    val received: Counter = registry.counter("edge.events.received")
    val invalid: Counter = registry.counter("edge.events.invalid")
    val publishFailures: Counter = registry.counter("edge.publish.failures")
    val replayed: Counter = registry.counter("edge.replayed.events")
    val receipts: Counter = registry.counter("edge.receipts.received")
    val receiptTimeouts: Counter = registry.counter("edge.receipt.timeouts")
    val sourceDuplicates: Counter = registry.counter("edge.source.duplicates")
    val sourceConflicts: Counter = registry.counter("edge.source.conflicts")
    val transportPublished = AtomicLong()

    init {
        registry.gauge("edge.outbox.pending", repository) { it.pendingCount().toDouble() }
        registry.gauge("edge.outbox.rejected", repository) { it.rejectedCount().toDouble() }
        registry.gauge("edge.outbox.oldest.pending.age.seconds", repository) {
            it.oldestPendingAgeSeconds().toDouble()
        }
    }
}
