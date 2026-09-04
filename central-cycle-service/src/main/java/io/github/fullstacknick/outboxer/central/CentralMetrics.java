package io.github.fullstacknick.outboxer.central;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
class CentralMetrics {

    final Counter received;
    final Counter invalid;
    final Counter duplicates;
    final Counter identityConflicts;
    final Counter receiptsPublished;
    final Counter sequenceGaps;
    final DistributionSummary ingestionLagSeconds;

    CentralMetrics(MeterRegistry registry) {
        received = registry.counter("cloud.events.received");
        invalid = registry.counter("cloud.invalid.events");
        duplicates = registry.counter("cloud.duplicate.events");
        identityConflicts = registry.counter("cloud.identity.conflicts");
        receiptsPublished = registry.counter("cloud.receipts.published");
        sequenceGaps = registry.counter("cloud.sequence.gaps");
        ingestionLagSeconds = DistributionSummary.builder("cloud.ingestion.lag.seconds")
                .description("Seconds from source occurrence to database ingestion")
                .publishPercentileHistogram()
                .register(registry);
    }
}
