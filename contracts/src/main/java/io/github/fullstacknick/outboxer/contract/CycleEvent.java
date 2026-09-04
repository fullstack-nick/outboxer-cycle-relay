package io.github.fullstacknick.outboxer.contract;

import java.time.Instant;
import java.util.UUID;

public record CycleEvent(
        int schemaVersion,
        UUID eventId,
        String tenantId,
        String siteId,
        String machineId,
        UUID machineBootId,
        long sequenceNumber,
        Instant occurredAt,
        Instant edgeReceivedAt,
        String type,
        CyclePayload payload) {

    public static CycleEvent from(MachineCycleEvent source, Instant edgeReceivedAt) {
        return new CycleEvent(
                source.schemaVersion(),
                source.eventId(),
                source.tenantId(),
                source.siteId(),
                source.machineId(),
                source.machineBootId(),
                source.sequenceNumber(),
                source.occurredAt(),
                edgeReceivedAt,
                source.type(),
                source.payload());
    }
}
