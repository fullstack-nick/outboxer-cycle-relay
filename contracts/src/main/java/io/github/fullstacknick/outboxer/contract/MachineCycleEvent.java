package io.github.fullstacknick.outboxer.contract;

import java.time.Instant;
import java.util.UUID;

public record MachineCycleEvent(
        int schemaVersion,
        UUID eventId,
        String tenantId,
        String siteId,
        String machineId,
        UUID machineBootId,
        long sequenceNumber,
        Instant occurredAt,
        String type,
        CyclePayload payload) {

    public static final String TYPE = "CYCLE_COMPLETED";
}
