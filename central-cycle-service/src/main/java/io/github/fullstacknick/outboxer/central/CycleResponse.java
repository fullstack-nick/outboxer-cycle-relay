package io.github.fullstacknick.outboxer.central;

import java.time.Instant;
import java.util.UUID;

public record CycleResponse(
        int schemaVersion,
        UUID eventId,
        String tenantId,
        String siteId,
        String machineId,
        UUID machineBootId,
        long sequenceNumber,
        Instant occurredAt,
        Instant edgeReceivedAt,
        Instant cloudReceivedAt,
        Instant ingestedAt,
        long cycleNumber,
        long totalDurationMs,
        long plasticizingDurationMs,
        long injectionDurationMs,
        long coolingDurationMs,
        long demoldingDurationMs,
        double peakInjectionPressureBar,
        double meltTemperatureC,
        long goodParts,
        long rejectedParts,
        Double energyConsumptionWh) {}
