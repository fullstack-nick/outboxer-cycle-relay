package io.github.fullstacknick.outboxer.central;

import java.time.Instant;

public record DataQualityResponse(
        String machineId,
        long lastSequenceNumber,
        long detectedGapCount,
        long duplicateEventCount,
        Instant lastCloudIngestionAt) {}
