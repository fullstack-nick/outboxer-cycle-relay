package io.github.fullstacknick.outboxer.central;

import java.time.Instant;

record InvalidEventEnvelope(
        String reasonCode,
        String message,
        String sourceTopic,
        Instant receivedAt,
        String payloadSha256,
        String payloadExcerpt) {}
