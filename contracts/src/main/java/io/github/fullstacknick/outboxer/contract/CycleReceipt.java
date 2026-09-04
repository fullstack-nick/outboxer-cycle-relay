package io.github.fullstacknick.outboxer.contract;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.UUID;

public record CycleReceipt(
        int schemaVersion,
        UUID eventId,
        ReceiptStatus status,
        Instant processedAt,
        @JsonInclude(JsonInclude.Include.NON_NULL) String errorCode,
        @JsonInclude(JsonInclude.Include.NON_NULL) String message) {

    public static CycleReceipt accepted(UUID eventId, Instant processedAt) {
        return new CycleReceipt(1, eventId, ReceiptStatus.ACCEPTED, processedAt, null, null);
    }

    public static CycleReceipt rejected(UUID eventId, Instant processedAt, String errorCode, String message) {
        return new CycleReceipt(1, eventId, ReceiptStatus.REJECTED, processedAt, errorCode, message);
    }
}
