package io.github.fullstacknick.outboxer.contract;

public record CyclePayload(
        long cycleNumber,
        long totalDurationMs,
        long plasticizingDurationMs,
        long injectionDurationMs,
        long coolingDurationMs,
        long demoldingDurationMs,
        double peakInjectionPressureBar,
        double meltTemperatureC,
        long goodParts,
        long rejectedParts) {}
