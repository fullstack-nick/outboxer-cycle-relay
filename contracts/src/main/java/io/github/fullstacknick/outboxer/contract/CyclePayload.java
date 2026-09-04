package io.github.fullstacknick.outboxer.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

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
        long rejectedParts,
        @JsonInclude(JsonInclude.Include.NON_NULL) Double energyConsumptionWh) {

    public CyclePayload(
            long cycleNumber,
            long totalDurationMs,
            long plasticizingDurationMs,
            long injectionDurationMs,
            long coolingDurationMs,
            long demoldingDurationMs,
            double peakInjectionPressureBar,
            double meltTemperatureC,
            long goodParts,
            long rejectedParts) {
        this(
                cycleNumber,
                totalDurationMs,
                plasticizingDurationMs,
                injectionDurationMs,
                coolingDurationMs,
                demoldingDurationMs,
                peakInjectionPressureBar,
                meltTemperatureC,
                goodParts,
                rejectedParts,
                null);
    }
}
