package io.github.fullstacknick.outboxer.central;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class CentralMetricsTest {

    @Test
    void metricIdentifiersHaveNoUnboundedLabels() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new CentralMetrics(registry);

        assertThat(registry.getMeters())
                .allSatisfy(meter -> assertThat(meter.getId().getTags()).isEmpty());
    }
}
