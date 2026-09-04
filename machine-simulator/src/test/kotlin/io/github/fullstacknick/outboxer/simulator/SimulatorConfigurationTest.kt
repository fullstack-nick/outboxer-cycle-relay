package io.github.fullstacknick.outboxer.simulator

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class SimulatorConfigurationTest {
    @Test
    fun `loads a bounded deterministic configuration`() {
        val configuration = SimulatorConfiguration.fromEnvironment(
            mapOf(
                "FACTORY_MQTT_PASSWORD" to "local-secret",
                "SIMULATOR_MACHINE_COUNT" to "12",
                "SIMULATOR_CYCLE_INTERVAL_SECONDS" to "6",
                "SIMULATOR_EVENT_COUNT" to "20",
                "SIMULATOR_RUN_ID" to "test-run",
            ),
        )

        assertThat(configuration.machineCount).isEqualTo(12)
        assertThat(configuration.eventCount).isEqualTo(20)
        assertThat(configuration.runId).isEqualTo("test-run")
    }

    @Test
    fun `requires broker credentials`() {
        assertThatThrownBy { SimulatorConfiguration.fromEnvironment(emptyMap()) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("FACTORY_MQTT_PASSWORD")
    }
}
