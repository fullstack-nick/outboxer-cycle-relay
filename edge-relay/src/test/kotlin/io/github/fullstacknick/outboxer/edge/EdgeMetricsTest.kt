package io.github.fullstacknick.outboxer.edge

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`

class EdgeMetricsTest {
    @Test
    fun `metric identifiers have no unbounded labels`() {
        val repository = mock(OutboxRepository::class.java)
        `when`(repository.pendingCount()).thenReturn(0)
        `when`(repository.rejectedCount()).thenReturn(0)
        `when`(repository.oldestPendingAgeSeconds()).thenReturn(0)
        val registry = SimpleMeterRegistry()

        EdgeMetrics(registry, repository)

        assertThat(registry.meters).allSatisfy { meter ->
            assertThat(meter.id.tags).isEmpty()
        }
    }
}
