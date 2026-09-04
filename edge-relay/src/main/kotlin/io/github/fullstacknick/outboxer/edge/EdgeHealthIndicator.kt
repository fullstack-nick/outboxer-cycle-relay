package io.github.fullstacknick.outboxer.edge

import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.stereotype.Component

@Component("uplink")
class EdgeHealthIndicator(
    private val bridge: EdgeMqttBridge,
    private val repository: OutboxRepository,
    private val properties: EdgeProperties,
) : HealthIndicator {
    override fun health(): Health {
        val pending = repository.pendingCount()
        val details = mapOf(
            "factoryConnected" to bridge.isFactoryConnected(),
            "uplinkConnected" to bridge.isUplinkConnected(),
            "subscriptionsReady" to bridge.isReady(),
            "pendingEvents" to pending,
            "pendingThreshold" to properties.readinessPendingThreshold,
        )
        val belowThreshold = properties.readinessPendingThreshold <= 0 || pending < properties.readinessPendingThreshold
        return if (bridge.isReady() && belowThreshold) {
            Health.up().withDetails(details).build()
        } else {
            Health.status("DEGRADED").withDetails(details).build()
        }
    }
}
