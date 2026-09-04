package io.github.fullstacknick.outboxer.central;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("uplinkMqtt")
class CentralHealthIndicator implements HealthIndicator {

    private final CentralMqttGateway gateway;

    CentralHealthIndicator(CentralMqttGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Health health() {
        return gateway.isReady()
                ? Health.up().build()
                : Health.status("DEGRADED").withDetail("ready", false).build();
    }
}
