package io.github.fullstacknick.outboxer.edge

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("outboxer.edge")
data class EdgeProperties(
    val futureClockToleranceSeconds: Long = 300,
    val dispatchBatchSize: Int = 5_000,
    val dispatchShards: Int = 64,
    val dispatchQueueCapacity: Int = 500,
    val receiptBatchSize: Int = 500,
    val receiptQueueCapacity: Int = 10_000,
    val receiptBatchLingerMillis: Long = 10,
    val receiptTimeoutSeconds: Long = 30,
    val sentRetentionSeconds: Long = 86_400,
    val retryBaseSeconds: Long = 1,
    val retryCapSeconds: Long = 60,
    val minimumFreeBytes: Long = 67_108_864,
    val readinessPendingThreshold: Long = 10_000,
    val haltAfterPublishCount: Long = 0,
    val factory: MqttEndpoint = MqttEndpoint(),
    val uplink: MqttEndpoint = MqttEndpoint(port = 11884, username = "edge-uplink"),
) {
    data class MqttEndpoint(
        val host: String = "localhost",
        val port: Int = 11883,
        val username: String = "edge-factory",
        val password: String = "",
        val clientId: String = "outboxer-edge",
    )
}
