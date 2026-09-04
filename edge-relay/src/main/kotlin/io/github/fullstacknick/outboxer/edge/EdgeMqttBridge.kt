package io.github.fullstacknick.outboxer.edge

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.MqttGlobalPublishFilter
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish
import io.github.fullstacknick.outboxer.contract.ContractValidator
import io.github.fullstacknick.outboxer.contract.ContractViolationException
import io.github.fullstacknick.outboxer.contract.CycleEvent
import io.github.fullstacknick.outboxer.contract.ReceiptStatus
import io.github.fullstacknick.outboxer.contract.Topics
import jakarta.annotation.PreDestroy
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.ArrayList
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class EdgeMqttBridge(
    private val properties: EdgeProperties,
    private val validator: ContractValidator,
    private val repository: OutboxRepository,
    private val metrics: EdgeMetrics,
    private val clock: Clock,
) : ApplicationRunner {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val sourceExecutor = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "edge-source") }
    private val sourceWriterExecutor =
        Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "edge-source-writer") }
    private val receiptExecutor = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "edge-receipt") }
    private val receiptWriterExecutor =
        Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "edge-receipt-writer") }
    private val factoryConnecting = AtomicBoolean()
    private val uplinkConnecting = AtomicBoolean()
    private val factorySubscribed = AtomicBoolean()
    private val receiptSubscribed = AtomicBoolean()
    private val factoryDeliveryEpoch = AtomicLong()
    private val uplinkDeliveryEpoch = AtomicLong()
    private val dispatching = AtomicBoolean()
    private val stopping = AtomicBoolean()
    private val sourceInputClosed = AtomicBoolean()
    private val receiptInputClosed = AtomicBoolean()
    private val inFlightRows = ConcurrentHashMap.newKeySet<Long>()
    private val inFlightDeadlines = ConcurrentHashMap<Long, Instant>()
    private val inFlightEventRows = ConcurrentHashMap<UUID, Long>()
    private val inFlightRowEvents = ConcurrentHashMap<Long, UUID>()
    private val sourceQueue = ArrayBlockingQueue<SourceWork>(properties.sourceQueueCapacity)
    private val receiptQueue = ArrayBlockingQueue<ReceiptWork>(properties.receiptQueueCapacity)
    private val dispatchExecutors = List(properties.dispatchShards) { shard ->
        serialExecutor("edge-dispatch-$shard", properties.dispatchQueueCapacity)
    }
    private val retryPolicy = RetryPolicy(
        Duration.ofSeconds(properties.retryBaseSeconds),
        Duration.ofSeconds(properties.retryCapSeconds),
    )
    private val factoryClient = mqttClient(properties.factory) {
        factorySubscribed.set(false)
        factoryDeliveryEpoch.incrementAndGet()
    }
    private val uplinkClient = mqttClient(properties.uplink) {
        receiptSubscribed.set(false)
        uplinkDeliveryEpoch.incrementAndGet()
    }

    init {
        // Register global flows before CONNECT so queued messages from a persistent
        // MQTT session cannot arrive in the connect-to-subscribe callback gap.
        factoryClient.publishes(MqttGlobalPublishFilter.ALL, ::handleSource, sourceExecutor, true)
        uplinkClient.publishes(MqttGlobalPublishFilter.ALL, ::handleReceipt, receiptExecutor, true)
        sourceWriterExecutor.execute(::persistSourceBatches)
        receiptWriterExecutor.execute(::persistReceiptBatches)
    }

    override fun run(args: ApplicationArguments) {
        require(properties.factory.password.isNotBlank()) { "Factory MQTT password is required" }
        require(properties.uplink.password.isNotBlank()) { "Uplink MQTT password is required" }
        require(properties.dispatchShards in 1..128) { "Dispatch shards must be between 1 and 128" }
        require(properties.dispatchQueueCapacity in 1..10_000) { "Dispatch queue capacity must be between 1 and 10000" }
        require(properties.sourceBatchSize in 1..5_000) { "Source batch size must be between 1 and 5000" }
        require(properties.sourceQueueCapacity in 1..100_000) { "Source queue capacity must be between 1 and 100000" }
        require(properties.sourceBatchLingerMillis in 1..1_000) { "Source batch linger must be between 1 and 1000 ms" }
        require(properties.receiptBatchSize in 1..5_000) { "Receipt batch size must be between 1 and 5000" }
        require(properties.receiptQueueCapacity in 1..100_000) { "Receipt queue capacity must be between 1 and 100000" }
        require(properties.receiptBatchLingerMillis in 1..1_000) { "Receipt batch linger must be between 1 and 1000 ms" }
        maintainConnections()
    }

    @Scheduled(fixedDelay = 1_000)
    fun maintainConnections() {
        connectAndSubscribeFactory()
        connectAndSubscribeReceipts()
    }

    @Scheduled(fixedDelayString = "\${outboxer.edge.dispatch-delay-millis:50}")
    fun dispatchPending() {
        if (stopping.get() || !uplinkClient.config.state.isConnected || !dispatching.compareAndSet(false, true)) return
        try {
            val now = clock.instant()
            expireReceiptWaits(now)
            repository.findEligible(properties.dispatchBatchSize, now).forEach { record ->
                if (inFlightRows.add(record.rowId)) {
                    try {
                        val shard = Math.floorMod(record.machineId.hashCode(), dispatchExecutors.size)
                        dispatchExecutors[shard].execute { dispatch(record) }
                    } catch (exception: RejectedExecutionException) {
                        inFlightRows.remove(record.rowId)
                        if (!stopping.get()) throw exception
                    }
                }
            }
        } finally {
            dispatching.set(false)
        }
    }

    private fun dispatch(record: OutboxRecord) {
        val attemptedAt = clock.instant()
        trackReceiptWait(record, attemptedAt.plusSeconds(properties.receiptTimeoutSeconds))
        try {
            uplinkClient.publishWith()
                .topic(Topics.uplinkCycles(record.siteId, record.machineId))
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(record.payloadJson.toByteArray(StandardCharsets.UTF_8))
                .responseTopic(Topics.receipts(record.siteId))
                .correlationData(record.eventId.toString().toByteArray(StandardCharsets.UTF_8))
                .send()
                .get(5, TimeUnit.SECONDS)
            val publishCount = metrics.transportPublished.incrementAndGet()
            if (properties.haltAfterPublishCount > 0 && publishCount == properties.haltAfterPublishCount) {
                logger.atError()
                    .addKeyValue("faultMarker", "halt_after_uplink_publish")
                    .addKeyValue("eventId", record.eventId)
                    .addKeyValue("machineId", record.machineId)
                    .addKeyValue("siteId", record.siteId)
                    .log("Injected edge crash after uplink transport acknowledgement")
                Runtime.getRuntime().halt(70)
            }
            if (record.attemptCount > 0) metrics.replayed.increment()
        } catch (exception: Exception) {
            clearReceiptWait(record.eventId, record.rowId)
            metrics.publishFailures.increment()
            val delay = retryPolicy.delay(record.attemptCount)
            repository.recordTransportFailure(record.rowId, clock.instant().plus(delay), "MQTT_UNAVAILABLE")
            logger.atWarn()
                .addKeyValue("eventId", record.eventId)
                .addKeyValue("machineId", record.machineId)
                .addKeyValue("siteId", record.siteId)
                .addKeyValue("nextRetryMs", delay.toMillis())
                .addKeyValue("reasonCode", "MQTT_UNAVAILABLE")
                .log("Uplink publish failed")
        }
    }

    @Scheduled(fixedDelay = 60_000)
    fun cleanupSent() {
        repository.deleteSentBefore(clock.instant().minusSeconds(properties.sentRetentionSeconds))
    }

    fun isFactoryConnected(): Boolean = factoryClient.config.state.isConnected

    fun isUplinkConnected(): Boolean = uplinkClient.config.state.isConnected

    fun isReady(): Boolean =
        isFactoryConnected() && isUplinkConnected() && factorySubscribed.get() && receiptSubscribed.get()

    private fun connectAndSubscribeFactory() {
        if (!factoryClient.config.state.isConnectedOrReconnect && factoryConnecting.compareAndSet(false, true)) {
            factoryClient.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(86_400)
                .restrictions()
                .receiveMaximum(1_000)
                .sendMaximum(1_000)
                .applyRestrictions()
                .send()
                .whenComplete { _, error ->
                factoryConnecting.set(false)
                if (error != null) logger.warn("factory_mqtt_connect_failed reason={}", error.javaClass.simpleName)
            }
        }
        if (factoryClient.config.state.isConnected && factorySubscribed.compareAndSet(false, true)) {
            factoryClient.subscribeWith()
                .topicFilter("factory/+/cycles")
                .qos(MqttQos.AT_LEAST_ONCE)
                .send()
                .whenComplete { _, error ->
                    if (error != null) {
                        factorySubscribed.set(false)
                        logger.warn("factory_mqtt_subscribe_failed reason={}", error.javaClass.simpleName)
                    }
                }
        }
    }

    private fun connectAndSubscribeReceipts() {
        if (!uplinkClient.config.state.isConnectedOrReconnect && uplinkConnecting.compareAndSet(false, true)) {
            uplinkClient.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(86_400)
                .restrictions()
                .receiveMaximum(1_000)
                .sendMaximum(1_000)
                .applyRestrictions()
                .send()
                .whenComplete { _, error ->
                uplinkConnecting.set(false)
                if (error != null) logger.warn("uplink_mqtt_connect_failed reason={}", error.javaClass.simpleName)
            }
        }
        if (uplinkClient.config.state.isConnected && receiptSubscribed.compareAndSet(false, true)) {
            uplinkClient.subscribeWith()
                .topicFilter("receipts/+/cycles")
                .qos(MqttQos.AT_LEAST_ONCE)
                .send()
                .whenComplete { _, error ->
                    if (error != null) {
                        receiptSubscribed.set(false)
                        logger.warn("receipt_subscribe_failed reason={}", error.javaClass.simpleName)
                    }
                }
        }
    }

    private fun handleSource(publish: Mqtt5Publish) {
        val epoch = factoryDeliveryEpoch.get()
        val topic = publish.topic.toString()
        val payload = publish.payloadAsBytes
        try {
            val source = validator.readSource(payload)
            Topics.requireFactoryTopic(topic, source)
            val normalizedSource = validator.write(source)
            val event = CycleEvent.from(source, clock.instant())
            val canonical = validator.write(event)
            validator.readCanonical(canonical)
            sourceQueue.put(SourceWork(publish, epoch, topic, payload, event, normalizedSource, canonical))
        } catch (exception: ContractViolationException) {
            repository.quarantine(topic, payload, "INVALID_SOURCE", exception.violations().firstOrNull() ?: "Invalid source")
            metrics.invalid.increment()
            if (isCurrentFactoryDelivery(epoch)) publish.acknowledge()
        } catch (exception: Exception) {
            logger.error("source_persistence_failed topic={} reason={}", topic, exception.javaClass.simpleName)
        }
    }

    private fun persistSourceBatches() {
        while (!sourceInputClosed.get() || sourceQueue.isNotEmpty()) {
            try {
                val first = sourceQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                val batch = ArrayList<SourceWork>(properties.sourceBatchSize)
                batch.add(first)
                val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(properties.sourceBatchLingerMillis)
                while (batch.size < properties.sourceBatchSize) {
                    val remaining = deadline - System.nanoTime()
                    if (remaining <= 0) break
                    val next = sourceQueue.poll(remaining, TimeUnit.NANOSECONDS) ?: break
                    batch.add(next)
                }
                persistSourceBatch(batch)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (exception: Exception) {
                logger.error("source_batch_persistence_failed reason={}", exception.javaClass.simpleName)
                sourceQueue.clear()
                disconnectForSourceRedelivery()
            }
        }
    }

    private fun persistSourceBatch(batch: List<SourceWork>) {
        val results = repository.storeBatch(batch.map { work ->
            StoreCommand(work.event, work.normalizedSource, work.canonical)
        })
        val conflicts = batch.zip(results).filter { (_, result) -> result == StoreResult.CONFLICT }
        if (conflicts.isNotEmpty()) {
            repository.quarantineBatch(conflicts.map { (work, _) ->
                QuarantineCommand(
                    work.topic,
                    work.payload,
                    "IDENTITY_CONFLICT",
                    "Source identity has conflicting content",
                )
            })
        }
        batch.zip(results).forEach { (work, result) ->
            when (result) {
                StoreResult.INSERTED -> {
                    metrics.received.increment()
                    logger.atDebug()
                        .addKeyValue("eventId", work.event.eventId())
                        .addKeyValue("machineId", work.event.machineId())
                        .addKeyValue("siteId", work.event.siteId())
                        .addKeyValue("sequenceNumber", work.event.sequenceNumber())
                        .addKeyValue("schemaVersion", work.event.schemaVersion())
                        .log("Source event persisted to edge outbox")
                }
                StoreResult.DUPLICATE -> metrics.sourceDuplicates.increment()
                StoreResult.CONFLICT -> metrics.sourceConflicts.increment()
            }
        }
        batch.forEach { work ->
            if (isCurrentFactoryDelivery(work.deliveryEpoch)) work.publish.acknowledge()
        }
    }

    private fun handleReceipt(publish: Mqtt5Publish) {
        val epoch = uplinkDeliveryEpoch.get()
        val topic = publish.topic.toString()
        val payload = publish.payloadAsBytes
        try {
            val receipt = validator.readReceipt(payload)
            val siteId = parseReceiptSite(topic)
            val correlation = publish.correlationData.map(::copyBytes).map { String(it, StandardCharsets.UTF_8) }.orElse("")
            if (correlation != receipt.eventId().toString()) {
                throw ContractViolationException(listOf("receipt correlation data does not match eventId"))
            }
            receiptQueue.put(
                ReceiptWork(
                    publish,
                    epoch,
                    ReceiptSettlement(receipt.eventId(), siteId, receipt.status(), receipt.errorCode()),
                ),
            )
        } catch (exception: ContractViolationException) {
            repository.quarantine(topic, payload, "INVALID_RECEIPT", exception.violations().firstOrNull() ?: "Invalid receipt")
            metrics.invalid.increment()
            if (isCurrentUplinkDelivery(epoch)) publish.acknowledge()
        } catch (exception: Exception) {
            logger.error("receipt_persistence_failed topic={} reason={}", topic, exception.javaClass.simpleName)
        }
    }

    private fun parseReceiptSite(topic: String): String {
        val segments = topic.split('/')
        if (segments.size != 3 || segments[0] != "receipts" || segments[2] != "cycles") {
            throw ContractViolationException(listOf("invalid receipt topic"))
        }
        return Topics.requireIdentifier(segments[1], "siteId")
    }

    private fun persistReceiptBatches() {
        while (!receiptInputClosed.get() || receiptQueue.isNotEmpty()) {
            try {
                val first = receiptQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                val batch = ArrayList<ReceiptWork>(properties.receiptBatchSize)
                batch.add(first)
                val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(properties.receiptBatchLingerMillis)
                while (batch.size < properties.receiptBatchSize) {
                    val remaining = deadline - System.nanoTime()
                    if (remaining <= 0) break
                    val next = receiptQueue.poll(remaining, TimeUnit.NANOSECONDS) ?: break
                    batch.add(next)
                }
                persistReceiptBatch(batch)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (exception: Exception) {
                logger.error("receipt_batch_persistence_failed reason={}", exception.javaClass.simpleName)
                receiptQueue.clear()
                disconnectForReceiptRedelivery()
            }
        }
    }

    private fun persistReceiptBatch(batch: List<ReceiptWork>) {
        repository.settleReceipts(batch.map { it.settlement })
        batch.forEach { work ->
            clearReceiptWait(work.settlement.eventId, inFlightEventRows[work.settlement.eventId])
            metrics.receipts.increment()
            if (isCurrentUplinkDelivery(work.deliveryEpoch)) work.publish.acknowledge()
        }
    }

    private fun trackReceiptWait(record: OutboxRecord, deadline: Instant) {
        inFlightDeadlines[record.rowId] = deadline
        inFlightEventRows[record.eventId] = record.rowId
        inFlightRowEvents[record.rowId] = record.eventId
    }

    private fun expireReceiptWaits(now: Instant) {
        inFlightDeadlines.forEach { (rowId, deadline) ->
            if (!deadline.isAfter(now) && inFlightDeadlines.remove(rowId, deadline)) {
                val eventId = inFlightRowEvents.remove(rowId)
                if (eventId != null) inFlightEventRows.remove(eventId, rowId)
                inFlightRows.remove(rowId)
                metrics.receiptTimeouts.increment()
                metrics.replayed.increment()
            }
        }
    }

    private fun clearReceiptWait(eventId: UUID, rowId: Long?) {
        inFlightEventRows.remove(eventId)
        if (rowId != null) {
            inFlightRowEvents.remove(rowId)
            inFlightDeadlines.remove(rowId)
            inFlightRows.remove(rowId)
        }
    }

    private fun disconnectForReceiptRedelivery() {
        receiptSubscribed.set(false)
        uplinkClient.disconnect().exceptionally { null }
    }

    private fun disconnectForSourceRedelivery() {
        factorySubscribed.set(false)
        factoryClient.disconnect().exceptionally { null }
    }

    private fun isCurrentFactoryDelivery(epoch: Long): Boolean =
        epoch == factoryDeliveryEpoch.get() && factoryClient.config.state.isConnected

    private fun isCurrentUplinkDelivery(epoch: Long): Boolean =
        epoch == uplinkDeliveryEpoch.get() && uplinkClient.config.state.isConnected

    private fun mqttClient(
        endpoint: EdgeProperties.MqttEndpoint,
        onDisconnected: () -> Unit,
    ): Mqtt5AsyncClient = MqttClient.builder()
        .useMqttVersion5()
        .identifier(endpoint.clientId)
        .serverHost(endpoint.host)
        .serverPort(endpoint.port)
        .simpleAuth()
        .username(endpoint.username)
        .password(endpoint.password.toByteArray(StandardCharsets.UTF_8))
        .applySimpleAuth()
        .automaticReconnectWithDefaultConfig()
        .addDisconnectedListener { onDisconnected() }
        .buildAsync()

    private fun copyBytes(buffer: ByteBuffer): ByteArray {
        val copy = buffer.asReadOnlyBuffer()
        return ByteArray(copy.remaining()).also(copy::get)
    }

    private fun serialExecutor(name: String, capacity: Int): ThreadPoolExecutor = ThreadPoolExecutor(
        1,
        1,
        0,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(capacity),
        { runnable -> Thread(runnable, name) },
        { task, executor ->
            if (executor.isShutdown) throw RejectedExecutionException("Dispatcher is shutting down")
            try {
                executor.queue.put(task)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                throw RejectedExecutionException("Interrupted while applying dispatcher backpressure", exception)
            }
        },
    )

    @PreDestroy
    fun stop() {
        stopping.set(true)
        runCatching { factoryClient.disconnect().get(5, TimeUnit.SECONDS) }
        sourceExecutor.shutdown()
        runCatching { sourceExecutor.awaitTermination(5, TimeUnit.SECONDS) }
        sourceInputClosed.set(true)
        sourceWriterExecutor.shutdown()
        runCatching { sourceWriterExecutor.awaitTermination(15, TimeUnit.SECONDS) }
        dispatchExecutors.forEach { it.shutdown() }
        dispatchExecutors.forEach { runCatching { it.awaitTermination(10, TimeUnit.SECONDS) } }
        receiptExecutor.shutdown()
        runCatching { receiptExecutor.awaitTermination(5, TimeUnit.SECONDS) }
        receiptInputClosed.set(true)
        receiptWriterExecutor.shutdown()
        runCatching { receiptWriterExecutor.awaitTermination(15, TimeUnit.SECONDS) }
        runCatching { uplinkClient.disconnect().get(5, TimeUnit.SECONDS) }
    }

    private data class SourceWork(
        val publish: Mqtt5Publish,
        val deliveryEpoch: Long,
        val topic: String,
        val payload: ByteArray,
        val event: CycleEvent,
        val normalizedSource: ByteArray,
        val canonical: ByteArray,
    )

    private data class ReceiptWork(
        val publish: Mqtt5Publish,
        val deliveryEpoch: Long,
        val settlement: ReceiptSettlement,
    )
}
