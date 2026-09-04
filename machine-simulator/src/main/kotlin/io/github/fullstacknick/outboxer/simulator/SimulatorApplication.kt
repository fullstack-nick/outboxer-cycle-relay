package io.github.fullstacknick.outboxer.simulator

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import io.github.fullstacknick.outboxer.contract.CyclePayload
import io.github.fullstacknick.outboxer.contract.ContractValidator
import io.github.fullstacknick.outboxer.contract.MachineCycleEvent
import io.github.fullstacknick.outboxer.contract.Topics
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport
import kotlin.math.max
import org.slf4j.LoggerFactory
import tools.jackson.databind.json.JsonMapper

private val logger = LoggerFactory.getLogger("machine-simulator")

fun main() {
    val configuration = SimulatorConfiguration.fromEnvironment()
    val mapper = JsonMapper.builder().findAndAddModules().build()
    val validator = ContractValidator(mapper, Clock.systemUTC(), Duration.ofMinutes(5))
    val simulator = CycleSimulator(configuration, validator)
    simulator.run()
}

internal data class SimulatorConfiguration(
    val brokerHost: String,
    val brokerPort: Int,
    val username: String,
    val password: String,
    val tenantId: String,
    val siteId: String,
    val machineCount: Int,
    val cycleInterval: Duration,
    val eventCount: Long,
    val runId: String,
    val expectedFile: Path?,
) {
    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): SimulatorConfiguration {
            fun value(name: String, default: String): String = environment[name]?.takeIf { it.isNotBlank() } ?: default

            val machineCount = value("SIMULATOR_MACHINE_COUNT", "3000").toInt()
            val cycleSeconds = value("SIMULATOR_CYCLE_INTERVAL_SECONDS", "30").toLong()
            require(machineCount in 1..100_000) { "SIMULATOR_MACHINE_COUNT must be between 1 and 100000" }
            require(cycleSeconds > 0) { "SIMULATOR_CYCLE_INTERVAL_SECONDS must be positive" }

            return SimulatorConfiguration(
                brokerHost = value("FACTORY_MQTT_HOST", "localhost"),
                brokerPort = value("FACTORY_MQTT_PORT", "11883").toInt(),
                username = value("FACTORY_MQTT_USERNAME", "simulator"),
                password = environment["FACTORY_MQTT_PASSWORD"]
                    ?: error("FACTORY_MQTT_PASSWORD is required"),
                tenantId = value("SIMULATOR_TENANT_ID", "tenant-017"),
                siteId = value("SIMULATOR_SITE_ID", "site-north-01"),
                machineCount = machineCount,
                cycleInterval = Duration.ofSeconds(cycleSeconds),
                eventCount = value("SIMULATOR_EVENT_COUNT", "0").toLong(),
                runId = value("SIMULATOR_RUN_ID", UUID.randomUUID().toString()),
                expectedFile = environment["SIMULATOR_EXPECTED_FILE"]?.takeIf { it.isNotBlank() }?.let(Path::of),
            )
        }
    }
}

internal class CycleSimulator(
    private val configuration: SimulatorConfiguration,
    private val validator: ContractValidator,
) {
    private val running = AtomicBoolean(true)
    private val published = AtomicLong()
    private val sequences = LongArray(configuration.machineCount)
    private lateinit var client: Mqtt5AsyncClient

    fun run() {
        client = mqttClient()
        client.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(86_400)
            .restrictions()
            .receiveMaximum(1_000)
            .sendMaximum(1_000)
            .applyRestrictions()
            .send()
            .join()
        logger.info(
            "simulator_started machineCount={} targetEventsPerSecond={} runId={}",
            configuration.machineCount,
            configuration.machineCount.toDouble() / configuration.cycleInterval.toSeconds(),
            configuration.runId,
        )

        val completed = CountDownLatch(1)
        Runtime.getRuntime().addShutdownHook(Thread {
            running.set(false)
            completed.await(5, TimeUnit.SECONDS)
        })

        try {
            expectedWriter().use { expected ->
                val nanosBetweenEvents = max(1L, configuration.cycleInterval.toNanos() / configuration.machineCount)
                var deadline = System.nanoTime()
                var machineIndex = 0
                while (running.get() && (configuration.eventCount == 0L || published.get() < configuration.eventCount)) {
                    publish(machineIndex, expected)
                    if (configuration.eventCount > 0 && published.get() >= configuration.eventCount) break
                    machineIndex = (machineIndex + 1) % configuration.machineCount
                    deadline += nanosBetweenEvents
                    LockSupport.parkNanos(max(0L, deadline - System.nanoTime()))
                }
            }
        } finally {
            running.set(false)
            client.disconnect().orTimeout(5, TimeUnit.SECONDS).exceptionally { null }.join()
            completed.countDown()
        }
        logger.info("simulator_stopped published={}", published.get())
    }

    private fun publish(machineIndex: Int, expected: java.io.BufferedWriter?) {
        val machineId = "IMM-%04d".format(machineIndex + 1)
        val sequence = sequences[machineIndex]++
        val bootId = stableUuid("${configuration.runId}:$machineId:boot")
        val eventId = stableUuid("${configuration.runId}:$machineId:$sequence")
        val event = MachineCycleEvent(
            1,
            eventId,
            configuration.tenantId,
            configuration.siteId,
            machineId,
            bootId,
            sequence,
            Instant.now(),
            MachineCycleEvent.TYPE,
            payload(machineIndex, sequence),
        )
        val bytes = validator.write(event)
        client.publishWith()
            .topic(Topics.factoryCycles(machineId))
            .qos(MqttQos.AT_LEAST_ONCE)
            .payload(bytes)
            .send()
            .join()
        expected?.apply {
            write("${event.eventId()},${event.tenantId()},${event.siteId()},${event.machineId()},${event.machineBootId()},${event.sequenceNumber()},${event.occurredAt()}")
            newLine()
            flush()
        }
        val count = published.incrementAndGet()
        if (count == 1L || count % 1000L == 0L) {
            logger.info("cycle_published count={} eventId={} machineId={} sequenceNumber={}", count, eventId, machineId, sequence)
        }
    }

    private fun payload(machineIndex: Int, sequence: Long): CyclePayload {
        val variation = (machineIndex % 17) * 23L + (sequence % 11) * 7L
        return CyclePayload(
            sequence,
            20_000L + variation,
            4_000L + variation / 5,
            700L + variation / 20,
            11_000L + variation / 3,
            2_000L + variation / 10,
            1_150.0 + machineIndex % 100,
            225.0 + machineIndex % 20,
            4,
            if ((machineIndex + sequence) % 97L == 0L) 1 else 0,
        )
    }

    private fun mqttClient(): Mqtt5AsyncClient = MqttClient.builder()
        .useMqttVersion5()
        .identifier("outboxer-simulator-${configuration.runId.take(12)}")
        .serverHost(configuration.brokerHost)
        .serverPort(configuration.brokerPort)
        .simpleAuth()
        .username(configuration.username)
        .password(configuration.password.toByteArray(StandardCharsets.UTF_8))
        .applySimpleAuth()
        .automaticReconnectWithDefaultConfig()
        .buildAsync()

    private fun expectedWriter(): java.io.BufferedWriter? {
        val path = configuration.expectedFile ?: return null
        path.parent?.let(Files::createDirectories)
        return Files.newBufferedWriter(
            path,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
        )
    }

    private fun stableUuid(value: String): UUID = UUID.nameUUIDFromBytes(value.toByteArray(StandardCharsets.UTF_8))
}
