package io.github.fullstacknick.outboxer.central;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.github.fullstacknick.outboxer.contract.ContractValidator;
import io.github.fullstacknick.outboxer.contract.ContractViolationException;
import io.github.fullstacknick.outboxer.contract.CycleEvent;
import io.github.fullstacknick.outboxer.contract.CycleReceipt;
import io.github.fullstacknick.outboxer.contract.Topics;
import jakarta.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class CentralMqttGateway implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(CentralMqttGateway.class);
    static final String CLOUD_RECEIVED_AT_HEADER = "outboxer-cloud-received-at";

    private final CentralProperties properties;
    private final ContractValidator validator;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper mapper;
    private final CentralMetrics metrics;
    private final Clock clock;
    private final ExecutorService routingExecutor =
            Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "central-mqtt-router"));
    private final List<ThreadPoolExecutor> ingressExecutors;
    private final AtomicBoolean connecting = new AtomicBoolean();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final AtomicBoolean redeliveryDisconnecting = new AtomicBoolean();
    private final AtomicLong deliveryEpoch = new AtomicLong();
    private final Mqtt5AsyncClient client;

    CentralMqttGateway(
            CentralProperties properties,
            ContractValidator validator,
            KafkaTemplate<String, String> kafkaTemplate,
            JsonMapper mapper,
            CentralMetrics metrics,
            Clock clock) {
        this.properties = properties;
        this.validator = validator;
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.metrics = metrics;
        this.clock = clock;
        if (properties.getIngressShards() < 1 || properties.getIngressShards() > 128) {
            throw new IllegalArgumentException("Ingress shards must be between 1 and 128");
        }
        if (properties.getIngressQueueCapacity() < 1 || properties.getIngressQueueCapacity() > 10_000) {
            throw new IllegalArgumentException("Ingress queue capacity must be between 1 and 10000");
        }
        this.ingressExecutors = new ArrayList<>(properties.getIngressShards());
        for (int shard = 0; shard < properties.getIngressShards(); shard++) {
            ingressExecutors.add(serialExecutor("central-mqtt-ingest-" + shard, properties.getIngressQueueCapacity()));
        }
        CentralProperties.MqttEndpoint endpoint = properties.getMqtt();
        this.client = MqttClient.builder()
                .useMqttVersion5()
                .identifier(endpoint.getClientId())
                .serverHost(endpoint.getHost())
                .serverPort(endpoint.getPort())
                .simpleAuth()
                .username(endpoint.getUsername())
                .password(endpoint.getPassword().getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .automaticReconnectWithDefaultConfig()
                .addDisconnectedListener(context -> {
                    subscribed.set(false);
                    deliveryEpoch.incrementAndGet();
                    redeliveryDisconnecting.set(false);
                })
                .buildAsync();
        // Install the flow before CONNECT so a resumed persistent session has a
        // handler ready before the broker releases queued QoS 1 publications.
        this.client.publishes(MqttGlobalPublishFilter.ALL, this::route, routingExecutor, true);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (properties.getMqtt().getPassword().isBlank()) {
            throw new IllegalStateException("Uplink MQTT password is required");
        }
        maintainConnection();
    }

    @Scheduled(fixedDelay = 1_000)
    void maintainConnection() {
        if (!client.getConfig().getState().isConnectedOrReconnect() && connecting.compareAndSet(false, true)) {
            client.connectWith()
                    .cleanStart(false)
                    .sessionExpiryInterval(86_400)
                    .restrictions()
                    .receiveMaximum(1_000)
                    .sendMaximum(1_000)
                    .applyRestrictions()
                    .send()
                    .whenComplete((ack, error) -> {
                        connecting.set(false);
                        if (error != null) {
                            logger.warn("uplink_mqtt_connect_failed reason={}", error.getClass().getSimpleName());
                        }
                    });
        }
        if (client.getConfig().getState().isConnected() && subscribed.compareAndSet(false, true)) {
            client.subscribeWith()
                    .topicFilter("uplink/+/+/cycles")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send()
                    .whenComplete((ack, error) -> {
                        if (error != null) {
                            subscribed.set(false);
                            logger.warn("uplink_mqtt_subscribe_failed reason={}", error.getClass().getSimpleName());
                        }
                    });
        }
    }

    boolean isReady() {
        return client.getConfig().getState().isConnected() && subscribed.get();
    }

    private void route(Mqtt5Publish publish) {
        if (stopping.get()) {
            return;
        }
        String topic = publish.getTopic().toString();
        int shard = Math.floorMod(machineKey(topic).hashCode(), ingressExecutors.size());
        long epoch = deliveryEpoch.get();
        try {
            ingressExecutors.get(shard).execute(() -> handle(publish, epoch));
        } catch (RejectedExecutionException exception) {
            if (!stopping.get()) {
                throw exception;
            }
        }
    }

    static String machineKey(String topic) {
        String[] segments = topic.split("/", -1);
        return segments.length == 4 && segments[0].equals("uplink") && segments[3].equals("cycles")
                ? segments[2]
                : topic;
    }

    private void handle(Mqtt5Publish publish, long epoch) {
        if (epoch != deliveryEpoch.get() || !client.getConfig().getState().isConnected()) {
            return;
        }
        byte[] payload = publish.getPayloadAsBytes();
        String topic = publish.getTopic().toString();
        try {
            Instant cloudReceivedAt = clock.instant();
            CycleEvent event = validator.readCanonical(payload);
            Topics.requireUplinkTopic(topic, event);
            RequestMetadata metadata = requireRequestMetadata(publish, event);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    KafkaTopicConfiguration.CYCLE_EVENTS,
                    event.machineId(),
                    new String(payload, StandardCharsets.UTF_8));
            record.headers().add(new RecordHeader(
                    CLOUD_RECEIVED_AT_HEADER,
                    cloudReceivedAt.toString().getBytes(StandardCharsets.UTF_8)));
            kafkaTemplate
                    .send(record)
                    .get(10, TimeUnit.SECONDS);
            if (!isCurrentDelivery(epoch)) {
                return;
            }
            delayReceiptWhenFaultInjectionIsEnabled();
            if (!isCurrentDelivery(epoch)) {
                return;
            }
            publishReceipt(metadata.responseTopic(), metadata.correlation(), CycleReceipt.accepted(event.eventId(), clock.instant()));
            if (!isCurrentDelivery(epoch)) {
                return;
            }
            metrics.received.increment();
            metrics.receiptsPublished.increment();
            publish.acknowledge();
        } catch (ContractViolationException exception) {
            handleInvalid(publish, topic, payload, exception.violations().getFirst(), epoch);
        } catch (Exception exception) {
            requestRedeliveryIfCurrent(topic, exception, epoch);
        }
    }

    private void handleInvalid(Mqtt5Publish publish, String topic, byte[] payload, String message, long epoch) {
        try {
            InvalidEventEnvelope invalid = new InvalidEventEnvelope(
                    "INVALID_CANONICAL_EVENT",
                    truncate(message, 256),
                    truncate(topic, 256),
                    clock.instant(),
                    sha256(payload),
                    truncate(new String(payload, StandardCharsets.UTF_8), 4_096));
            kafkaTemplate
                    .send(KafkaTopicConfiguration.INVALID_EVENTS, sha256(payload), mapper.writeValueAsString(invalid))
                    .get(10, TimeUnit.SECONDS);
            if (!isCurrentDelivery(epoch)) {
                return;
            }
            Optional<RequestMetadata> metadata = requestMetadataIfUsable(publish);
            if (metadata.isPresent()) {
                UUID eventId = UUID.fromString(new String(metadata.get().correlation(), StandardCharsets.UTF_8));
                publishReceipt(
                        metadata.get().responseTopic(),
                        metadata.get().correlation(),
                        CycleReceipt.rejected(eventId, clock.instant(), "INVALID_CANONICAL_EVENT", truncate(message, 256)));
                metrics.receiptsPublished.increment();
            }
            if (!isCurrentDelivery(epoch)) {
                return;
            }
            metrics.invalid.increment();
            publish.acknowledge();
        } catch (Exception exception) {
            requestRedeliveryIfCurrent(topic, exception, epoch);
        }
    }

    private RequestMetadata requireRequestMetadata(Mqtt5Publish publish, CycleEvent event) {
        RequestMetadata metadata = requestMetadataIfUsable(publish)
                .orElseThrow(() -> new ContractViolationException(List.of("response topic and correlation data are required")));
        if (!metadata.responseTopic().equals(Topics.receipts(event.siteId()))) {
            throw new ContractViolationException(List.of("response topic does not match site identity"));
        }
        String correlation = new String(metadata.correlation(), StandardCharsets.UTF_8);
        if (!correlation.equals(event.eventId().toString())) {
            throw new ContractViolationException(List.of("correlation data does not match eventId"));
        }
        return metadata;
    }

    private Optional<RequestMetadata> requestMetadataIfUsable(Mqtt5Publish publish) {
        if (publish.getResponseTopic().isEmpty() || publish.getCorrelationData().isEmpty()) {
            return Optional.empty();
        }
        byte[] correlation = copyBytes(publish.getCorrelationData().orElseThrow());
        try {
            UUID.fromString(new String(correlation, StandardCharsets.UTF_8));
        } catch (IllegalArgumentException exception) {
            return Optional.empty();
        }
        return Optional.of(new RequestMetadata(publish.getResponseTopic().orElseThrow().toString(), correlation));
    }

    private void publishReceipt(String responseTopic, byte[] correlation, CycleReceipt receipt) throws Exception {
        client.publishWith()
                .topic(responseTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .correlationData(correlation)
                .payload(validator.write(receipt))
                .send()
                .get(10, TimeUnit.SECONDS);
    }

    private void requestRedeliveryIfCurrent(String topic, Exception exception, long epoch) {
        if (epoch != deliveryEpoch.get()) {
            return;
        }
        logger.warn("mqtt_processing_failed topic={} reason={}", topic, exception.getClass().getSimpleName());
        if (redeliveryDisconnecting.compareAndSet(false, true) && deliveryEpoch.compareAndSet(epoch, epoch + 1)) {
            subscribed.set(false);
            client.disconnect().whenComplete((ignored, error) -> redeliveryDisconnecting.set(false));
        }
    }

    private boolean isCurrentDelivery(long epoch) {
        return epoch == deliveryEpoch.get() && client.getConfig().getState().isConnected();
    }

    private void delayReceiptWhenFaultInjectionIsEnabled() throws InterruptedException {
        if (properties.getReceiptDelayMillis() > 0) {
            Thread.sleep(properties.getReceiptDelayMillis());
        }
    }

    private static byte[] copyBytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static String sha256(byte[] payload) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static String truncate(String value, int maximum) {
        return value.length() <= maximum ? value : value.substring(0, maximum);
    }

    private static ThreadPoolExecutor serialExecutor(String name, int capacity) {
        return new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity),
                runnable -> new Thread(runnable, name),
                (task, executor) -> {
                    if (executor.isShutdown()) {
                        throw new RejectedExecutionException("Ingress worker is shutting down");
                    }
                    try {
                        executor.getQueue().put(task);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new RejectedExecutionException(
                                "Interrupted while applying ingress backpressure", exception);
                    }
                });
    }

    @PreDestroy
    void stop() {
        stopping.set(true);
        routingExecutor.shutdown();
        try {
            routingExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        ingressExecutors.forEach(ThreadPoolExecutor::shutdown);
        for (ThreadPoolExecutor executor : ingressExecutors) {
            try {
                executor.awaitTermination(15, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        try {
            client.disconnect().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            logger.debug("MQTT client was already disconnected");
        }
    }

    private record RequestMetadata(String responseTopic, byte[] correlation) {}
}
