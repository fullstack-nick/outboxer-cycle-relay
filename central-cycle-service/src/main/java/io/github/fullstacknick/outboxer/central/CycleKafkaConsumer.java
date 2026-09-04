package io.github.fullstacknick.outboxer.central;

import io.github.fullstacknick.outboxer.contract.ContractValidator;
import io.github.fullstacknick.outboxer.contract.ContractViolationException;
import io.github.fullstacknick.outboxer.contract.CycleEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class CycleKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CycleKafkaConsumer.class);

    private final ContractValidator validator;
    private final CyclePersistenceService persistence;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper mapper;
    private final CentralMetrics metrics;
    private final CentralProperties properties;
    private final Clock clock;
    private final AtomicLong committed = new AtomicLong();
    private final Set<String> loggedPersistenceFailures = ConcurrentHashMap.newKeySet();

    CycleKafkaConsumer(
            ContractValidator validator,
            CyclePersistenceService persistence,
            KafkaTemplate<String, String> kafkaTemplate,
            JsonMapper mapper,
            CentralMetrics metrics,
            CentralProperties properties,
            Clock clock) {
        this.validator = validator;
        this.persistence = persistence;
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    @KafkaListener(topics = KafkaTopicConfiguration.CYCLE_EVENTS, groupId = "outboxer-cycle-store")
    void consume(List<ConsumerRecord<String, String>> records) throws Exception {
        List<ValidatedRecord> valid = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                CycleEvent event = validator.readCanonical(record.value().getBytes(StandardCharsets.UTF_8));
                if (!event.machineId().equals(record.key())) {
                    throw new ContractViolationException(java.util.List.of("Kafka key does not match machineId"));
                }
                valid.add(new ValidatedRecord(record, event, cloudReceivedAt(record)));
            } catch (ContractViolationException exception) {
                publishInvalid(record, "INVALID_KAFKA_EVENT", exception.violations().getFirst());
                metrics.invalid.increment();
            }
        }
        if (valid.isEmpty()) {
            return;
        }

        List<CyclePersistenceService.Outcome> outcomes;
        try {
            outcomes = persistence.persistBatch(valid.stream()
                    .map(item -> new CyclePersistenceService.PersistCommand(
                            item.event(),
                            item.record().value(),
                            item.record().partition(),
                            item.record().offset(),
                            item.cloudReceivedAt()))
                    .toList());
            valid.forEach(item -> loggedPersistenceFailures.remove(position(item.record())));
        } catch (RuntimeException exception) {
            ConsumerRecord<String, String> first = valid.getFirst().record();
            String position = position(first);
            if (loggedPersistenceFailures.add(position)) {
                logger.error(
                        "cycle_batch_persistence_failed position={} batchSize={} reason={}",
                        position,
                        valid.size(),
                        exception.getMessage(),
                        exception);
            }
            throw exception;
        }

        for (int index = 0; index < valid.size(); index++) {
            processOutcome(valid.get(index), outcomes.get(index));
        }
    }

    private void processOutcome(ValidatedRecord item, CyclePersistenceService.Outcome outcome) throws Exception {
        ConsumerRecord<String, String> record = item.record();
        CycleEvent event = item.event();
        switch (outcome.result()) {
            case INSERTED -> {
                metrics.ingestionLagSeconds.record(
                        Math.max(0, Duration.between(event.occurredAt(), clock.instant()).toMillis() / 1000.0));
                if (outcome.detectedGaps() > 0) {
                    metrics.sequenceGaps.increment(outcome.detectedGaps());
                }
            }
            case DUPLICATE -> metrics.duplicates.increment();
            case CONFLICT -> {
                metrics.identityConflicts.increment();
                publishInvalid(record, "IDENTITY_CONFLICT", "Source identity conflicts with stored content");
            }
        }

        long count = committed.incrementAndGet();
        if (properties.getHaltAfterDatabaseCommitCount() > 0
                && count == properties.getHaltAfterDatabaseCommitCount()) {
            logger.atError()
                    .addKeyValue("faultMarker", "halt_after_database_commit")
                    .addKeyValue("eventId", event.eventId())
                    .addKeyValue("machineId", event.machineId())
                    .addKeyValue("siteId", event.siteId())
                    .addKeyValue("sequenceNumber", event.sequenceNumber())
                    .addKeyValue("partition", record.partition())
                    .addKeyValue("offset", record.offset())
                    .log("Injected central crash after database commit");
            Runtime.getRuntime().halt(71);
        }
        logger.atDebug()
                .addKeyValue("result", outcome.result())
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("machineId", event.machineId())
                .addKeyValue("siteId", event.siteId())
                .addKeyValue("sequenceNumber", event.sequenceNumber())
                .addKeyValue("schemaVersion", event.schemaVersion())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .log("Cycle event processed");
    }

    private static String position(ConsumerRecord<String, String> record) {
        return record.topic() + '-' + record.partition() + '@' + record.offset();
    }

    private Instant cloudReceivedAt(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader(CentralMqttGateway.CLOUD_RECEIVED_AT_HEADER);
        if (header == null) {
            return clock.instant();
        }
        try {
            return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
        } catch (RuntimeException exception) {
            throw new ContractViolationException(java.util.List.of("cloud receive timestamp header is invalid"));
        }
    }

    private void publishInvalid(ConsumerRecord<String, String> record, String code, String message) throws Exception {
        byte[] payload = record.value().getBytes(StandardCharsets.UTF_8);
        InvalidEventEnvelope envelope = new InvalidEventEnvelope(
                code,
                truncate(message, 256),
                record.topic(),
                clock.instant(),
                sha256(payload),
                truncate(record.value(), 4_096));
        kafkaTemplate
                .send(KafkaTopicConfiguration.INVALID_EVENTS, record.key(), mapper.writeValueAsString(envelope))
                .get(10, TimeUnit.SECONDS);
    }

    private static String truncate(String value, int maximum) {
        return value.length() <= maximum ? value : value.substring(0, maximum);
    }

    private static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private record ValidatedRecord(
            ConsumerRecord<String, String> record, CycleEvent event, Instant cloudReceivedAt) {}
}
