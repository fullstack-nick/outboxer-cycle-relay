package io.github.fullstacknick.outboxer.contract;

import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ContractValidator {

    public static final int MAX_PAYLOAD_BYTES = 65_536;

    private final JsonMapper jsonMapper;
    private final Clock clock;
    private final Duration futureTolerance;
    private final Schema sourceV1;
    private final Schema canonicalV1;
    private final Schema receiptV1;

    public ContractValidator(JsonMapper jsonMapper, Clock clock, Duration futureTolerance) {
        this.jsonMapper = jsonMapper;
        this.clock = clock;
        this.futureTolerance = futureTolerance;
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        this.sourceV1 = load(registry, "/machine-cycle-completed-v1.schema.json");
        this.canonicalV1 = load(registry, "/cycle-completed-v1.schema.json");
        this.receiptV1 = load(registry, "/cycle-receipt-v1.schema.json");
    }

    public MachineCycleEvent readSource(byte[] payload) {
        JsonNode node = parseAndValidate(payload, sourceV1, "source");
        validateNotFuture(node.path("occurredAt").asText());
        return jsonMapper.treeToValue(node, MachineCycleEvent.class);
    }

    public CycleEvent readCanonical(byte[] payload) {
        JsonNode node = parseAndValidate(payload, canonicalV1, "canonical");
        validateNotFuture(node.path("occurredAt").asText());
        validateNotFuture(node.path("edgeReceivedAt").asText());
        return jsonMapper.treeToValue(node, CycleEvent.class);
    }

    public CycleReceipt readReceipt(byte[] payload) {
        JsonNode node = parseAndValidate(payload, receiptV1, "receipt");
        return jsonMapper.treeToValue(node, CycleReceipt.class);
    }

    public byte[] write(Object value) {
        return jsonMapper.writeValueAsBytes(value);
    }

    private JsonNode parseAndValidate(byte[] payload, Schema schema, String boundary) {
        if (payload == null || payload.length == 0) {
            throw new ContractViolationException(List.of(boundary + " payload is empty"));
        }
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new ContractViolationException(List.of(boundary + " payload exceeds 65536 bytes"));
        }
        try {
            JsonNode node = jsonMapper.readTree(payload);
            List<Error> errors = schema.validate(node);
            if (!errors.isEmpty()) {
                List<String> messages = errors.stream()
                        .map(error -> error.getInstanceLocation() + ": " + error.getMessage())
                        .sorted()
                        .toList();
                throw new ContractViolationException(messages);
            }
            return node;
        } catch (ContractViolationException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new ContractViolationException(List.of(boundary + " payload is not valid JSON"));
        }
    }

    private void validateNotFuture(String value) {
        List<String> violations = new ArrayList<>();
        try {
            Instant instant = Instant.parse(value);
            if (instant.isAfter(clock.instant().plus(futureTolerance))) {
                violations.add("timestamp is beyond the allowed future-clock tolerance");
            }
        } catch (DateTimeParseException exception) {
            violations.add("timestamp is not a valid UTC instant");
        }
        if (!violations.isEmpty()) {
            throw new ContractViolationException(violations);
        }
    }

    private static Schema load(SchemaRegistry registry, String resource) {
        try (InputStream input = ContractValidator.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("Missing contract resource " + resource);
            }
            return registry.getSchema(input);
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot load contract resource " + resource, exception);
        }
    }
}
