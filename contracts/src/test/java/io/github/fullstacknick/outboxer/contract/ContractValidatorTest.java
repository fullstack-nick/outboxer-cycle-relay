package io.github.fullstacknick.outboxer.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class ContractValidatorTest {

    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private final ContractValidator validator = new ContractValidator(
            mapper, Clock.fixed(Instant.parse("2026-09-04T09:00:00Z"), ZoneOffset.UTC), Duration.ofMinutes(5));

    @Test
    void readsValidSourceAndCanonicalFixtures() throws IOException {
        MachineCycleEvent source = validator.readSource(resource("machine-cycle-v1.valid.json"));
        CycleEvent canonical = validator.readCanonical(resource("cycle-v1.valid.json"));
        MachineCycleEvent sourceV2 = validator.readSource(resource("machine-cycle-v2.valid.json"));
        CycleEvent canonicalV2 = validator.readCanonical(resource("cycle-v2.valid.json"));

        assertThat(source.machineId()).isEqualTo("IMM-0042");
        assertThat(canonical.eventId()).isEqualTo(source.eventId());
        assertThat(canonical.edgeReceivedAt()).isAfter(source.occurredAt());
        assertThat(source.payload().energyConsumptionWh()).isNull();
        assertThat(sourceV2.schemaVersion()).isEqualTo(2);
        assertThat(sourceV2.payload().energyConsumptionWh()).isEqualTo(312.5);
        assertThat(canonicalV2.eventId()).isEqualTo(sourceV2.eventId());
        assertThat(canonicalV2.payload().energyConsumptionWh()).isEqualTo(312.5);
    }

    @Test
    void preservesV1WireShapeAndAllowsOnlyV2ToCarryEnergy() throws IOException {
        MachineCycleEvent sourceV1 = validator.readSource(resource("machine-cycle-v1.valid.json"));
        String serializedV1 = new String(validator.write(sourceV1));
        String v1WithEnergy = new String(resource("machine-cycle-v1.valid.json"))
                .replace("\"rejectedParts\": 0", "\"rejectedParts\": 0,\n    \"energyConsumptionWh\": 312.5");
        String negativeV2 = new String(resource("machine-cycle-v2.valid.json"))
                .replace("\"energyConsumptionWh\": 312.5", "\"energyConsumptionWh\": -0.1");

        assertThat(serializedV1).doesNotContain("energyConsumptionWh");
        assertThatThrownBy(() -> validator.readSource(v1WithEnergy.getBytes()))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("energyConsumptionWh");
        assertThatThrownBy(() -> validator.readSource(negativeV2.getBytes()))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("minimum");
    }

    @Test
    void rejectsNegativeDuration() throws IOException {
        assertThatThrownBy(() -> validator.readSource(resource("machine-cycle-v1.invalid-negative-duration.json")))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("minimum");
    }

    @Test
    void rejectsAnEdgeOwnedTimestampAtTheSourceBoundary() throws IOException {
        String source = new String(resource("machine-cycle-v1.valid.json"));
        String injected = source.replace("\"occurredAt\":", "\"edgeReceivedAt\": \"2026-09-04T08:14:31.500Z\",\n  \"occurredAt\":");

        assertThatThrownBy(() -> validator.readSource(injected.getBytes()))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("edgeReceivedAt");
    }

    @Test
    void rejectsTimestampBeyondTolerance() throws IOException {
        String source = new String(resource("machine-cycle-v1.valid.json"))
                .replace("2026-09-04T08:14:31.482Z", "2026-09-04T09:06:00Z");

        assertThatThrownBy(() -> validator.readSource(source.getBytes()))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("future-clock tolerance");
    }

    @Test
    void verifiesTopicIdentity() throws IOException {
        MachineCycleEvent source = validator.readSource(resource("machine-cycle-v1.valid.json"));

        Topics.requireFactoryTopic("factory/IMM-0042/cycles", source);
        assertThatThrownBy(() -> Topics.requireFactoryTopic("factory/IMM-9999/cycles", source))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void rejectsUnknownVersionsAndUnexpectedFields() throws IOException {
        String valid = new String(resource("machine-cycle-v1.valid.json"));

        assertThatThrownBy(() -> validator.readSource(valid.replace("\"schemaVersion\": 1", "\"schemaVersion\": 9").getBytes()))
                .isInstanceOf(ContractViolationException.class);
        assertThatThrownBy(() -> validator.readSource(valid.replace("\"type\":", "\"unexpected\": true,\n  \"type\":").getBytes()))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("unexpected");
    }

    @Test
    void rejectsEmptyAndOversizedPayloads() {
        assertThatThrownBy(() -> validator.readSource(new byte[0]))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("empty");
        assertThatThrownBy(() -> validator.readSource(new byte[ContractValidator.MAX_PAYLOAD_BYTES + 1]))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("65536");
    }

    @Test
    void rejectsUnsafeTopicIdentifiers() {
        assertThatThrownBy(() -> Topics.factoryCycles("machine/+"))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("unsupported");
        assertThatThrownBy(() -> Topics.receipts("site/#"))
                .isInstanceOf(ContractViolationException.class)
                .hasMessageContaining("unsupported");
    }

    @Test
    void validatesAcceptedAndRejectedReceipts() {
        byte[] accepted = """
                {
                  "schemaVersion": 1,
                  "eventId": "de73fc96-87ab-460f-b24f-a81c51dfec6f",
                  "status": "ACCEPTED",
                  "processedAt": "2026-09-04T08:14:31.731Z"
                }
                """.getBytes();
        String invalidAccepted = new String(accepted).replace(
                "\"processedAt\":", "\"errorCode\": \"NOT_ALLOWED\",\n  \"processedAt\":");

        assertThat(validator.readReceipt(accepted).status()).isEqualTo(ReceiptStatus.ACCEPTED);
        assertThatThrownBy(() -> validator.readReceipt(invalidAccepted.getBytes()))
                .isInstanceOf(ContractViolationException.class);
    }

    private static byte[] resource(String name) throws IOException {
        try (var input = ContractValidatorTest.class.getResourceAsStream("/examples/" + name)) {
            if (input == null) {
                throw new IOException("Missing fixture " + name);
            }
            return input.readAllBytes();
        }
    }
}
