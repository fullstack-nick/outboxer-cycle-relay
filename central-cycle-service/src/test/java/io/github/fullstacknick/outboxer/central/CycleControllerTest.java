package io.github.fullstacknick.outboxer.central;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class CycleControllerTest {

    private CycleQueryRepository repository;
    private WebTestClient client;

    @BeforeEach
    void setUp() {
        repository = Mockito.mock(CycleQueryRepository.class);
        client = WebTestClient.bindToController(new CycleController(repository)).build();
    }

    @Test
    void scopesLatestCycleByRequiredTenantHeader() {
        when(repository.latest("tenant-017", "IMM-0042")).thenReturn(Mono.just(response()));

        client.get()
                .uri("/api/v1/machines/IMM-0042/cycles/latest")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .valueEquals("Cache-Control", "no-store")
                .expectBody()
                .jsonPath("$.machineId")
                .isEqualTo("IMM-0042")
                .jsonPath("$.schemaVersion")
                .isEqualTo(1)
                .consumeWith(result -> assertThat(new String(result.getResponseBody()))
                        .contains("\"energyConsumptionWh\":null"));

        verify(repository).latest("tenant-017", "IMM-0042");
    }

    @Test
    void returnsTheV2EnergyValue() {
        when(repository.latest("tenant-017", "IMM-0042")).thenReturn(Mono.just(response(2, 318.75)));

        client.get()
                .uri("/api/v1/machines/IMM-0042/cycles/latest")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.schemaVersion")
                .isEqualTo(2)
                .jsonPath("$.energyConsumptionWh")
                .isEqualTo(318.75);
    }

    @Test
    void rejectsMissingTenantScope() {
        client.get()
                .uri("/api/v1/machines/IMM-0042/cycles/latest")
                .exchange()
                .expectStatus()
                .isBadRequest();

        verifyNoInteractions(repository);
    }

    @Test
    void boundsRecentCycleLimit() {
        client.get()
                .uri("/api/v1/machines/IMM-0042/cycles?limit=101")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isBadRequest();

        verifyNoInteractions(repository);
    }

    @Test
    void returnsRecentCyclesAndQualityOnTheOnlySupportedResources() {
        when(repository.recent("tenant-017", "IMM-0042", 2)).thenReturn(Flux.just(response()));
        when(repository.quality("tenant-017", "IMM-0042"))
                .thenReturn(Mono.just(new DataQualityResponse("IMM-0042", 41, 0, 2, Instant.parse("2026-09-04T12:00:03Z"))));

        client.get()
                .uri("/api/v1/machines/IMM-0042/cycles?limit=2")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$[0].eventId")
                .isEqualTo("de73fc96-87ab-460f-b24f-a81c51dfec6f");

        client.get()
                .uri("/api/v1/machines/IMM-0042/data-quality")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.duplicateEventCount")
                .isEqualTo(2);

        verify(repository).recent("tenant-017", "IMM-0042", 2);
        verify(repository).quality("tenant-017", "IMM-0042");
    }

    @Test
    void returnsNotFoundForAnUnknownMachine() {
        when(repository.latest("tenant-017", "IMM-9999")).thenReturn(Mono.empty());

        client.get()
                .uri("/api/v1/machines/IMM-9999/cycles/latest")
                .header("X-Tenant-Id", "tenant-017")
                .exchange()
                .expectStatus()
                .isNotFound();
    }

    private static CycleResponse response() {
        return response(1, null);
    }

    private static CycleResponse response(int schemaVersion, Double energyConsumptionWh) {
        return new CycleResponse(
                schemaVersion,
                UUID.fromString("de73fc96-87ab-460f-b24f-a81c51dfec6f"),
                "tenant-017",
                "site-north-01",
                "IMM-0042",
                UUID.fromString("f16fa35a-836c-4246-b707-f05e0ff491e2"),
                41,
                Instant.parse("2026-09-04T12:00:00Z"),
                Instant.parse("2026-09-04T12:00:01Z"),
                Instant.parse("2026-09-04T12:00:02Z"),
                Instant.parse("2026-09-04T12:00:03Z"),
                41,
                20_000,
                4_000,
                700,
                11_000,
                2_000,
                1_200,
                230,
                4,
                0,
                energyConsumptionWh);
    }
}
