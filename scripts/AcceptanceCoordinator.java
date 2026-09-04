import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Cross-platform Docker Compose acceptance coordinator.
 *
 * <p>PowerShell, Bash, and Gradle all invoke this Java 21 source file so the assertions have one
 * implementation. It deliberately operates only on the Compose project named {@code outboxer}.
 */
public final class AcceptanceCoordinator {

    private static final Path ROOT = Path.of("").toAbsolutePath().normalize();
    private static final Path COMPOSE_FILE = ROOT.resolve("infra/compose.yaml");
    private static final Path ENV_FILE = ROOT.resolve(".secrets/compose.env");
    private static final Path EVIDENCE_DIRECTORY = ROOT.resolve(".outboxer/evidence");
    private static final String PROJECT = "outboxer";
    private static final String EDGE = "outboxer-edge-relay";
    private static final String CENTRAL = "outboxer-central-cycle-service";
    private static final String FACTORY = "outboxer-factory-mqtt";
    private static final String UPLINK = "outboxer-uplink-mqtt";
    private static final String KAFKA = "outboxer-kafka";
    private static final String POSTGRES = "outboxer-postgres";
    private static final Duration COMMAND_TIMEOUT = Duration.ofMinutes(20);
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private final String mode;
    private final Instant startedAt = Instant.now();
    private final Map<String, Boolean> assertions = new LinkedHashMap<>();
    private final Map<String, Object> measurements = new LinkedHashMap<>();
    private Throwable failure;

    private AcceptanceCoordinator(String mode) {
        this.mode = mode;
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1 || !Set.of("smoke", "load", "crash", "outage").contains(arguments[0])) {
            throw new IllegalArgumentException("Usage: java scripts/AcceptanceCoordinator.java smoke|load|crash|outage");
        }
        AcceptanceCoordinator coordinator = new AcceptanceCoordinator(arguments[0]);
        int exitCode = 0;
        try {
            coordinator.execute();
        } catch (Throwable throwable) {
            coordinator.failure = throwable;
            exitCode = 1;
            System.err.println("ACCEPTANCE FAILED: " + throwable.getMessage());
            throwable.printStackTrace(System.err);
        } finally {
            try {
                coordinator.writeReport();
            } catch (Throwable reportFailure) {
                reportFailure.printStackTrace(System.err);
                exitCode = 1;
            }
            if (coordinator.cleanupRequested()) {
                try {
                    coordinator.destroyProject("explicit OUTBOXER_CLEANUP=true");
                } catch (Throwable cleanupFailure) {
                    cleanupFailure.printStackTrace(System.err);
                    exitCode = 1;
                }
            }
        }
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private void execute() throws Exception {
        requireWorkspace();
        requireCommand("docker", "version", "--format", "{{.Server.Version}}");
        ensureSecrets();
        validateComposeScope();
        preflight();
        switch (mode) {
            case "smoke" -> runSmoke();
            case "load" -> runLoad();
            case "crash" -> runCrashWindows();
            case "outage" -> runOutage();
            default -> throw new IllegalStateException("Unexpected mode " + mode);
        }
    }

    private void runSmoke() throws Exception {
        resetIfRequested(false);
        startBaseStack();
        assertOperationalSecurity();

        Path expected = freshExpectedFile("smoke");
        String runId = runId("smoke");
        runSimulator(runId, expected, 2, 1, 1);
        Set<String> initialIds = expectedIds(expected);
        waitForDatabaseIds(initialIds, Duration.ofMinutes(2));
        waitUntil("edge outbox to drain", Duration.ofMinutes(2), () -> edgePending() == 0);
        assertCondition(
                "happy_path_exact_rows",
                initialIds.stream().allMatch(id -> postgresCount(id) == 1),
                "two source IDs reached PostgreSQL once each");
        assertApi("IMM-0001");

        long edgeRejectedBefore = edgeScalar("SELECT COUNT(*) FROM edge_rejected_message");
        publishFactory("factory/IMM-0001/cycles", Files.readAllBytes(ROOT.resolve(
                "contracts/examples/machine-cycle-v1.invalid-negative-duration.json")));
        waitUntil(
                "factory invalid message quarantine",
                Duration.ofSeconds(30),
                () -> edgeScalar("SELECT COUNT(*) FROM edge_rejected_message") > edgeRejectedBefore);
        assertCondition("factory_invalid_isolated", true, "malformed source was durably quarantined");

        long invalidOffsetBefore = invalidTopicEndOffset();
        publishUplink("uplink/site-north-01/IMM-0001/cycles", "{\"schemaVersion\":99}".getBytes(StandardCharsets.UTF_8));
        waitUntil(
                "central invalid topic publication",
                Duration.ofSeconds(45),
                () -> invalidTopicEndOffset() > invalidOffsetBefore);
        assertCondition("central_invalid_isolated", true, "malformed canonical input reached the invalid topic");

        Path validAfterInvalid = freshExpectedFile("valid-after-invalid");
        runSimulator(runId("valid-after-invalid"), validAfterInvalid, 1, 1, 1);
        waitForDatabaseIds(expectedIds(validAfterInvalid), Duration.ofMinutes(2));
        assertCondition("valid_after_invalid", true, "a later valid event traversed both isolation boundaries");

        verifyApplicationReceiptBoundary();
        verifyFactoryBrokerPersistence();
        assertMetrics();
        measurements.put("postgresRows", postgresScalar("SELECT COUNT(*) FROM cycle_event"));
        measurements.put("edgeRejectedRows", edgeScalar("SELECT COUNT(*) FROM edge_rejected_message"));
        measurements.put("invalidTopicEndOffset", invalidTopicEndOffset());
    }

    private void verifyApplicationReceiptBoundary() throws Exception {
        compose(Map.of(), "stop", "central-cycle-service");
        Path expected = freshExpectedFile("receipt-boundary");
        runSimulator(runId("receipt-boundary"), expected, 1, 1, 1);
        waitUntil("edge pending without central receipt", Duration.ofSeconds(30), () -> edgePending() >= 1);
        Thread.sleep(2_000);
        Set<String> ids = expectedIds(expected);
        assertCondition(
                "mqtt_puback_is_not_completion",
                edgePending() >= 1 && postgresCount(ids.iterator().next()) == 0,
                "broker PUBACK left the edge row pending while the central service was stopped");

        List<String> beforeRestart = edgePendingIds();
        command(COMMAND_TIMEOUT, Map.of(), null, "docker", "kill", EDGE);
        compose(Map.of(), "start", "edge-relay");
        waitForContainerRunning(EDGE, Duration.ofMinutes(2));
        waitForHttp("http://127.0.0.1:18081/actuator/health/liveness", 200, Duration.ofMinutes(2));
        List<String> afterRestart = edgePendingIds();
        assertCondition(
                "edge_restart_preserves_pending",
                new HashSet<>(afterRestart).containsAll(beforeRestart),
                "all pre-crash pending event IDs remained durable");

        compose(Map.of(), "start", "central-cycle-service");
        waitForHealthy(CENTRAL, Duration.ofMinutes(3));
        waitForDatabaseIds(ids, Duration.ofMinutes(2));
        waitUntil("receipt completes pending row", Duration.ofMinutes(2), () -> edgePending() == 0);
        assertCondition("application_receipt_completes_row", true, "Kafka-backed receipt changed the edge row to SENT");
    }

    private void verifyFactoryBrokerPersistence() throws Exception {
        compose(Map.of(), "stop", "edge-relay");
        Path expected = freshExpectedFile("factory-session");
        runSimulator(runId("factory-session"), expected, 1, 1, 1);
        compose(Map.of(), "restart", "factory-mqtt");
        waitForHealthy(FACTORY, Duration.ofMinutes(2));
        compose(Map.of(), "start", "edge-relay");
        waitForHealthy(EDGE, Duration.ofMinutes(3));
        Set<String> ids = expectedIds(expected);
        waitForDatabaseIds(ids, Duration.ofMinutes(2));
        waitUntil("broker-session event receipt", Duration.ofMinutes(2), () -> edgePending() == 0);
        assertCondition(
                "factory_session_survives_broker_restart",
                ids.stream().allMatch(id -> postgresCount(id) == 1),
                "queued QoS 1 source delivery survived a broker restart");
    }

    private void runLoad() throws Exception {
        requireReset();
        resetIfRequested(true);
        startBaseStack();
        int durationSeconds = integerEnvironment("OUTBOXER_LOAD_SECONDS", 60, 10, 3_600);
        long eventCount = durationSeconds * 100L;
        Path expected = freshExpectedFile("load");
        runSimulator(runId("load"), expected, eventCount, 3_000, 30);
        double publishSeconds = sourcePublishSeconds(expected);
        Set<String> ids = expectedIds(expected);
        waitForDatabaseIds(ids, Duration.ofMinutes(5));
        waitUntil("load outbox drain", Duration.ofMinutes(5), () -> edgePending() == 0);
        double sourceRate = (eventCount - 1) / publishSeconds;

        measurements.put("events", eventCount);
        measurements.put("sourceEventsPerSecond", round(sourceRate));
        measurements.put("durationSeconds", round(publishSeconds));

        assertCondition("load_expected_identities", ids.size() == eventCount, "simulator emitted every expected identity");
        assertCondition("load_complete", databaseContainsExactly(ids), "every expected identity reached PostgreSQL");
        assertCondition("load_no_sequence_gaps", sequenceViolationCount() == 0, "all per-machine sequences are contiguous");
        assertCondition("load_rate_near_100_eps", sourceRate >= 90 && sourceRate <= 115, "source pacing remained near 100 events/s");
    }

    private void runCrashWindows() throws Exception {
        requireReset();
        resetIfRequested(true);
        startBaseStack();
        int repetitions = integerEnvironment("OUTBOXER_CRASH_REPETITIONS", 3, 1, 10);
        long duplicateBaseline = duplicateCount("IMM-0001");

        for (int iteration = 1; iteration <= repetitions; iteration++) {
            System.out.printf("Edge crash-window repetition %d/%d%n", iteration, repetitions);
            Map<String, String> fault = Map.of(
                    "EDGE_FAULT_HALT_AFTER_PUBLISH_COUNT", "1",
                    "CENTRAL_FAULT_RECEIPT_DELAY_MILLIS", "15000");
            compose(fault, "up", "-d", "--no-deps", "--force-recreate", "central-cycle-service", "edge-relay");
            waitForHealthy(CENTRAL, Duration.ofMinutes(3));
            waitForHealthy(EDGE, Duration.ofMinutes(3));

            Path expected = freshExpectedFile("edge-crash-" + iteration);
            runSimulator(runId("edge-crash-" + iteration), expected, 1, 1, 1);
            waitForLog(EDGE, "halt_after_uplink_publish", Duration.ofMinutes(2));
            compose(
                    Map.of("EDGE_FAULT_HALT_AFTER_PUBLISH_COUNT", "0"),
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "edge-relay");
            waitForHealthy(EDGE, Duration.ofMinutes(3));
            Set<String> ids = expectedIds(expected);
            waitForDatabaseIds(ids, Duration.ofMinutes(3));
            waitUntil("edge crash replay receipt", Duration.ofMinutes(3), () -> edgePending() == 0);
            long wantedDuplicates = duplicateBaseline + iteration;
            waitUntil(
                    "edge crash duplicate observation",
                    Duration.ofMinutes(2),
                    () -> duplicateCount("IMM-0001") >= wantedDuplicates);
            assertCondition(
                    "edge_crash_exactly_one_row_" + iteration,
                    postgresCount(ids.iterator().next()) == 1,
                    "edge retry remained customer-visible once");
            compose(
                    Map.of("CENTRAL_FAULT_RECEIPT_DELAY_MILLIS", "0"),
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "central-cycle-service");
            waitForHealthy(CENTRAL, Duration.ofMinutes(3));
        }

        long centralDuplicateBaseline = duplicateCount("IMM-0001");
        for (int iteration = 1; iteration <= repetitions; iteration++) {
            System.out.printf("Central crash-window repetition %d/%d%n", iteration, repetitions);
            compose(
                    Map.of("CENTRAL_FAULT_HALT_AFTER_DATABASE_COMMIT_COUNT", "1"),
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "central-cycle-service");
            waitForHealthy(CENTRAL, Duration.ofMinutes(3));

            Path expected = freshExpectedFile("central-crash-" + iteration);
            runSimulator(runId("central-crash-" + iteration), expected, 1, 1, 1);
            waitForLog(CENTRAL, "halt_after_database_commit", Duration.ofMinutes(2));
            compose(
                    Map.of("CENTRAL_FAULT_HALT_AFTER_DATABASE_COMMIT_COUNT", "0"),
                    "up",
                    "-d",
                    "--no-deps",
                    "--force-recreate",
                    "central-cycle-service");
            waitForHealthy(CENTRAL, Duration.ofMinutes(3));
            Set<String> ids = expectedIds(expected);
            waitForDatabaseIds(ids, Duration.ofMinutes(3));
            long wantedDuplicates = centralDuplicateBaseline + iteration;
            waitUntil(
                    "central crash Kafka replay",
                    Duration.ofMinutes(3),
                    () -> duplicateCount("IMM-0001") >= wantedDuplicates);
            assertCondition(
                    "central_crash_exactly_one_row_" + iteration,
                    postgresCount(ids.iterator().next()) == 1,
                    "post-commit Kafka replay remained customer-visible once");
        }

        assertCondition(
                "edge_crash_repeated",
                duplicateCount("IMM-0001") >= duplicateBaseline + repetitions * 2L,
                "both crash windows produced harmless reprocessing in every repetition");
        measurements.put("repetitionsPerCrashWindow", repetitions);
        measurements.put("duplicateEventsObserved", duplicateCount("IMM-0001"));
    }

    private void runOutage() throws Exception {
        requireReset();
        resetIfRequested(true);
        startBaseStack();
        int outageSeconds = integerEnvironment("OUTBOXER_OUTAGE_SECONDS", 600, 10, 3_600);
        Path expected = freshExpectedFile("outage");
        String runId = runId("outage");
        startSimulator(runId, expected, 3_000, 30);
        waitUntil("simulator baseline", Duration.ofMinutes(2), () -> fileLineCount(expected) >= 100);
        waitUntil(
                "central baseline",
                Duration.ofMinutes(2),
                () -> postgresScalar("SELECT COUNT(*) FROM cycle_event") >= 100);
        waitUntil("baseline outbox drain", Duration.ofMinutes(2), () -> edgePending() <= 20);

        compose(Map.of(), "stop", "uplink-mqtt");
        long expectedAtOutageStart = fileLineCount(expected);
        Instant outageStarted = Instant.now();
        boolean restartVerified = false;
        long nextProgress = 0;
        while (Duration.between(outageStarted, Instant.now()).getSeconds() < outageSeconds) {
            long elapsed = Duration.between(outageStarted, Instant.now()).getSeconds();
            if (!restartVerified && elapsed >= Math.max(5, outageSeconds / 2)) {
                List<String> before = edgePendingIds();
                command(COMMAND_TIMEOUT, Map.of(), null, "docker", "kill", EDGE);
                command(COMMAND_TIMEOUT, Map.of(), null, "docker", "start", EDGE);
                waitForContainerRunning(EDGE, Duration.ofMinutes(2));
                waitForHttp("http://127.0.0.1:18081/actuator/health/liveness", 200, Duration.ofMinutes(3));
                List<String> after = edgePendingIds();
                assertCondition(
                        "outage_edge_restart_preserves_pending",
                        new HashSet<>(after).containsAll(before),
                        "the abrupt restart retained every snapshotted pending ID");
                restartVerified = true;
            }
            if (elapsed >= nextProgress) {
                System.out.printf(
                        Locale.ROOT,
                        "Outage progress: %ds/%ds, expected=%d, pending=%d%n",
                        elapsed,
                        outageSeconds,
                        fileLineCount(expected) - expectedAtOutageStart,
                        edgePending());
                nextProgress = elapsed + 30;
            }
            Thread.sleep(Math.min(10_000, Math.max(250, (outageSeconds - elapsed) * 1_000L)));
        }

        waitUntil(
                "factory-side catch-up after edge restart",
                Duration.ofMinutes(2),
                () -> edgePending() >= (fileLineCount(expected) - expectedAtOutageStart) * 0.95);
        long outageExpected = fileLineCount(expected) - expectedAtOutageStart;
        long outagePending = edgePending();
        double actualOutageSeconds = secondsBetween(outageStarted, Instant.now());
        double target = actualOutageSeconds * 100.0;
        int livenessStatus = httpStatus("http://127.0.0.1:18081/actuator/health/liveness");
        int readinessStatus = httpStatus("http://127.0.0.1:18081/actuator/health/readiness");
        assertCondition(
                "outage_source_rate",
                outageExpected >= target * 0.90 && outageExpected <= target * 1.10,
                "factory intake stayed near 100 events/s");
        assertCondition(
                "outage_pending_matches_source",
                outagePending >= outageExpected * 0.95,
                "nearly every outage event was present in the durable edge outbox");
        assertCondition(
                "outage_liveness_stays_up",
                livenessStatus == 200,
                "edge liveness stayed healthy without its uplink");
        assertCondition(
                "outage_readiness_degrades",
                readinessStatus == 503,
                "edge readiness exposed the unavailable dependency/backlog");

        Instant restoreStarted = Instant.now();
        compose(Map.of(), "start", "uplink-mqtt");
        waitForHealthy(UPLINK, Duration.ofMinutes(2));
        waitForHttp("http://127.0.0.1:18080/actuator/health/readiness", 200, Duration.ofMinutes(3));
        double restoreReadinessSeconds = secondsBetween(restoreStarted, Instant.now());
        long databaseAtRecovery = postgresScalar("SELECT COUNT(*) FROM cycle_event");
        long expectedAtRecovery = fileLineCount(expected);
        Instant recoveryStarted = Instant.now();

        long recoveryDeadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
        long recoveryProgress = 0;
        long recoveryTargetRows = -1;
        Instant recoveryFinished = null;
        while (System.nanoTime() < recoveryDeadline) {
            long pending = edgePending();
            long elapsed = Duration.between(recoveryStarted, Instant.now()).getSeconds();
            long liveEvents = fileLineCount(expected) - expectedAtRecovery;
            if (recoveryTargetRows < 0 && pending <= Math.max(10, outagePending / 100) && liveEvents >= 500) {
                recoveryTargetRows = fileLineCount(expected);
                System.out.printf(
                        Locale.ROOT,
                        "Recovery checkpoint: pending=%d, liveEvents=%d, databaseTarget=%d%n",
                        pending,
                        liveEvents,
                        recoveryTargetRows);
            }
            if (recoveryTargetRows >= 0 && postgresScalar("SELECT COUNT(*) FROM cycle_event") >= recoveryTargetRows) {
                recoveryFinished = Instant.now();
                break;
            }
            if (elapsed >= recoveryProgress) {
                System.out.printf(
                        Locale.ROOT,
                        "Recovery progress: %ds, pending=%d, liveEvents=%d%n",
                        elapsed,
                        pending,
                        liveEvents);
                recoveryProgress = elapsed + 20;
            }
            Thread.sleep(1_000);
        }
        if (recoveryFinished == null) {
            throw new IllegalStateException("Recovery did not reach its database checkpoint within ten minutes");
        }
        double recoverySeconds = secondsBetween(recoveryStarted, recoveryFinished);
        long recoveredRows = recoveryTargetRows - databaseAtRecovery;
        double grossRecoveryRate = recoveredRows / recoverySeconds;

        compose(Map.of(), "--profile", "simulation", "stop", "-t", "20", "machine-simulator");
        waitUntil("final edge outbox drain", Duration.ofMinutes(2), () -> edgePending() == 0);
        Set<String> expectedIds = expectedIds(expected);
        waitForDatabaseIds(expectedIds, Duration.ofMinutes(5));
        long databaseAfterRecovery = postgresScalar("SELECT COUNT(*) FROM cycle_event");
        long liveDuringRecovery = fileLineCount(expected) - expectedAtRecovery;

        measurements.put("machineCount", 3_000);
        measurements.put("cycleIntervalSeconds", 30);
        measurements.put("outageSeconds", outageSeconds);
        measurements.put("actualOutageSeconds", round(actualOutageSeconds));
        measurements.put("outageEvents", outageExpected);
        measurements.put("peakPending", outagePending);
        measurements.put("restoreReadinessSeconds", round(restoreReadinessSeconds));
        measurements.put("liveEventsDuringRecovery", liveDuringRecovery);
        measurements.put("totalExpectedEvents", expectedIds.size());
        measurements.put("postgresRows", databaseAfterRecovery);
        measurements.put("recoveredRowsDuringWindow", recoveredRows);
        measurements.put("recoverySeconds", round(recoverySeconds));
        measurements.put("grossRecoveryEventsPerSecond", round(grossRecoveryRate));

        assertCondition("outage_restart_executed", restartVerified, "edge was abruptly restarted during the outage");
        assertCondition("outage_backlog_drained", edgePending() == 0, "pending outbox returned to zero");
        assertCondition(
                "outage_live_flow_during_replay",
                liveDuringRecovery >= 500,
                "new source events continued while the backlog replayed");
        assertCondition(
                "outage_every_identity_present",
                databaseContainsExactly(expectedIds),
                "PostgreSQL contains the exact simulator event-ID set");
        assertCondition("outage_no_sequence_gaps", sequenceViolationCount() == 0, "every machine sequence is contiguous");
        assertCondition(
                "outage_no_customer_visible_duplicates",
                naturalDuplicateCount() == 0,
                "database uniqueness has one row per natural source identity");
        assertCondition(
                "outage_offsets_preserve_machine_order",
                offsetOrderingViolationCount() == 0,
                "stored Kafka offsets increase with per-machine sequence");
        assertCondition(
                "outage_replay_throughput",
                grossRecoveryRate >= 300,
                "gross database recovery throughput reached at least 300 events/s");
        assertApi("IMM-0001");

    }

    private void startBaseStack() throws Exception {
        Map<String, String> safe = Map.of(
                "EDGE_FAULT_HALT_AFTER_PUBLISH_COUNT", "0",
                "CENTRAL_FAULT_HALT_AFTER_DATABASE_COMMIT_COUNT", "0",
                "CENTRAL_FAULT_RECEIPT_DELAY_MILLIS", "0");
        compose(safe, "up", "-d", "--build");
        compose(safe, "--profile", "simulation", "build", "machine-simulator");
        waitForHealthy(FACTORY, Duration.ofMinutes(3));
        waitForHealthy(UPLINK, Duration.ofMinutes(3));
        waitForHealthy(KAFKA, Duration.ofMinutes(4));
        waitForHealthy(POSTGRES, Duration.ofMinutes(3));
        waitForHealthy(CENTRAL, Duration.ofMinutes(4));
        waitForHealthy(EDGE, Duration.ofMinutes(4));
        assertCondition("base_stack_healthy", true, "all six base services reported healthy");
    }

    private void runSimulator(
            String runId,
            Path expected,
            long eventCount,
            int machineCount,
            int cycleIntervalSeconds)
            throws Exception {
        compose(
                Map.of(),
                "--profile",
                "simulation",
                "run",
                "--rm",
                "--no-deps",
                "-e",
                "SIMULATOR_RUN_ID=" + runId,
                "-e",
                "SIMULATOR_EVENT_COUNT=" + eventCount,
                "-e",
                "SIMULATOR_MACHINE_COUNT=" + machineCount,
                "-e",
                "SIMULATOR_CYCLE_INTERVAL_SECONDS=" + cycleIntervalSeconds,
                "-e",
                "SIMULATOR_EXPECTED_FILE=/evidence/" + expected.getFileName(),
                "machine-simulator");
        assertCondition(
                "simulator_file_" + runId,
                fileLineCount(expected) == eventCount,
                "simulator wrote the exact requested identity count");
    }

    private void startSimulator(String runId, Path expected, int machineCount, int cycleIntervalSeconds)
            throws Exception {
        Map<String, String> environment = new LinkedHashMap<>();
        environment.put("SIMULATOR_RUN_ID", runId);
        environment.put("SIMULATOR_EVENT_COUNT", "0");
        environment.put("SIMULATOR_MACHINE_COUNT", Integer.toString(machineCount));
        environment.put("SIMULATOR_CYCLE_INTERVAL_SECONDS", Integer.toString(cycleIntervalSeconds));
        environment.put("SIMULATOR_EXPECTED_FILE", "/evidence/" + expected.getFileName());
        compose(
                environment,
                "--profile",
                "simulation",
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "machine-simulator");
        waitForContainerRunning("outboxer-machine-simulator", Duration.ofMinutes(2));
    }

    private void assertApi(String machineId) throws Exception {
        HttpResponse<String> latest = httpGet(
                "http://127.0.0.1:18080/api/v1/machines/" + machineId + "/cycles/latest",
                Map.of("X-Tenant-Id", "tenant-017"));
        HttpResponse<String> recent = httpGet(
                "http://127.0.0.1:18080/api/v1/machines/" + machineId + "/cycles?limit=2",
                Map.of("X-Tenant-Id", "tenant-017"));
        HttpResponse<String> quality = httpGet(
                "http://127.0.0.1:18080/api/v1/machines/" + machineId + "/data-quality",
                Map.of("X-Tenant-Id", "tenant-017"));
        HttpResponse<String> missingTenant = httpGet(
                "http://127.0.0.1:18080/api/v1/machines/" + machineId + "/cycles/latest", Map.of());
        assertCondition(
                "api_latest_tenant_scoped",
                latest.statusCode() == 200 && latest.body().contains("\"machineId\":\"" + machineId + "\""),
                "latest cycle is queryable with tenant scope");
        assertCondition(
                "api_recent_tenant_scoped",
                recent.statusCode() == 200 && recent.body().startsWith("["),
                "recent cycles are queryable with tenant scope");
        assertCondition(
                "api_quality_tenant_scoped",
                quality.statusCode() == 200 && quality.body().contains("\"detectedGapCount\""),
                "data quality is queryable with tenant scope");
        assertCondition(
                "api_requires_tenant_header",
                missingTenant.statusCode() == 400,
                "missing local-demo tenant scope is rejected");
    }

    private void assertMetrics() throws Exception {
        String edgeMetrics = httpGet("http://127.0.0.1:18081/actuator/prometheus", Map.of()).body();
        String centralMetrics = httpGet("http://127.0.0.1:18080/actuator/prometheus", Map.of()).body();
        List<String> names = List.of(
                "edge_events_received_total",
                "edge_outbox_pending",
                "edge_publish_failures_total",
                "edge_replayed_events_total",
                "edge_receipt_timeouts_total",
                "cloud_events_received_total",
                "cloud_invalid_events_total",
                "cloud_duplicate_events_total",
                "cloud_ingestion_lag_seconds",
                "cloud_sequence_gaps_total");
        boolean present = names.stream().allMatch(name -> edgeMetrics.contains(name) || centralMetrics.contains(name));
        boolean noIdentityLabels = !(edgeMetrics + centralMetrics)
                .matches("(?s).*\\{[^}]*?(machineId|tenantId|siteId|eventId)=.*");
        assertCondition("metrics_required_names", present, "required edge and central metrics are exposed");
        assertCondition("metrics_bounded_cardinality", noIdentityLabels, "identity values are absent from metric labels");
    }

    private void assertOperationalSecurity() throws Exception {
        String edge = capture("docker", "inspect", "--format", "{{.Config.User}}|{{.HostConfig.ReadonlyRootfs}}", EDGE);
        String central = capture("docker", "inspect", "--format", "{{.Config.User}}|{{.HostConfig.ReadonlyRootfs}}", CENTRAL);
        assertCondition(
                "containers_non_root_read_only",
                edge.trim().equals("10001:10001|true") && central.trim().equals("10001:10001|true"),
                "application containers run as UID 10001 with read-only root filesystems");

        Set<String> edgeNetworks = containerNetworks(EDGE);
        Set<String> centralNetworks = containerNetworks(CENTRAL);
        Set<String> postgresNetworks = containerNetworks(POSTGRES);
        assertCondition(
                "networks_are_segmented",
                edgeNetworks.equals(Set.of("outboxer-factory-net", "outboxer-uplink-net"))
                        && centralNetworks.equals(Set.of("outboxer-uplink-net", "outboxer-data-net"))
                        && postgresNetworks.equals(Set.of("outboxer-data-net")),
                "network membership matches the factory/uplink/data trust boundaries");

        String publishedPorts = capture("docker", "ps", "--filter", "label=com.docker.compose.project=outboxer", "--format", "{{.Ports}}");
        assertCondition(
                "ports_are_loopback_only",
                !publishedPorts.contains("0.0.0.0:") && !publishedPorts.contains("[::]:"),
                "all diagnostic host ports bind only to loopback");
    }

    private Set<String> containerNetworks(String container) throws Exception {
        String output = capture(
                "docker",
                "inspect",
                "--format",
                "{{range $name, $network := .NetworkSettings.Networks}}{{$name}} {{end}}",
                container);
        return new HashSet<>(Arrays.asList(output.trim().split("\\s+")));
    }

    private void resetIfRequested(boolean required) throws Exception {
        if (required && !resetRequested()) {
            throw new IllegalStateException(
                    mode + " requires explicit OUTBOXER_RESET=true because it deletes only the five named Outboxer volumes");
        }
        if (resetRequested()) {
            destroyProject("explicit OUTBOXER_RESET=true");
        }
    }

    private void destroyProject(String reason) throws Exception {
        validateComposeScope();
        System.out.println("Destructive scope authorized by " + reason + ":");
        System.out.println("  project: outboxer");
        System.out.println("  containers: outboxer-* only");
        System.out.println(
                "  volumes: outboxer-factory-mqtt-data, outboxer-uplink-mqtt-data, outboxer-edge-data, outboxer-kafka-data, outboxer-postgres-data");
        compose(Map.of(), "down", "--volumes", "--remove-orphans");
    }

    private void validateComposeScope() throws IOException {
        String compose = Files.readString(COMPOSE_FILE);
        if (!compose.startsWith("name: outboxer\n") && !compose.startsWith("name: outboxer\r\n")) {
            throw new IllegalStateException("Compose project name is not fixed to outboxer");
        }
        for (String container : List.of(FACTORY, UPLINK, KAFKA, POSTGRES, CENTRAL, EDGE, "outboxer-machine-simulator")) {
            if (!compose.contains("container_name: " + container)) {
                throw new IllegalStateException("Expected scoped container name is absent: " + container);
            }
        }
        String existing = runCaptureAllowFailure(
                "docker", "ps", "-a", "--filter", "label=com.docker.compose.project=outboxer", "--format", "{{.Names}}");
        for (String name : existing.lines().filter(line -> !line.isBlank()).toList()) {
            if (!name.startsWith("outboxer-")) {
                throw new IllegalStateException("Refusing to operate on unexpected Compose resource " + name);
            }
        }
    }

    private void preflight() throws Exception {
        FileStore store = Files.getFileStore(ROOT);
        long freeBytes = store.getUsableSpace();
        assertCondition("preflight_free_disk", freeBytes >= 10L * 1024 * 1024 * 1024, "at least 10 GiB is available");
        measurements.put("preflightFreeGiB", round(freeBytes / 1024.0 / 1024 / 1024));
        String containers = capture("docker", "ps", "--format", "{{.Names}}|{{.Ports}}");
        for (int port : List.of(11883, 11884, 19092, 15432, 18080, 18081)) {
            boolean conflict = containers.lines()
                    .filter(line -> !line.startsWith("outboxer-"))
                    .anyMatch(line -> line.contains("127.0.0.1:" + port + "->") || line.contains("0.0.0.0:" + port + "->"));
            if (conflict) {
                throw new IllegalStateException("Host port " + port + " is occupied by a non-Outboxer container");
            }
        }
        assertCondition("preflight_ports", true, "dedicated local ports are not occupied by unrelated containers");
        measurements.put("dockerServerVersion", capture("docker", "version", "--format", "{{.Server.Version}}").trim());
        measurements.put("javaVersion", System.getProperty("java.version"));
        measurements.put("gitCommit", runCaptureAllowFailure("git", "rev-parse", "--short", "HEAD").trim());
    }

    private void ensureSecrets() throws Exception {
        if (Files.isRegularFile(ENV_FILE)) {
            return;
        }
        if (System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows")) {
            command(
                    Duration.ofMinutes(5),
                    Map.of(),
                    null,
                    "pwsh",
                    "-NoProfile",
                    "-File",
                    ROOT.resolve("scripts/generate-secrets.ps1").toString());
        } else {
            command(
                    Duration.ofMinutes(5),
                    Map.of(),
                    null,
                    "bash",
                    ROOT.resolve("scripts/generate-secrets.sh").toString());
        }
    }

    private void requireWorkspace() {
        if (!Files.isRegularFile(COMPOSE_FILE) || !Files.isRegularFile(ROOT.resolve("settings.gradle.kts"))) {
            throw new IllegalStateException("Run the coordinator from the Outboxer repository root");
        }
    }

    private void requireCommand(String... command) throws Exception {
        command(Duration.ofSeconds(30), Map.of(), null, command);
    }

    private void requireReset() {
        if (!resetRequested()) {
            throw new IllegalStateException("Set OUTBOXER_RESET=true to acknowledge the isolated test-data reset");
        }
    }

    private boolean resetRequested() {
        return booleanEnvironment("OUTBOXER_RESET");
    }

    private boolean cleanupRequested() {
        return booleanEnvironment("OUTBOXER_CLEANUP");
    }

    private static boolean booleanEnvironment(String name) {
        return "true".equalsIgnoreCase(System.getenv().getOrDefault(name, "false"));
    }

    private static int integerEnvironment(String name, int defaultValue, int minimum, int maximum) {
        int value = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(defaultValue)));
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(name + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    private Path freshExpectedFile(String name) throws IOException {
        Files.createDirectories(EVIDENCE_DIRECTORY);
        Path path = EVIDENCE_DIRECTORY.resolve(mode + "-" + name + "-expected.csv").normalize();
        if (!path.startsWith(EVIDENCE_DIRECTORY)) {
            throw new IllegalStateException("Evidence path escaped its directory");
        }
        Files.deleteIfExists(path);
        return path;
    }

    private static String runId(String prefix) {
        return prefix + '-' + UUID.randomUUID();
    }

    private long fileLineCount(Path path) {
        if (!Files.isRegularFile(path)) {
            return 0;
        }
        try (var lines = Files.lines(path)) {
            return lines.count();
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot count " + path, exception);
        }
    }

    private Set<String> expectedIds(Path path) throws IOException {
        Set<String> result = new HashSet<>();
        for (String line : Files.readAllLines(path)) {
            if (!line.isBlank()) {
                result.add(line.split(",", 2)[0]);
            }
        }
        return result;
    }

    private double sourcePublishSeconds(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);
        if (lines.size() < 2) {
            throw new IllegalStateException("At least two simulator observations are required to calculate pacing");
        }
        String[] first = lines.getFirst().split(",", -1);
        String[] last = lines.getLast().split(",", -1);
        if (first.length != 7 || last.length != 7) {
            throw new IllegalStateException("Simulator evidence does not contain source observation timestamps");
        }
        return secondsBetween(Instant.parse(first[6]), Instant.parse(last[6]));
    }

    private void waitForDatabaseIds(Set<String> ids, Duration timeout) throws Exception {
        if (mode.equals("smoke") || ids.size() <= 10) {
            waitUntil("PostgreSQL expected IDs", timeout, () -> ids.stream().allMatch(id -> postgresCount(id) == 1));
            return;
        }
        waitUntil(
                "PostgreSQL expected row count",
                timeout,
                () -> postgresScalar("SELECT COUNT(*) FROM cycle_event") >= ids.size());
    }

    private boolean databaseContainsExactly(Set<String> expected) throws Exception {
        Set<String> actual = new HashSet<>(postgresLines("SELECT event_id FROM cycle_event ORDER BY event_id"));
        return actual.equals(expected);
    }

    private long postgresCount(String eventId) {
        return postgresScalar("SELECT COUNT(*) FROM cycle_event WHERE event_id = '" + sqlIdentifier(eventId) + "'");
    }

    private long duplicateCount(String machineId) {
        return postgresScalar(
                "SELECT COALESCE(MAX(duplicate_event_count), 0) FROM machine_data_quality WHERE tenant_id = 'tenant-017' AND machine_id = '"
                        + sqlIdentifier(machineId)
                        + "'");
    }

    private long naturalDuplicateCount() {
        return postgresScalar(
                "SELECT COUNT(*) FROM (SELECT tenant_id, machine_id, machine_boot_id, sequence_number FROM cycle_event GROUP BY 1,2,3,4 HAVING COUNT(*) > 1) duplicates");
    }

    private long sequenceViolationCount() {
        return postgresScalar(
                "SELECT COUNT(*) FROM (SELECT tenant_id, machine_id, machine_boot_id FROM cycle_event GROUP BY 1,2,3 HAVING MIN(sequence_number) <> 0 OR MAX(sequence_number) + 1 <> COUNT(*)) violations");
    }

    private long offsetOrderingViolationCount() {
        return postgresScalar(
                "WITH ordered AS (SELECT tenant_id, machine_id, machine_boot_id, sequence_number, kafka_partition, kafka_offset, LAG(kafka_partition) OVER (PARTITION BY tenant_id, machine_id, machine_boot_id ORDER BY sequence_number) previous_partition, LAG(kafka_offset) OVER (PARTITION BY tenant_id, machine_id, machine_boot_id ORDER BY sequence_number) previous_offset FROM cycle_event) SELECT COUNT(*) FROM ordered WHERE previous_offset IS NOT NULL AND (kafka_partition <> previous_partition OR kafka_offset <= previous_offset)");
    }

    private long postgresScalar(String sql) {
        try {
            String value = capture(
                            "docker",
                            "exec",
                            POSTGRES,
                            "psql",
                            "-X",
                            "-U",
                            "outboxer_admin",
                            "-d",
                            "outboxer",
                            "-At",
                            "-v",
                            "ON_ERROR_STOP=1",
                            "-c",
                            sql)
                    .trim();
            return value.isBlank() ? 0 : Long.parseLong(value);
        } catch (Exception exception) {
            throw new IllegalStateException("PostgreSQL scalar query failed", exception);
        }
    }

    private List<String> postgresLines(String sql) throws Exception {
        String output = capture(
                "docker",
                "exec",
                POSTGRES,
                "psql",
                "-X",
                "-U",
                "outboxer_admin",
                "-d",
                "outboxer",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql);
        return output.lines().map(String::trim).filter(line -> !line.isBlank()).toList();
    }

    private long edgePending() {
        return edgeScalar("SELECT COUNT(*) FROM outbox_event WHERE state = 'PENDING'");
    }

    private List<String> edgePendingIds() {
        try {
            String output = capture(
                    "docker",
                    "exec",
                    EDGE,
                    "sqlite3",
                    "-noheader",
                    "/var/lib/outboxer/edge.db",
                    "SELECT event_id FROM outbox_event WHERE state = 'PENDING' ORDER BY event_id;");
            return output.lines().map(String::trim).filter(line -> !line.isBlank()).toList();
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot read edge pending IDs", exception);
        }
    }

    private long edgeScalar(String sql) {
        try {
            String value = capture(
                            "docker",
                            "exec",
                            EDGE,
                            "sqlite3",
                            "-noheader",
                            "/var/lib/outboxer/edge.db",
                            sql + ';')
                    .trim();
            return value.isBlank() ? 0 : Long.parseLong(value);
        } catch (Exception exception) {
            throw new IllegalStateException("SQLite scalar query failed", exception);
        }
    }

    private long invalidTopicEndOffset() {
        try {
            String output = capture(
                    "docker",
                    "exec",
                    KAFKA,
                    "/opt/kafka/bin/kafka-get-offsets.sh",
                    "--bootstrap-server",
                    "127.0.0.1:9092",
                    "--topic",
                    "cycle-events-invalid");
            return output.lines()
                    .filter(line -> line.contains(":"))
                    .mapToLong(line -> Long.parseLong(line.substring(line.lastIndexOf(':') + 1).trim()))
                    .sum();
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot read invalid-topic offsets", exception);
        }
    }

    private void publishFactory(String topic, byte[] payload) throws Exception {
        command(
                Duration.ofSeconds(30),
                Map.of(),
                payload,
                "docker",
                "exec",
                "-i",
                FACTORY,
                "sh",
                "-ec",
                "mosquitto_pub -h 127.0.0.1 -p 1883 -u simulator -P \"$FACTORY_SIMULATOR_PASSWORD\" -q 1 -t \"$1\" -s",
                "publish",
                topic);
    }

    private void publishUplink(String topic, byte[] payload) throws Exception {
        command(
                Duration.ofSeconds(30),
                Map.of(),
                payload,
                "docker",
                "exec",
                "-i",
                UPLINK,
                "sh",
                "-ec",
                "mosquitto_pub -h 127.0.0.1 -p 1883 -u edge-uplink -P \"$UPLINK_EDGE_PASSWORD\" -q 1 -t \"$1\" -s",
                "publish",
                topic);
    }

    private void waitForHealthy(String container, Duration timeout) throws Exception {
        waitUntil(container + " healthy", timeout, () -> {
            try {
                return capture("docker", "inspect", "--format", "{{.State.Health.Status}}", container)
                        .trim()
                        .equals("healthy");
            } catch (Exception exception) {
                return false;
            }
        });
    }

    private void waitForContainerRunning(String container, Duration timeout) throws Exception {
        waitUntil(container + " running", timeout, () -> {
            try {
                return capture("docker", "inspect", "--format", "{{.State.Running}}", container)
                        .trim()
                        .equals("true");
            } catch (Exception exception) {
                return false;
            }
        });
    }

    private void waitForLog(String container, String marker, Duration timeout) throws Exception {
        waitUntil(container + " log marker " + marker, timeout, () -> {
            try {
                return capture("docker", "logs", container).contains(marker);
            } catch (Exception exception) {
                return false;
            }
        });
    }

    private void waitForHttp(String url, int wantedStatus, Duration timeout) throws Exception {
        waitUntil(url + " HTTP " + wantedStatus, timeout, () -> httpStatus(url) == wantedStatus);
    }

    private int httpStatus(String url) {
        try {
            return httpGet(url, Map.of()).statusCode();
        } catch (Exception exception) {
            return -1;
        }
    }

    private HttpResponse<String> httpGet(String url, Map<String, String> headers) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET();
        headers.forEach(builder::header);
        return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private void waitUntil(String description, Duration timeout, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        AtomicReference<RuntimeException> lastError = new AtomicReference<>();
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    System.out.println("PASS: " + description);
                    return;
                }
            } catch (RuntimeException exception) {
                lastError.set(exception);
            }
            Thread.sleep(1_000);
        }
        String suffix = lastError.get() == null ? "" : ": " + lastError.get().getMessage();
        throw new IllegalStateException("Timed out waiting for " + description + suffix);
    }

    private void assertCondition(String name, boolean passed, String detail) {
        assertions.put(name, passed);
        System.out.println((passed ? "PASS" : "FAIL") + ": " + name + " - " + detail);
        if (!passed) {
            throw new IllegalStateException(name + " failed: " + detail);
        }
    }

    private void compose(Map<String, String> environment, String... arguments) throws Exception {
        List<String> command = composeCommand(arguments);
        command(COMMAND_TIMEOUT, environment, null, command.toArray(String[]::new));
    }

    private List<String> composeCommand(String... arguments) {
        List<String> command = new ArrayList<>(List.of(
                "docker",
                "compose",
                "--project-name",
                PROJECT,
                "--file",
                COMPOSE_FILE.toString(),
                "--env-file",
                ENV_FILE.toString()));
        command.addAll(List.of(arguments));
        return command;
    }

    private String capture(String... command) throws Exception {
        return command(Duration.ofMinutes(2), Map.of(), null, command);
    }

    private String runCaptureAllowFailure(String... command) {
        try {
            return command(Duration.ofSeconds(30), Map.of(), null, command);
        } catch (Exception ignored) {
            return "";
        }
    }

    private String command(Duration timeout, Map<String, String> environment, byte[] standardInput, String... command)
            throws Exception {
        boolean quiet = isQuietCommand(command);
        if (!quiet) {
            System.out.println("$ " + String.join(" ", command));
        }
        ProcessBuilder builder = new ProcessBuilder(command)
                .directory(ROOT.toFile())
                .redirectErrorStream(true);
        builder.environment().putAll(environment);
        Process process = builder.start();
        if (standardInput != null) {
            process.getOutputStream().write(standardInput);
        }
        process.getOutputStream().close();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AtomicReference<IOException> readFailure = new AtomicReference<>();
        Thread reader = Thread.ofVirtual().start(() -> {
            try {
                process.getInputStream().transferTo(output);
            } catch (IOException exception) {
                readFailure.set(exception);
            }
        });
        if (!process.waitFor(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Command timed out after " + timeout + ": " + command[0]);
        }
        reader.join();
        String text = output.toString(StandardCharsets.UTF_8);
        if (!text.isBlank() && !quiet) {
            System.out.print(text);
            if (!text.endsWith("\n")) {
                System.out.println();
            }
        }
        if (readFailure.get() != null) {
            throw readFailure.get();
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException(
                    "Command failed with exit " + process.exitValue() + ": " + String.join(" ", command) + "\n" + text);
        }
        return text;
    }

    private static boolean isQuietCommand(String[] command) {
        return Arrays.asList(command).contains("inspect")
                || Arrays.asList(command).contains("psql")
                || Arrays.asList(command).contains("sqlite3")
                || Arrays.asList(command).contains("logs")
                || Arrays.asList(command).contains("--format");
    }

    private static String sqlIdentifier(String value) {
        if (!value.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Unsafe SQL test identifier");
        }
        return value;
    }

    private void writeReport() throws IOException {
        Files.createDirectories(EVIDENCE_DIRECTORY.resolve("generated"));
        Instant finishedAt = Instant.now();
        boolean passed = failure == null && assertions.values().stream().allMatch(Boolean::booleanValue);
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(finishedAt);
        Path json = EVIDENCE_DIRECTORY.resolve("generated/" + mode + '-' + timestamp + ".json");
        Path markdown = EVIDENCE_DIRECTORY.resolve("generated/" + mode + '-' + timestamp + ".md");

        StringBuilder jsonBody = new StringBuilder("{\n");
        appendJson(jsonBody, "mode", mode, true);
        appendJson(jsonBody, "startedAt", startedAt.toString(), true);
        appendJson(jsonBody, "finishedAt", finishedAt.toString(), true);
        appendJson(jsonBody, "passed", passed, true);
        appendJson(jsonBody, "failure", failure == null ? null : failure.getMessage(), true);
        jsonBody.append("  \"assertions\": ").append(jsonObject(assertions)).append(",\n");
        jsonBody.append("  \"measurements\": ").append(jsonObject(measurements)).append("\n}\n");
        Files.writeString(json, jsonBody, StandardCharsets.UTF_8);

        StringBuilder markdownBody = new StringBuilder();
        markdownBody.append("# ").append(capitalize(mode)).append(" acceptance result\n\n");
        markdownBody.append("- Result: **").append(passed ? "PASS" : "FAIL").append("**\n");
        markdownBody.append("- Started: `").append(startedAt).append("`\n");
        markdownBody.append("- Finished: `").append(finishedAt).append("`\n");
        if (failure != null) {
            markdownBody.append("- Failure: `").append(markdownEscape(failure.getMessage())).append("`\n");
        }
        markdownBody.append("\n## Assertions\n\n| Assertion | Result |\n| --- | --- |\n");
        assertions.forEach((name, result) -> markdownBody
                .append("| `")
                .append(name)
                .append("` | ")
                .append(result ? "PASS" : "FAIL")
                .append(" |\n"));
        markdownBody.append("\n## Measurements\n\n| Measurement | Value |\n| --- | ---: |\n");
        measurements.forEach((name, value) -> markdownBody
                .append("| `")
                .append(name)
                .append("` | ")
                .append(markdownEscape(String.valueOf(value)))
                .append(" |\n"));
        Files.writeString(markdown, markdownBody, StandardCharsets.UTF_8);
        System.out.println("Acceptance evidence: " + markdown);
    }

    private static void appendJson(StringBuilder body, String key, Object value, boolean comma) {
        body.append("  \"").append(jsonEscape(key)).append("\": ").append(jsonValue(value));
        body.append(comma ? ",\n" : "\n");
    }

    private static String jsonObject(Map<String, ?> values) {
        StringBuilder body = new StringBuilder("{\n");
        int index = 0;
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            body.append("    \"")
                    .append(jsonEscape(entry.getKey()))
                    .append("\": ")
                    .append(jsonValue(entry.getValue()));
            if (++index < values.size()) {
                body.append(',');
            }
            body.append('\n');
        }
        return body.append("  }").toString();
    }

    private static String jsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return '"' + jsonEscape(value.toString()) + '"';
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String markdownEscape(String value) {
        return value == null ? "" : value.replace("|", "\\|").replace("\n", " ");
    }

    private static String capitalize(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private static double secondsBetween(Instant start, Instant end) {
        return Math.max(0.001, Duration.between(start, end).toNanos() / 1_000_000_000.0);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
