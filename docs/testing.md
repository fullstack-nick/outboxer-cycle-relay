# Testing strategy

The test suite separates fast logic checks, real dependency integration, and destructive local acceptance runs. Each layer proves a different part of the reliability claim.

## Verification layers

| Layer | Command | What it proves |
| --- | --- | --- |
| Formatting and unit tests | `./gradlew check` | schemas, validators, retry policy, repositories, metrics, API behavior, simulator configuration |
| PostgreSQL integration | `./gradlew integrationTest` | Flyway migrations, idempotent transactions, duplicates, conflicts, gaps, v1/v2 persistence using Testcontainers |
| Contract compatibility | `./gradlew contractCompatibilityTest` | strict v1 shape, v2 optional energy, invalid boundaries, future-time tolerance |
| Manifest validation | `./gradlew kubernetesManifestTest` | kustomize output, strict cached schemas with networking disabled |
| Compose smoke | `./gradlew composeSmokeTest` | complete path, ACLs, network isolation, invalid paths, receipts, restart persistence, metrics |
| Load | `./gradlew loadTest` | exact identity set at approximately 100 events/second |
| Crash windows | `./gradlew crashWindowTest` | edge and central forced-halt replay safety, repeated three times |
| Outage | `./gradlew outageTest` | 10-minute accumulation, edge restart, live replay, full identity reconciliation, APIs |
| kind smoke | `./gradlew kindSmokeTest` | actual isolated Kubernetes runtime with locally loaded images and mixed contracts |
| Publication | `./gradlew publicationCheck` | tests, manifest validation, history/content safety, required artifacts, links, clean tree |

On Windows, use `gradlew.bat` or the PowerShell wrappers under `scripts/`.

## Unit and integration focus

The contract module tests valid examples and rejects unknown fields, invalid identifiers, negative measurements, malformed UUIDs, oversized payloads, unsupported schema versions, topic/payload mismatches, and timestamps beyond the configured tolerance.

Edge repository tests exercise durable insert, identical replay, identity conflict, per-machine head-of-line selection, retry state, receipt settlement, and quarantine. Central integration tests use a real PostgreSQL container to verify unique keys and data-quality updates rather than relying on an in-memory substitute.

API tests cover all three routes, tenant-header requirements, bounds on `limit`, missing rows, cache policy, and v1/v2 energy shape.

## Acceptance harness

`scripts/AcceptanceCoordinator.java` is a source-file Java program invoked by Gradle. It performs preflight checks, scopes Compose commands to project `outboxer`, starts only the required profile, injects deterministic input, queries internal durable stores, and writes Markdown plus JSON raw evidence under ignored `.outboxer/evidence/generated/`.

Every full-flow assertion compares identities, not only counts. A count can hide one lost event and one duplicated event; set equality cannot. Ordering checks query stored Kafka partitions and offsets per machine. Tests also assert liveness/readiness behavior, header scoping, rejection isolation, and bounded-cardinality metric names.

The simulator derives UUIDs from `(runId, machineId, sequenceNumber)` and writes expected identities before each publish completes. A fixed run ID therefore makes input reproducible.

## Reset safety

Acceptance state is intentionally disposable, but removal requires an explicit `-Reset` or `--reset`. The harness resolves and verifies the exact project volume names before deleting only:

```text
outboxer-factory-mqtt-data
outboxer-uplink-mqtt-data
outboxer-edge-data
outboxer-kafka-data
outboxer-postgres-data
```

Without reset, existing volumes are preserved. `-Cleanup` / `--cleanup` stops only the Outboxer Compose project at the end and also preserves volumes. Unrelated containers and networks are outside the script scope.

## Formal outage gates

The 600-second run passes only when all of these hold:

- at least the configured free disk is available and required loopback ports are free;
- the base stack becomes healthy;
- source rate remains near 100 events/second;
- pending SQLite rows match events produced during the outage;
- edge liveness stays up while readiness degrades;
- restarting the edge does not reduce pending custody;
- after uplink restoration the backlog drains while new events continue;
- every expected event identity exists exactly once in PostgreSQL;
- per-machine sequences have no unexplained gap and Kafka offsets preserve order;
- recovery throughput exceeds incoming throughput;
- all three tenant-scoped APIs return correct data and reject a missing tenant header.

## kind isolation

The kind scripts never use the default kubeconfig. They create a temporary kubeconfig, address only `kind-outboxer-cycle-relay`, and delete that exact cluster in cleanup. The node image and tool versions are pinned and downloaded with published SHA-256 verification. Application images are built locally and loaded directly; no registry is required.

The manifest validator first populates a local schema cache, then runs again with all proxy routes directed to a closed local address. This demonstrates that the decisive strict validation is offline. The isolated API server also performs a server-side dry run.

## Evidence policy

Raw runs are ignored because they contain machine-specific details and large identity lists. Reviewed summaries under `docs/evidence/` record the relevant commit, configuration, assertions, measurements, and limitations without credentials. A failed run is never represented as a pass; reruns create new timestamped files.

Before publication, run from a clean checkout:

```bash
./scripts/verify.sh
```

Dependency locks and Gradle verification metadata make JVM resolution reproducible. `licenseReport` writes the exact resolved component inventory to `build/reports/licenses/resolved-dependencies.txt` for local review.
