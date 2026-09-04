# Mock pull request: make cycle delivery durable across uplink outages

## Business reason

A transient uplink failure must not force a synthetic factory source to choose between stopping event production and silently losing completed-cycle data. This work introduces a local reliability boundary that accepts events durably at the edge, replays them after connectivity returns, and provides a duplicate-safe query model for support and data-quality inspection.

## Components affected

- Adds strict source, canonical, and receipt contracts for `CYCLE_COMPLETED` v1 and v2.
- Adds a deterministic Kotlin machine simulator.
- Adds a Kotlin/Spring edge relay with SQLite WAL outbox, quarantine, ordered dispatch, bounded retry, and application-receipt settlement.
- Adds a Java/Spring central service with MQTT intake, keyed Kafka publication, idempotent PostgreSQL persistence, data-quality state, and three WebFlux endpoints.
- Adds two Mosquitto brokers, Kafka, PostgreSQL, segmented Compose networks, generated local credentials, and hardened application containers.
- Adds minimal Kubernetes application manifests and an isolated local kind proof.
- Adds unit, integration, smoke, load, crash-window, outage, compatibility, and publication checks.

## Backward compatibility

V1 wire payloads are unchanged. V2 adds optional, non-negative `energyConsumptionWh`; v1 rejects that field, while v2 accepts its presence or absence. PostgreSQL adds a nullable column and the existing `/api/v1` response keeps one stable shape: v1 returns `null`, v2 returns the supplied value. Mixed v1/v2 traffic passed both Compose and kind smoke tests.

## Failure modes considered

| Failure | Expected behavior |
| --- | --- |
| Uplink unavailable | edge commits new source events to SQLite, stays live, reports unready, retries later |
| Edge exits after uplink publish | pending row is replayed; central deduplication prevents a second query row |
| Central exits after database commit | Kafka redelivers; constraints and hash comparison classify a duplicate |
| PostgreSQL unavailable | consumer holds the uncommitted partition position and retries |
| Invalid source or canonical payload | message is isolated with reason and bounded evidence; valid flow continues |
| Identity reused with altered content | event is classified as conflict, not accepted as a duplicate |
| Application receipt lost | edge timeout causes safe replay |
| Explicit local volume deletion | data is lost; this is outside the stated persistence boundary |

## Rollout plan

1. Run `clean check integrationTest contractCompatibilityTest` with locked dependencies.
2. Run strict offline manifest validation.
3. Start the Compose stack with generated credentials and run the v1/v2 smoke test.
4. Run the 60-second 100 events/second load test and repeated crash-window test.
5. Run the formal 600-second uplink outage test from fresh disposable project volumes.
6. Run the isolated kind smoke with locally loaded images.
7. Run `publicationCheck` from a clean tree and review the dependency inventory and third-party notices.

This is a local project release; there is no cloud rollout or CI/CD pipeline.

## Rollback plan

Stop only project `outboxer` with `scripts/stop-local.*`. Source changes can be reverted with a normal Git revert. The v2 database migration is additive and nullable, so v1 readers remain compatible; avoid manually downgrading a populated database. If a fully fresh demo is required, use an acceptance wrapper's explicit reset option after confirming the exact five Outboxer volume names. No unrelated Docker resource is part of rollback.

## Test evidence

- Formal outage: 60,072 events produced during 601.54 seconds; 60,154 peak pending; readiness restored in 13.02 seconds; 84,914 expected identities matched 84,914 database rows; 367.37 gross recovered events/second.
- Load: 6,000 exact identities at approximately 100 events/second.
- Crash injection: three edge and three central repetitions, all ending with one customer-visible row per event despite six observed duplicate deliveries.
- V2 Compose smoke: v1 null and v2 numeric energy values verified through storage and API; invalid paths, persistent broker session, application receipt, and metric gates passed.
- kind smoke: 100 event identities, a 50/50 v1/v2 split, zero pending edge rows, strict validation, and temporary-cluster cleanup passed.

Reviewed reports are indexed from the [README](../README.md#test-the-reliability-claims).

## Reviewer focus

- Validate the exact acknowledgement points in `EdgeMqttBridge` and `CentralMqttGateway`.
- Check that `OutboxRepository.findEligible` cannot allow per-machine overtaking.
- Check the PostgreSQL conflict classification and transaction boundary.
- Confirm that no public claim says exactly-once transport or production readiness.
- Confirm reset and kind operations remain exact, local, and opt-in.
