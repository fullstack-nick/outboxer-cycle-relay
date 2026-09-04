# ADR 0001: Use a disk-backed edge outbox

- Status: Accepted
- Date: 2026-09-04

## Context

The edge must keep accepting cycle events while the uplink is unavailable and must recover after its own process or container restarts. An in-memory queue cannot own acknowledged data across either failure. Relying only on the factory broker would also leave the application without explicit replay state, attempt history, or application-receipt settlement.

The demonstrated source rate is approximately 100 events/second, including a 10-minute outage, on one local host. A small embedded store is sufficient and avoids adding another edge service.

## Decision

Persist every valid source event in SQLite before manually acknowledging its MQTT delivery. Use WAL mode, `synchronous=FULL`, foreign keys, and a busy timeout, and verify those active settings at startup. Store immutable canonical JSON plus identity, hash, state, retry timing, and error data.

Use `PENDING`, `SENT`, and `REJECTED` states. Batch source and receipt writes through single writer threads so SQLite transactions remain serialized. Keep pending and rejected rows until an explicit outcome; clean old sent rows after a configurable retention period.

## Consequences

- An acknowledged source event survives application and container restart while the volume remains intact.
- The database provides an inspectable backlog and deterministic retry source.
- Batched transactions sustain the target source rate without weakening acknowledgement order.
- SQLite is a single-host boundary; it does not provide remote replication or host-failure recovery.
- Operators must monitor disk space, pending count, and oldest pending age.

## Alternatives considered

- An in-memory queue was rejected because process failure loses acknowledged work.
- Broker-only buffering was rejected because it cannot represent the central application receipt or local retry state.
- A separate edge database server was rejected as unnecessary operational weight for the local single-process boundary.
