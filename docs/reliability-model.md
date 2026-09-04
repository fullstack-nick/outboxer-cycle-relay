# Reliability model

Outboxer is designed around custody rather than successful method calls. At every step, the system asks which durable component can reproduce an event after the next process is killed.

## Delivery semantics

The end-to-end contract is **at-least-once delivery with an effectively-once PostgreSQL effect**. Retries and duplicates are expected. They are made safe through stable identities, immutable canonical bytes, content hashes, and database uniqueness constraints.

“Exactly once” is intentionally not used: MQTT, Kafka, application processes, and PostgreSQL do not share one atomic transaction. A process can always fail after one side commits and before the other side observes completion.

## Core invariants

1. A valid factory delivery is never acknowledged before its SQLite transaction commits.
2. A pending edge row is never marked sent from an MQTT transport acknowledgement.
3. An accepted application receipt is never published before Kafka acknowledges the canonical record.
4. A Kafka listener batch never returns successfully before its PostgreSQL transaction commits.
5. Reprocessing an identical identity produces at most one `cycle_event` row.
6. Reusing an identity with different canonical content is a conflict, not a harmless duplicate.
7. A later pending row for one scoped machine cannot pass an earlier pending row at the edge.

These invariants are asserted at repository boundaries and exercised by crash injection.

## Custody ledger

| Point in time | Durable owner | Recovery action |
| --- | --- | --- |
| Before factory delivery | factory broker session | redeliver QoS 1 message |
| After SQLite commit | edge outbox | retry canonical publish from stored bytes |
| After Kafka acknowledgement | Kafka | consumer resumes from committed group offset |
| After PostgreSQL commit | PostgreSQL | duplicate replay is classified and ignored as a new customer row |

An event may exist in two adjacent stores during handoff. That overlap is deliberate: loss prevention requires a period of duplicate custody, and idempotency makes it safe.

## The two tested crash windows

### Edge: publish succeeded, receipt not settled

The edge publishes an event, receives the broker acknowledgement, and is forcibly halted before a receipt settles the outbox row. After restart, the row is still `PENDING` and is published again. Kafka and PostgreSQL deduplication prevent a second customer-visible row.

### Central: database committed, offset not advanced

The consumer commits PostgreSQL and is forcibly halted before the listener can return. Kafka redelivers from the previous committed offset. The event ID and natural identity match the stored hash, so the second attempt is a duplicate and no extra row appears.

Both windows run three times in the reviewed [crash evidence](evidence/crash-windows.md).

## Identity and conflict rules

`eventId` is the primary idempotency key. A second natural key—`tenantId`, `machineId`, `machineBootId`, and `sequenceNumber` centrally, with `siteId` also present at the edge—guards against a sender minting a different event ID for the same machine position.

Canonical JSON is hashed with SHA-256. The outcomes are:

| Existing identity | Content hash | Outcome |
| --- | --- | --- |
| no | any valid content | insert |
| yes | same | duplicate; no new event row |
| yes | different | identity conflict; isolate for inspection |

Database uniqueness is the final arbiter under concurrency. Classification occurs inside the transactional persistence path.

## Ordering and gaps

The system preserves processing order for one machine but does not invent missing data. Sequence numbers and boot identities let it detect missing positions. `machine_data_quality` exposes the last sequence, cumulative detected gaps, duplicate count, and last central ingestion time.

If sequence 12 arrives after sequence 10, the system stores 12 and records one gap. A later arrival of 11 does not rewrite history or reduce the cumulative gap count. This makes the quality signal monotonic and auditable.

## Retry and backpressure

- MQTT clients use persistent sessions and QoS 1.
- Edge delivery retry is exponential with a one-second base and 60-second cap.
- Application receipt waits default to 30 seconds; expiry makes the row eligible again.
- Edge source, receipt, dispatch, and central ingress queues are bounded.
- Kafka database failures retry indefinitely with one-second pauses, preserving the partition position.
- Invalid data follows explicit quarantine paths and does not enter an endless retry loop.

Readiness degradation is a signal to operators; it does not discard work. Disk free-space checks and pending-age metrics make capacity exhaustion visible before writes fail.

## Measured recovery envelope

The formal v1 test used 3,000 machines on 30-second cycles, approximately 100 source events/second. During a measured 601.54-second uplink outage it created 60,072 source events and observed 60,154 pending rows at peak. After restoration, readiness returned in 13.02 seconds and the system recovered 84,340 rows in 229.58 seconds while 23,004 new events continued to arrive. Gross recovery throughput was 367.37 events/second.

Those numbers describe one recorded local machine, container versions, and configuration; they are evidence, not universal capacity promises. The identity-set assertion and final row count are more important than the rate.

## Persistence limits

Named Docker volumes protect against process and container restart only. The demo has one broker instance per side, one Kafka node, and one PostgreSQL instance. There is no replication, remote backup, host-failure tolerance, or media-corruption recovery.

SQLite sent rows are retained for one day by default and then removed. Pending and rejected rows are not silently expired. Mosquitto persistent sessions expire after 14 days. These retention values are operational policy, not delivery guarantees beyond the stated local boundary.

## Schema evolution

V1 payload bytes remain valid and do not gain a new field. V2 permits an optional, non-negative `energyConsumptionWh`. The central table stores it in a nullable column, and the stable v1 HTTP API returns either `null` or a number. Mixed-version traffic is validated end to end in both Compose and kind.
