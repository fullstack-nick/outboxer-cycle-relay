# ADR 0006: Keep database retry inside the ordered Kafka listener boundary

- Status: Accepted
- Date: 2026-09-04

## Context

A database outage can occur after Kafka has accepted events. Skipping a failed valid record to a dead-letter topic would violate the durable-delivery promise, while acknowledging a batch before its database transaction commits would create loss. Processing every record with a separate transaction would also limit recovery throughput.

## Decision

Use a Spring Kafka batch listener with at most 250 records per poll and three concurrent consumers across 12 partitions. Revalidate records before persistence. Send poison data to the invalid topic explicitly, but allow valid database failures to escape the listener. Configure unlimited fixed-backoff retries so the partition remains at the uncommitted position.

Persist all valid records from the poll with set-based PostgreSQL operations in one transaction. Record Kafka partition and offset under a unique index. Only a successful listener return permits the batch offset to advance.

## Consequences

- A PostgreSQL outage pauses affected partitions without dropping valid data.
- Batch writes provide substantially more recovery capacity than the input rate in the recorded test.
- One persistently failing valid record blocks later records on its partition until the dependency or code issue is corrected.
- Poison validation errors must be identified before transactional persistence and isolated explicitly.
- An ambiguous post-commit crash causes replay, which the idempotency design handles.

## Alternatives considered

- A finite retry followed by dead-letter routing was rejected for valid records because it converts dependency downtime into manual data recovery.
- Per-record acknowledgement was rejected because it complicates ordering and reduces batch efficiency.
- Database writes in the MQTT callback were rejected because Kafka is the intended central durable boundary and recovery buffer.
