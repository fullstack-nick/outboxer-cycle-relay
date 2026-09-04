# ADR 0003: Preserve order per machine

- Status: Accepted
- Date: 2026-09-04

## Context

Cycle order matters within one machine, but a global ordering lock would serialize thousands of unrelated machines and reduce recovery throughput. Retry makes overtaking possible unless the edge selection rule and downstream partition key express the same scope.

## Decision

Define ordering per machine boot and carry `machineBootId` plus monotonic `sequenceNumber`. At the edge, select a pending row only when no earlier pending row exists for the same tenant, site, and machine. Route all work for a machine through one of 64 serial dispatch executors.

Publish Kafka records with `machineId` as the key into 12 partitions. Use ordered batch consumption and commit the PostgreSQL transaction before the consumer offset advances. Track the active boot, last sequence, and cumulative detected gaps in `machine_data_quality`.

## Consequences

- A retrying event blocks later events for that scoped machine but not other machines.
- Different machines recover concurrently.
- Kafka partition order and recorded offsets provide audit evidence.
- Machines sharing an identifier across tenants share a Kafka partition, which is safe but may reduce distribution in that unusual case.
- Late data is stored and classified; cumulative gap history is not rewritten.

## Alternatives considered

- Global ordering was rejected because it makes one slow event block the entire fleet.
- Unkeyed round-robin Kafka publication was rejected because one machine could span partitions.
- Allowing later edge rows to pass a retrying row was rejected because it breaks the stated per-machine guarantee.
