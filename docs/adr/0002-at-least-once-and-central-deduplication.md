# ADR 0002: Use at-least-once delivery with central deduplication

- Status: Accepted
- Date: 2026-09-04

## Context

The edge, two MQTT brokers, Kafka, and PostgreSQL cannot participate in one atomic transaction. Any attempt to remove all duplicates at the transport layer would still leave a crash window between durable systems. Preferring duplicate suppression over replay would risk loss.

## Decision

Accept at-least-once delivery at every asynchronous boundary and make duplicate effects safe. Each source supplies a stable UUID and a natural machine-position identity. The edge and central stores enforce both. Canonical bytes are hashed with SHA-256 so an identical retry can be separated from an identity collision carrying altered content.

Configure the Kafka producer for all acknowledgements and idempotence. In PostgreSQL, insert event rows and update data-quality state within one transaction. Treat matching identities and hashes as duplicates; treat a mismatch as an isolated conflict. Let Kafka advance a consumer offset only after the listener returns from the committed database transaction.

## Consequences

- Retries can occur after ambiguous failures without creating a second customer-visible event row.
- Duplicate metrics and machine quality state show that redelivery occurred.
- An identity conflict is visible and cannot silently overwrite accepted content.
- Network-level duplicates still consume processing resources.
- Stable producer identity generation is part of the contract.

## Alternatives considered

- Claiming exactly-once end to end was rejected because no shared transaction spans MQTT, Kafka, and PostgreSQL.
- Dropping repeat UUIDs without comparing content was rejected because it hides identity reuse with different data.
- Distributed transactions were rejected due to protocol support, complexity, and mismatch with the local edge boundary.
