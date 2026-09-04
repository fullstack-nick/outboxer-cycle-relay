# ADR 0005: Complete edge delivery with an application receipt

- Status: Accepted
- Date: 2026-09-04

## Context

An MQTT QoS 1 acknowledgement proves only that the next broker or client accepted a publish according to MQTT. It does not prove that the central service validated the event or placed it in its durable Kafka log. Marking an edge row sent at transport acknowledgement creates an event-loss window if central processing fails immediately afterward.

## Decision

Use MQTT 5 response topics and correlation data. The edge publishes the canonical event with `receipts/{siteId}/cycles` and the event UUID as correlation. Central validates payload, topic, response topic, and correlation; waits for Kafka acknowledgement; then publishes an `ACCEPTED` receipt carrying the same event ID and correlation.

The edge validates and durably settles that receipt before acknowledging receipt delivery. A central validation failure is written to the invalid Kafka topic and, where metadata is usable, returns a `REJECTED` receipt. Missing receipts time out and make the pending row eligible for retry.

## Consequences

- Edge state reflects central durable-log custody rather than broker transport alone.
- A lost receipt produces a duplicate retry instead of event loss.
- Response metadata becomes part of the validation and ACL surface.
- The accepted receipt does not promise immediate PostgreSQL query visibility.
- Receipt timeout and rejection paths require dedicated metrics and tests.

## Alternatives considered

- Settling on the uplink publish acknowledgement was rejected because it leaves central application processing outside the guarantee.
- Waiting for final PostgreSQL persistence was rejected because it couples edge backlog release to query-model lag and makes Kafka custody less useful.
- A separate HTTP callback was rejected because it adds another protocol and reachability path without improving the local custody model.
