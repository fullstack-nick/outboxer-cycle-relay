# Outboxer

**Reliable industrial cycle-event relay**

Outboxer is an independent, local-first demonstration of how synthetic production-cycle events can survive an uplink outage and still reach a central database without duplicate customer-visible records.

> This repository uses only synthetic data and manufacturer-neutral interfaces. It is not connected to physical equipment, is not a machine-control or safety system, and is not intended for production use.

## Status

Implementation is in progress. Executable commands, measured reliability evidence, and the complete architecture guide will replace this bootstrap outline as each local verification gate passes.

## Intended reliability boundary

Outboxer persists each accepted source event to a disk-backed edge outbox before acknowledging it. Transport retries are expected. A central application receipt marks an edge row complete only after Kafka accepts the event, while PostgreSQL uniqueness constraints make repeated delivery harmless to API consumers.

## Planned local stack

- Kotlin machine simulator and edge relay
- Java central cycle service
- MQTT 5 with two locally isolated brokers
- SQLite edge outbox
- Kafka in KRaft mode
- PostgreSQL
- Docker Compose for full end-to-end tests
- An isolated temporary kind cluster for Kubernetes smoke validation

See [the architecture guide](docs/architecture.md) for the custody chain and explicit non-guarantees.

## License

Outboxer is available under the [MIT License](LICENSE). Third-party components retain their own licenses; see [third-party notices](THIRD_PARTY_NOTICES.md).
