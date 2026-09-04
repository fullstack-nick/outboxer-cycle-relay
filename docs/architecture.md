# Architecture

Outboxer demonstrates a local reliability boundary for one synthetic business event: `CYCLE_COMPLETED`.

```text
machine-simulator -> factory-mqtt -> edge-relay + SQLite
                                      |
                                      v
                                  uplink-mqtt
                                      |
                                      v
                              central-cycle-service
                                   |          |
                                   v          v
                                 Kafka    PostgreSQL -> REST API
```

The edge relay is the only component connected to the factory-side and uplink-side networks. The central service is the only application connected to MQTT, Kafka, and PostgreSQL. Everything runs on the developer's local machine.

## Custody chain

1. The simulator publishes a source event with MQTT 5 QoS 1.
2. The edge validates and durably writes a canonical event to SQLite before acknowledging the source delivery.
3. The edge publishes the stored bytes to the uplink broker with a response topic and correlation data.
4. The central service validates the event and waits for Kafka acknowledgement.
5. The central service publishes an application receipt.
6. The edge marks its outbox row sent only after receiving that receipt.
7. The Kafka consumer commits an idempotent PostgreSQL transaction before its offset advances.

This is at-least-once transport with an effectively-once customer-visible database effect. It is not exactly-once network delivery.

## Failure boundary

Named volumes protect state from application and container restart. They do not protect against destruction of the host, Docker volumes, or storage media. The single-node local Kafka deployment is intentionally not highly available.

