# V1/V2 Docker Compose smoke evidence

- Result: **PASS**
- Tested commit: `cb014c50f0c6b668259adf4144c67bf66dcd9cc1`
- Started: `2026-09-04T15:34:34.377185400Z`
- Finished: `2026-09-04T15:35:57.588331900Z`
- Host runtime: Docker Engine `29.7.2`, Java `21.0.10`

## Scope

This run tested the complete Compose path with fresh Outboxer volumes after the additive v2 contract and PostgreSQL migration. It emitted explicit v1 and v2 traffic and exercised happy, invalid, delayed-receipt, application-restart, and broker-restart paths.

## Passed gates

- Application containers ran non-root with read-only root filesystems.
- Factory, uplink, and data networks were segmented; all host ports were loopback-only.
- Exact v1 and v2 rows reached PostgreSQL.
- V1 returned `energyConsumptionWh: null`; v2 returned its numeric value through the same API.
- All three API routes enforced the tenant header and returned tenant-scoped data.
- Invalid factory and central payloads entered their rejection paths without blocking the next valid event.
- An MQTT publish acknowledgement alone did not complete the edge row.
- Edge restart preserved that pending row; its later application receipt completed it.
- The factory persistent session survived a broker restart.
- Required metrics were present without event, machine, site, or tenant labels.

## Measurements

| Measurement | Value |
| --- | ---: |
| Final PostgreSQL rows | 10 |
| Edge rejected rows | 2 |
| Invalid Kafka topic end offset | 2 |
| Preflight free disk | 163.0 GiB |

The small row count is intentional: this scenario prioritizes branch coverage and exact inspection. Sustained throughput and long-outage behavior have separate evidence. Run it through [the smoke wrapper](../../scripts/compose-smoke.sh).
