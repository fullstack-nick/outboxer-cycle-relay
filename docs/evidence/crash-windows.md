# Deterministic crash-window evidence

- Result: **PASS**
- Tested commit: `8ca0bd0bf9beea25aa2eefe09af7da2f737d2e76`
- Started: `2026-09-04T16:24:51.733344500Z`
- Finished: `2026-09-04T16:31:54.855043100Z`
- Host runtime: Docker Engine `29.7.2`, Java `21.0.10`

## Scenario

The harness began from fresh, explicitly reset Outboxer volumes and used deliberate `Runtime.halt` fault markers at both ambiguous transaction boundaries. Each boundary was exercised three times with a new deterministic event identity.

### Edge boundary

The edge was halted immediately after the uplink MQTT transport acknowledgement but before an application receipt could durably settle its SQLite row. After restart, the pending row replayed, the receipt arrived, and PostgreSQL was required to contain exactly one row for that identity.

### Central boundary

The central consumer was halted immediately after the PostgreSQL transaction committed but before the Kafka listener could return and advance its group offset. After restart, Kafka replayed the record, which had to classify as an identical duplicate and remain one customer-visible row.

## Assertions

| Gate | Repetitions | Result |
| --- | ---: | --- |
| intended edge halt marker observed | 3 | PASS |
| edge replay settled its receipt | 3 | PASS |
| edge duplicate delivery observed | 3 | PASS |
| edge event had exactly one PostgreSQL row | 3 | PASS |
| intended central halt marker observed | 3 | PASS |
| Kafka replay observed after restart | 3 | PASS |
| central event had exactly one PostgreSQL row | 3 | PASS |

## Measurements

| Measurement | Value |
| --- | ---: |
| Repetitions per crash boundary | 3 |
| Injected process halts | 6 |
| Duplicate events observed | 6 |
| Customer-visible rows per identity | 1 |
| Preflight free disk | 162.5 GiB |

The result demonstrates harmless reprocessing at the two intentionally injected points. It does not prove safety at untested hardware or storage-corruption boundaries. Run it with [the crash wrapper](../../scripts/crash-window-test.sh); the fault switches default to disabled outside the harness.
