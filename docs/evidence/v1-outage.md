# V1 10-minute uplink-outage evidence

- Result: **PASS**
- Tested commit: `ab230cf5106ad860cc27952f0361a840c99e8fce` (`v1.0.0-evidence`)
- Started: `2026-09-04T15:09:57.996447Z`
- Finished: `2026-09-04T15:25:19.364144900Z`
- Host runtime: Docker Engine `29.7.2`, Java `21.0.10`

## Scenario

The deterministic simulator represented 3,000 machines, each completing one cycle every 30 seconds, for a target source rate of 100 events/second. The test stopped only the uplink broker for 600 configured seconds while leaving the factory broker, edge process, and simulator active. It restarted the edge during the outage, restored the uplink, continued live source traffic during replay, and reconciled every expected event identity with PostgreSQL.

The run began from an explicit reset of the five disposable Outboxer volumes. Required loopback ports and free disk were checked before mutation.

## Assertions

| Gate | Result |
| --- | --- |
| preflight disk, ports, and healthy base stack | PASS |
| edge restart preserved pending custody | PASS |
| source rate and pending/source reconciliation | PASS |
| edge liveness stayed up and readiness degraded | PASS |
| backlog drained after broker restoration | PASS |
| live events progressed during replay | PASS |
| exact expected identity set present | PASS |
| no customer-visible duplicate row | PASS |
| no per-machine sequence gap | PASS |
| Kafka offsets preserved machine order | PASS |
| recovery throughput exceeded source throughput | PASS |
| latest, recent, and data-quality APIs were tenant-scoped | PASS |
| API rejected a missing tenant header | PASS |

## Measurements

| Measurement | Value |
| --- | ---: |
| Actual uplink outage | 601.54 s |
| Events produced during outage | 60,072 |
| Peak pending SQLite rows | 60,154 |
| Readiness restoration after uplink return | 13.02 s |
| Live events accepted during recovery | 23,004 |
| Total expected event identities | 84,914 |
| Final PostgreSQL rows | 84,914 |
| Rows recovered during measured window | 84,340 |
| Recovery duration | 229.58 s |
| Gross recovery throughput | 367.37 events/s |
| Preflight free disk | 163.17 GiB |

## Interpretation

The exact identity and row equality is the loss/duplicate acceptance gate; throughput alone is insufficient. Readiness correctly signaled an impaired uplink while liveness allowed durable source intake. The edge restart proved pending state was on disk rather than only in process memory.

This result applies to the listed commit, local host, versions, and configuration. It does not establish host-failure tolerance or a universal capacity figure. The executable scenario is [the outage wrapper](../../scripts/outage-test.sh), backed by `AcceptanceCoordinator.java`.
