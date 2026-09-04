# Sustained load evidence

- Result: **PASS**
- Tested commit: `8ca0bd0bf9beea25aa2eefe09af7da2f737d2e76`
- Started: `2026-09-04T16:22:13.407511600Z`
- Finished: `2026-09-04T16:24:38.233942700Z`
- Host runtime: Docker Engine `29.7.2`, Java `21.0.10`

## Scenario

From fresh, explicitly reset Outboxer volumes, the deterministic simulator published 6,000 mixed-version events from 3,000 machines. Each machine emitted twice during the 60-second measured source window, for a target of 100 events/second. The harness saved the exact expected event identities and waited for both PostgreSQL reconciliation and edge outbox drain.

## Assertions

| Gate | Result |
| --- | --- |
| preflight disk, ports, and healthy base stack | PASS |
| simulator produced exactly 6,000 expected identities | PASS |
| every expected identity reached PostgreSQL | PASS |
| edge outbox drained | PASS |
| every machine sequence was contiguous | PASS |
| source pacing remained near 100 events/second | PASS |

## Measurements

| Measurement | Value |
| --- | ---: |
| Events | 6,000 |
| Source duration | 59.98 s |
| Source rate | 100.02 events/s |
| Preflight free disk | 162.51 GiB |

Identity-set equality is the acceptance condition: equal totals alone could hide one missing event and one unexpected event. This test covers sustained healthy flow; outage recovery has a separate [10-minute proof](v1-outage.md). Run the scenario with [the load wrapper](../../scripts/load-test.sh).

The result is specific to the listed commit, host, container versions, and local resource state. It is not a general performance benchmark or sizing promise.
