# Threat model

## Scope

This model covers the local Outboxer runtime, generated synthetic events, ignored credentials, tracked source, container images, local Docker networks, named volumes, and the temporary kind cluster. It assumes a developer-controlled workstation and no direct connection to physical equipment or a public network.

The primary security goals are:

- prevent one component from publishing or subscribing outside its assigned MQTT topics;
- prevent malformed or spoofed identities from crossing a trust boundary unnoticed;
- keep credentials and private keys out of Git history;
- preserve evidence of invalid or conflicting input without letting it block valid flow;
- reduce the effect of a compromised application container;
- make exhaustion and dependency failure visible.

## Assets

| Asset | Security property |
| --- | --- |
| Event identity and measurements | integrity, traceability |
| SQLite/Kafka/PostgreSQL state | integrity, availability |
| MQTT and database credentials | confidentiality |
| Topic and tenant boundaries | integrity, separation |
| Acceptance evidence | integrity, reproducibility |
| Developer workstation | availability, local confidentiality |

Synthetic event content is not confidential. Local credentials still matter because they grant write access to the demo's trust boundaries.

## Trust boundaries

1. A source event crosses from the simulator identity into the factory broker and edge relay.
2. A canonical event crosses from edge custody into the uplink broker and central service.
3. A record crosses from MQTT processing into Kafka custody.
4. A Kafka record crosses into the PostgreSQL query model.
5. An HTTP caller supplies a tenant scope header to the local API.
6. Build tooling downloads dependencies, container images, schemas, kind, and the manifest validator.

Docker network isolation limits reachability but is not a defense against a host administrator. The kind node temporarily joins the three Compose networks during its smoke test.

## Threats and controls

| Threat | Implemented controls | Residual risk |
| --- | --- | --- |
| Source spoofing | generated per-role credentials; factory topic ACLs; topic/payload identity validation | any process that reads the ignored simulator credential can impersonate it |
| Uplink spoofing | distinct edge and central credentials; uplink ACLs; canonical schema; response-topic and correlation checks | MQTT is not encrypted inside the local network |
| Topic injection | fixed topic grammar, bounded identifiers, payload/topic equality, Mosquitto ACL patterns | a compromised broker can bypass its own policy |
| Replay or duplicate delivery | stable UUIDs, natural unique key, payload hash, idempotent Kafka producer, PostgreSQL constraints | repeated delivery still consumes CPU, storage I/O, and metrics capacity |
| Identity reuse with different data | SHA-256 comparison and explicit conflict isolation | an operator must inspect and resolve the sender fault |
| Malformed or oversized payload | 64 KiB broker/application limit, strict schemas, timestamp/value bounds, quarantine | bounded excerpts still contain supplied synthetic text |
| Poison message blocks a stream | invalid input is stored in a rejection table or invalid Kafka topic and then acknowledged | invalid-topic retention is finite only through broker policy/operator action |
| Disk exhaustion | startup free-space preflight, SQLite storage readiness, pending-age/count metrics, bounded memory queues | no automatic storage expansion or remote alert receiver exists |
| Memory/CPU denial of service | bounded queues, bounded payloads, container memory limits, Kubernetes resources | a local user can still exhaust host resources or bypass container limits |
| Broker compromise | network segmentation, least-topic ACLs, isolated role credentials | broker administrators can read, alter, or delete all broker state |
| Application compromise | non-root images, read-only root filesystems, dropped capabilities, no new privileges; restricted Kubernetes security context | edge and central still possess credentials required for their roles |
| Database compromise | application role separate from bootstrap administrator; data network isolation | local Docker/host administrators can access the volume and superuser secret |
| HTTP tenant confusion | required validated `X-Tenant-Id`, tenant predicates on every query, no-store responses | the header is scoping only and provides no authentication or authorization |
| Secret committed to Git | ignored `.secrets/`, generated credentials, publication scan of tracked files and all revisions | heuristic scanning cannot prove absence of every possible secret form |
| Dependency tampering | Gradle wrapper checksum, dependency locks, verification metadata, pinned container/tool versions, SHA-256 tool downloads | upstream image tags and base packages are not vendored; no offline mirror or signature policy |
| Unsafe Kubernetes context | dedicated temporary kubeconfig and explicit kind context for every operation | a compromised `kubectl`, Docker daemon, or kind executable remains trusted local code |
| Acceptance-script overreach | exact project name and exact volume allowlist; no default reset; dedicated cluster name | a user with shell access can modify scripts or invoke Docker directly |

## Abuse cases

### Valid identity on the wrong topic

The edge and central validators compare topic segments with payload fields. The event is rejected even if its JSON alone is valid. This prevents a publisher from routing one machine's data under another machine's topic.

### Same UUID with altered measurements

The persisted SHA-256 hash differs, so the event becomes `IDENTITY_CONFLICT`. It is not treated as a duplicate and does not overwrite the first accepted row.

### Missing application receipt

The edge times out the in-flight attempt and republishes from SQLite. This favors availability and loss prevention over suppressing transport duplicates.

### Tenant header switched by a caller

The API returns only rows matching the supplied tenant and machine. However, because any local caller may supply any valid tenant string, this is not an access-control boundary. A deployed variant would require authenticated identity mapped server-side to authorized tenants.

## Deliberate exclusions

The current version does not implement TLS or mutual TLS, an external identity provider, certificate rotation, an encrypted volume, a secrets manager, signed container admission, host intrusion detection, broker clustering, database replication, or public ingress. These would be necessary considerations before adapting the design to a real environment.

## Reporting and review

Security reports should follow [SECURITY.md](../SECURITY.md). Revisit this model whenever a new externally reachable interface, payload type, identity source, persistence engine, or deployment target is introduced.
