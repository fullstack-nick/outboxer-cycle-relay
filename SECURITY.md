# Security policy

Outboxer is an independent, local educational simulation. It processes synthetic data and is not intended to control physical equipment or serve public traffic.

## Supported version

Security fixes are applied to the current `main` branch. Historical evidence tags preserve tested source and are not maintained release lines.

## Report a vulnerability

Please use [GitHub private vulnerability reporting](https://github.com/fullstack-nick/outboxer-cycle-relay/security/advisories/new). Do not include credentials, exploit details, or sensitive workstation information in a public issue.

Include:

- the affected component and commit;
- required configuration and preconditions;
- a minimal reproduction using synthetic data;
- the expected and observed security boundary;
- impact and any suggested mitigation.

Reports will be acknowledged and assessed on a best-effort basis. This open-source project does not promise a response-time service level. Please allow time for a fix and coordinated disclosure before publishing exploit details.

## Relevant reports

Examples include MQTT ACL bypass, topic/payload identity confusion, tenant predicate omission, unsafe deserialization, SQL injection, credential disclosure, container privilege escalation, dependency verification bypass, unintended Kubernetes-context mutation, or acceptance scripts deleting resources outside their exact scope.

The following are expected properties rather than vulnerabilities:

- `X-Tenant-Id` scopes local-demo queries but is not authentication or authorization;
- local MQTT traffic is not encrypted inside isolated Docker networks;
- a Docker or host administrator can access generated secrets and local volumes;
- deleting named project volumes deletes the local data;
- the single-node services do not tolerate host or storage failure;
- duplicate network delivery can occur and is handled idempotently.

See the [threat model](docs/threat-model.md) for assets, boundaries, implemented controls, and residual risks.

## Safe testing

Test only resources you own. Keep reproduction traffic on loopback or an isolated local network, use synthetic payloads, and avoid denial-of-service volumes that could affect other workloads on the workstation. The repository's reset routines are opt-in and constrained to five named Outboxer volumes; do not broaden them in a security reproduction.
