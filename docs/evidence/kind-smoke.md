# Isolated local kind evidence

- Result: **PASS**
- Tested commit: `7ee9a778ca1f200035ae68ed69e0da406062f19a`
- Started: `2026-09-04T15:58:06.6602025Z`
- Finished: `2026-09-04T16:00:16.1292391Z`
- kind: `v0.33.0`
- kubeconform: `v0.8.0`
- Kubernetes node: `v1.36.1`, pinned by digest

## Isolation controls

The script created cluster `outboxer-cycle-relay` with a dedicated temporary kubeconfig and explicit context `kind-outboxer-cycle-relay`. It did not contact the pre-existing default `kubectl` context. Application images were built locally and loaded directly into the node; no registry or cloud service was used.

Tracked manifests first passed strict cached-schema validation with networking forced to a closed local route. The isolated API server then accepted the rendered resources with server-side dry run before apply. Runtime secrets and dependency endpoint objects were generated from ignored local data and were not written to this report.

## Runtime assertions

| Gate | Result |
| --- | --- |
| edge and central Deployments available | PASS |
| readiness probes healthy | PASS |
| 100 exact expected identities in PostgreSQL | PASS |
| stored schema counts v1/v2 | 50 / 50 |
| API v1 null and v2 numeric energy shape | PASS |
| edge pending rows after receipts | 0 |
| temporary cluster deleted | PASS |

Docker Compose supplied only Kafka, PostgreSQL, and both MQTT dependencies; the two application workloads ran inside Kubernetes. The node was temporarily attached to the three Outboxer Docker networks, then the cluster was deleted and prior Compose application state restored.

This proves the tracked application manifests can run in an isolated local cluster. It does not claim a production Kubernetes platform, high availability, or cloud deployment. See [the kind instructions](../../infra/k8s/README.md).
