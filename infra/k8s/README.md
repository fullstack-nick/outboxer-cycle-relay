# Local Kubernetes proof

The tracked base contains only the Outboxer edge and central workloads: two Deployments, two internal Services, configuration, secret references, probes, resource bounds, security contexts, and a persistent volume claim for the edge SQLite database. Docker Compose remains the primary executable and reliability-test environment.

The smoke scripts create a temporary single-node cluster named `outboxer-cycle-relay`. They use a dedicated kubeconfig and the explicit context `kind-outboxer-cycle-relay` for every cluster operation. They never select or modify the user's default Kubernetes context.

During the test, the kind node is temporarily attached to Outboxer's three local Compose networks. Generated endpoint objects connect the Kubernetes applications to the local MQTT, Kafka, and PostgreSQL test dependencies. Credentials are created from the ignored `.secrets/compose.env`; no Secret values exist in these manifests or reports.

Run on Windows:

```powershell
.\scripts\kind-smoke.ps1
```

Run on Linux or macOS:

```bash
bash scripts/kind-smoke.sh
```

Use `-OfflineOnly` or `--offline-only` to run strict schema validation without creating a cluster. The validator and Kubernetes schemas are cached locally, then validation is repeated with network access disabled. The full smoke also asks the isolated API server for a server-side dry run.

By default, the scripts delete only the named temporary cluster and restore any Compose applications that were running before the test. `-KeepCluster` or `--keep-cluster` is intended only for local diagnosis; Compose applications remain stopped in that mode to avoid duplicate MQTT client identities.
