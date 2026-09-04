# Troubleshooting

## Start with scoped status

From the repository root:

```powershell
docker compose --project-name outboxer --file infra/compose.yaml --env-file .secrets/compose.env --profile simulation ps
```

```bash
docker compose --project-name outboxer --file infra/compose.yaml --env-file .secrets/compose.env --profile simulation ps
```

These commands inspect only this project. Avoid broad Docker cleanup commands on a workstation that runs other projects.

## Local secrets are missing

Symptom: Compose reports a missing `.secrets/compose.env` or password file.

Generate credentials:

```powershell
.\scripts\generate-secrets.ps1
```

```bash
./scripts/generate-secrets.sh
```

To replace credentials intentionally, stop the project first and pass `-Force` or `--force`. Existing broker and PostgreSQL volumes were initialized with the old credentials, so rotating credentials while preserving initialized volumes can make services fail authentication. For a disposable demo, use a reset acceptance run after reviewing its exact five-volume warning.

## A loopback port is already in use

The preflight reports the occupied port before it starts containers. Check its owner on Windows:

```powershell
Get-NetTCPConnection -State Listen | Where-Object LocalPort -In 11883,11884,15432,18080,18081,19092
```

On Linux:

```bash
ss -ltnp | grep -E ':(11883|11884|15432|18080|18081|19092)\b'
```

Stop or reconfigure the owning application. Do not terminate an unrelated process solely because an acceptance test wants the port.

## A service is unhealthy

Inspect recent logs for one named service:

```bash
docker compose --project-name outboxer --file infra/compose.yaml --env-file .secrets/compose.env logs --tail 200 edge-relay
docker compose --project-name outboxer --file infra/compose.yaml --env-file .secrets/compose.env logs --tail 200 central-cycle-service
```

Health endpoints separate process liveness from dependency readiness:

```bash
curl http://127.0.0.1:18081/actuator/health/liveness
curl http://127.0.0.1:18081/actuator/health/readiness
curl http://127.0.0.1:18080/actuator/health/liveness
curl http://127.0.0.1:18080/actuator/health/readiness
```

An edge process can remain live but become unready during an uplink outage. That is expected: it continues durable source intake while reporting degraded forwarding.

## The edge backlog is not draining

Check these edge metrics:

```bash
curl --silent http://127.0.0.1:18081/actuator/prometheus | grep -E 'edge_outbox_(pending|oldest_pending_age_seconds)|edge_(publish_failures|receipt_timeouts)_total'
```

Then verify the uplink broker and central service are healthy. Frequent receipt timeouts with a healthy broker usually indicate central Kafka publication trouble or invalid response metadata. Do not edit SQLite manually; its pending rows are the current durable owner.

## PostgreSQL has fewer rows than expected

An accepted edge receipt means Kafka owns the event, not necessarily that PostgreSQL has consumed it yet. Check central readiness and consumer logs, then wait for Kafka lag to drain. The acceptance harness uses exact identity sets and a bounded wait instead of assuming immediate query visibility.

If central repeatedly logs database persistence failure at one partition position, fix PostgreSQL availability. The listener intentionally does not skip valid records when persistence is unavailable.

## An event is rejected

Common reasons are an unsupported `schemaVersion`, an unknown field, a negative or excessive value, invalid UUID/identifier syntax, a future timestamp beyond tolerance, topic/payload mismatch, missing response metadata, or an identity reused with different content.

Edge rejections are stored in SQLite with a payload hash and bounded excerpt. Central rejections are published to `cycle-events-invalid`. Correct the producer; do not make validation permissive to force one message through.

## The API returns 400, 404, or 503

- `400`: supply a valid `X-Tenant-Id`, valid machine identifier, and `limit` from 1 through 100.
- `404`: no event or data-quality state exists for that tenant and machine.
- `503`: a readiness dependency is unavailable; inspect `/actuator/health/readiness`.

The header scopes local queries but is not authentication.

## Docker image builds fail dependency verification

Run the same wrapper outside Docker first:

```bash
./gradlew clean check --no-daemon
```

Do not bypass checksum verification. If a dependency was intentionally updated, review its origin and license, refresh locks and verification metadata in a dedicated commit, and inspect the diff before accepting it.

## kind validation or smoke fails

Run schema validation alone:

```powershell
.\scripts\kind-smoke.ps1 -OfflineOnly
```

```bash
./scripts/kind-smoke.sh --offline-only
```

The full script needs Docker, Java 21, `kubectl`, enough disk, and access to download pinned tools on the first run. Later schema validation uses the local cache. The script does not use your default Kubernetes context.

If `-KeepCluster` / `--keep-cluster` was used, inspect the exact context:

```bash
kubectl --kubeconfig .outboxer/kind/kubeconfig --context kind-outboxer-cycle-relay get pods -n outboxer
```

Use the script's printed kubeconfig path if it differs. Delete only the named test cluster when finished:

```bash
kind delete cluster --name outboxer-cycle-relay
```

## Return to a non-destructive stopped state

Use [the stop script](../scripts/stop-local.sh), which preserves volumes:

```powershell
.\scripts\stop-local.ps1
```

```bash
./scripts/stop-local.sh
```
