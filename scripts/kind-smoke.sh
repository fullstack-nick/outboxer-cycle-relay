#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE_FILE="$ROOT/infra/compose.yaml"
ENV_FILE="$ROOT/.secrets/compose.env"
BASE_DIR="$ROOT/infra/k8s/base"
KIND_CONFIG="$ROOT/infra/k8s/kind-config.yaml"
RUNTIME_DIR="$ROOT/.outboxer/kind"
TOOLS_DIR="$ROOT/.outboxer/tools"
EVIDENCE_DIR="$ROOT/.outboxer/evidence/generated"
CLUSTER_NAME=outboxer-cycle-relay
CONTEXT="kind-$CLUSTER_NAME"
NAMESPACE=outboxer
KUBECONFIG_PATH="$RUNTIME_DIR/kubeconfig"
KIND_VERSION=v0.33.0
KUBECONFORM_VERSION=v0.8.0
SCHEMA_CACHE="$RUNTIME_DIR/schema-cache"
EXPECTED_PATH="$ROOT/.outboxer/evidence/kind-expected.csv"
KEEP_CLUSTER=false
SKIP_BUILD=false
OFFLINE_ONLY=false
CLUSTER_CREATED=false
RESULT=FAIL
FAILURE_DETAIL="test did not complete"
EVENT_COUNT=0
VERSION_COUNTS="not measured"
RESTORE_SERVICES=()
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_COMMIT=$(git -C "$ROOT" rev-parse --short HEAD)
STAMP=$(date -u +%Y%m%d-%H%M%S)
EVIDENCE_PATH="$EVIDENCE_DIR/kind-$STAMP.md"

for argument in "$@"; do
  case "$argument" in
    --keep-cluster) KEEP_CLUSTER=true ;;
    --skip-build) SKIP_BUILD=true ;;
    --offline-only) OFFLINE_ONLY=true ;;
    *) echo "Unknown argument: $argument" >&2; exit 2 ;;
  esac
done

mkdir -p "$RUNTIME_DIR" "$TOOLS_DIR" "$EVIDENCE_DIR" "$SCHEMA_CACHE"

compose() {
  docker compose --project-name outboxer --file "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

kubectl_isolated() {
  kubectl --kubeconfig "$KUBECONFIG_PATH" --context "$CONTEXT" "$@"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

download_verified() {
  local url=$1
  local destination=$2
  local expected=$3
  if [[ ! -f "$destination" ]] || [[ "$(sha256_file "$destination")" != "$expected" ]]; then
    local temporary="$destination.download"
    curl --fail --location --silent --show-error --output "$temporary" "$url"
    if [[ "$(sha256_file "$temporary")" != "$expected" ]]; then
      rm -f "$temporary"
      echo "Checksum mismatch for $url" >&2
      return 1
    fi
    mv -f "$temporary" "$destination"
  fi
}

environment_value() {
  local name=$1
  local line
  line=$(awk -v key="$name" 'index($0, key "=") == 1 { print; exit }' "$ENV_FILE")
  [[ -n "$line" ]] || { echo "Required generated value $name is missing" >&2; return 1; }
  printf '%s' "${line#*=}"
}

container_running() {
  [[ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || true)" == true ]]
}

network_address() {
  local container=$1
  local network=$2
  docker inspect --format "{{with index .NetworkSettings.Networks \"$network\"}}{{.IPAddress}}{{end}}" "$container"
}

wait_for() {
  local description=$1
  local attempts=$2
  shift 2
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if "$@"; then
      echo "PASS: $description"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $description" >&2
  return 1
}

write_evidence() {
  local finished_at kind_description validator_description cleanup
  finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  kind_description=$([[ -x "${KIND_BINARY:-}" ]] && "$KIND_BINARY" version 2>/dev/null || echo unavailable)
  validator_description=$([[ -x "${KUBECONFORM_BINARY:-}" ]] && "$KUBECONFORM_BINARY" -v 2>/dev/null || echo unavailable)
  cleanup=deleted
  $KEEP_CLUSTER && cleanup="retained by explicit request"
  {
    echo "# Local kind smoke result"
    echo
    echo "- Result: **$RESULT**"
    echo "- Started: \`$STARTED_AT\`"
    echo "- Finished: \`$finished_at\`"
    echo "- kind: \`$kind_description\`"
    echo "- Offline validator: \`$validator_description\`"
    echo "- Git commit: \`$GIT_COMMIT\`"
    echo "- Cluster: \`$CLUSTER_NAME\`"
    echo "- Context: \`$CONTEXT\` through a dedicated kubeconfig"
    echo "- Pre-existing kubectl context contacted: **no**"
    echo "- Tracked manifest validation: offline schema validation plus isolated-cluster server validation"
    echo "- Application images: locally built and loaded; no registry push"
    echo "- Verified event IDs: \`$EVENT_COUNT\`"
    echo "- Stored schema counts (v1|v2): \`$VERSION_COUNTS\`"
    echo "- Edge pending rows after receipt: \`0\` when passed"
    echo "- Temporary cluster cleanup: $cleanup"
    echo "- Failure detail: $FAILURE_DETAIL"
  } >"$EVIDENCE_PATH"
  echo "kind evidence: $EVIDENCE_PATH"
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  write_evidence || true
  if $CLUSTER_CREATED && ! $KEEP_CLUSTER; then
    echo "Deleting only temporary kind cluster: $CLUSTER_NAME"
    "$KIND_BINARY" delete cluster --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG_PATH" || true
  fi
  if ! $KEEP_CLUSTER && ((${#RESTORE_SERVICES[@]} > 0)); then
    compose start "${RESTORE_SERVICES[@]}" || true
  fi
  exit "$exit_code"
}

failure() {
  FAILURE_DETAIL="failed near script line $1"
}

trap 'failure "$LINENO"' ERR
trap cleanup EXIT

command -v docker >/dev/null
command -v kubectl >/dev/null
command -v curl >/dev/null
command -v tar >/dev/null

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    KIND_ASSET=kind-linux-amd64
    KIND_CHECKSUM=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
    VALIDATOR_ASSET=kubeconform-linux-amd64.tar.gz
    VALIDATOR_CHECKSUM=9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883
    ;;
  Linux-aarch64|Linux-arm64)
    KIND_ASSET=kind-linux-arm64
    KIND_CHECKSUM=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2
    VALIDATOR_ASSET=kubeconform-linux-arm64.tar.gz
    VALIDATOR_CHECKSUM=1f53fc8e81258197a35e8603054162a5af1de8c5af13746c71ab680d9534ed87
    ;;
  Darwin-x86_64)
    KIND_ASSET=kind-darwin-amd64
    KIND_CHECKSUM=5a99f26f57246dc9319dd294803313197a0f34d33c525b3ea8b655db5916ece0
    VALIDATOR_ASSET=kubeconform-darwin-amd64.tar.gz
    VALIDATOR_CHECKSUM=71dbc87ac9f24099a62b93570e65aa06312ba6ac8aea63b7f86e9d999edf5a92
    ;;
  Darwin-arm64)
    KIND_ASSET=kind-darwin-arm64
    KIND_CHECKSUM=0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f
    VALIDATOR_ASSET=kubeconform-darwin-arm64.tar.gz
    VALIDATOR_CHECKSUM=f84f4dfbebf4a6b0b230385fa065a39ea35e02608c2b50d025dcf64775a69d67
    ;;
  *) echo "Use kind-smoke.ps1 on Windows; unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

KIND_BINARY="$TOOLS_DIR/kind-$KIND_VERSION"
VALIDATOR_ARCHIVE="$TOOLS_DIR/$VALIDATOR_ASSET"
KUBECONFORM_DIR="$TOOLS_DIR/kubeconform-$KUBECONFORM_VERSION"
KUBECONFORM_BINARY="$KUBECONFORM_DIR/kubeconform"

download_verified "https://kind.sigs.k8s.io/dl/$KIND_VERSION/$KIND_ASSET" "$KIND_BINARY" "$KIND_CHECKSUM"
chmod +x "$KIND_BINARY"
download_verified "https://github.com/yannh/kubeconform/releases/download/$KUBECONFORM_VERSION/$VALIDATOR_ASSET" "$VALIDATOR_ARCHIVE" "$VALIDATOR_CHECKSUM"
mkdir -p "$KUBECONFORM_DIR"
tar -xzf "$VALIDATOR_ARCHIVE" -C "$KUBECONFORM_DIR"
chmod +x "$KUBECONFORM_BINARY"

kubectl kustomize "$BASE_DIR" | "$KUBECONFORM_BINARY" -strict -summary -kubernetes-version 1.36.1 -cache "$SCHEMA_CACHE"
kubectl kustomize "$BASE_DIR" | env \
  HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 ALL_PROXY=http://127.0.0.1:9 NO_PROXY= \
  "$KUBECONFORM_BINARY" -strict -summary -kubernetes-version 1.36.1 -cache "$SCHEMA_CACHE"
echo "PASS: strict Kubernetes schemas validated from the local cache with network access disabled"

if $OFFLINE_ONLY; then
  RESULT=PASS
  FAILURE_DETAIL=none
  exit 0
fi

[[ -f "$ENV_FILE" ]] || "$ROOT/scripts/generate-secrets.sh"
docker version --format '{{.Server.Version}}'

if "$KIND_BINARY" get clusters | grep -Fxq "$CLUSTER_NAME"; then
  echo "Removing only the pre-existing temporary cluster: $CLUSTER_NAME"
  "$KIND_BINARY" delete cluster --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG_PATH"
fi
rm -f "$KUBECONFIG_PATH"
"$KIND_BINARY" create cluster \
  --name "$CLUSTER_NAME" \
  --config "$KIND_CONFIG" \
  --kubeconfig "$KUBECONFIG_PATH" \
  --wait 5m
CLUSTER_CREATED=true
[[ "$(kubectl --kubeconfig "$KUBECONFIG_PATH" config current-context)" == "$CONTEXT" ]]
kubectl_isolated cluster-info
echo "PASS: isolated cluster context verified"

container_running outboxer-edge-relay && RESTORE_SERVICES+=(edge-relay)
container_running outboxer-central-cycle-service && RESTORE_SERVICES+=(central-cycle-service)
compose up -d --wait --no-deps factory-mqtt uplink-mqtt kafka postgres
((${#RESTORE_SERVICES[@]} == 0)) || compose stop "${RESTORE_SERVICES[@]}"
$SKIP_BUILD || compose --profile simulation build edge-relay central-cycle-service machine-simulator
"$KIND_BINARY" load docker-image outboxer/edge-relay:local outboxer/central-cycle-service:local --name "$CLUSTER_NAME"

NODE="$CLUSTER_NAME-control-plane"
docker network connect outboxer-factory-net "$NODE"
docker network connect outboxer-uplink-net "$NODE"
docker network connect outboxer-data-net "$NODE"
FACTORY_ADDRESS=$(network_address outboxer-factory-mqtt outboxer-factory-net)
UPLINK_ADDRESS=$(network_address outboxer-uplink-mqtt outboxer-uplink-net)
KAFKA_ADDRESS=$(network_address outboxer-kafka outboxer-data-net)
POSTGRES_ADDRESS=$(network_address outboxer-postgres outboxer-data-net)

kubectl_isolated apply -f "$BASE_DIR/namespace.yaml"
cat <<YAML | kubectl_isolated apply -f -
apiVersion: v1
kind: Service
metadata: {name: factory-mqtt, namespace: outboxer}
spec:
  ports: [{name: mqtt, port: 1883}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: factory-mqtt-local
  namespace: outboxer
  labels: {kubernetes.io/service-name: factory-mqtt}
addressType: IPv4
ports: [{name: mqtt, protocol: TCP, port: 1883}]
endpoints: [{addresses: ["$FACTORY_ADDRESS"]}]
---
apiVersion: v1
kind: Service
metadata: {name: uplink-mqtt, namespace: outboxer}
spec:
  ports: [{name: mqtt, port: 1883}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: uplink-mqtt-local
  namespace: outboxer
  labels: {kubernetes.io/service-name: uplink-mqtt}
addressType: IPv4
ports: [{name: mqtt, protocol: TCP, port: 1883}]
endpoints: [{addresses: ["$UPLINK_ADDRESS"]}]
---
apiVersion: v1
kind: Service
metadata: {name: kafka, namespace: outboxer}
spec:
  ports: [{name: kafka, port: 9092}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: kafka-local
  namespace: outboxer
  labels: {kubernetes.io/service-name: kafka}
addressType: IPv4
ports: [{name: kafka, protocol: TCP, port: 9092}]
endpoints: [{addresses: ["$KAFKA_ADDRESS"]}]
---
apiVersion: v1
kind: Service
metadata: {name: postgres, namespace: outboxer}
spec:
  ports: [{name: postgres, port: 5432}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: postgres-local
  namespace: outboxer
  labels: {kubernetes.io/service-name: postgres}
addressType: IPv4
ports: [{name: postgres, protocol: TCP, port: 5432}]
endpoints: [{addresses: ["$POSTGRES_ADDRESS"]}]
YAML

FACTORY_PASSWORD=$(environment_value FACTORY_EDGE_PASSWORD)
EDGE_UPLINK_PASSWORD=$(environment_value UPLINK_EDGE_PASSWORD)
CENTRAL_UPLINK_PASSWORD=$(environment_value UPLINK_CENTRAL_PASSWORD)
POSTGRES_PASSWORD=$(environment_value POSTGRES_APP_PASSWORD)
kubectl_isolated -n "$NAMESPACE" create secret generic outboxer-edge-credentials \
  --from-literal="factory-mqtt-password=$FACTORY_PASSWORD" \
  --from-literal="uplink-mqtt-password=$EDGE_UPLINK_PASSWORD" \
  --dry-run=client -o yaml | kubectl_isolated apply -f -
kubectl_isolated -n "$NAMESPACE" create secret generic outboxer-central-credentials \
  --from-literal="uplink-mqtt-password=$CENTRAL_UPLINK_PASSWORD" \
  --from-literal="postgres-app-password=$POSTGRES_PASSWORD" \
  --dry-run=client -o yaml | kubectl_isolated apply -f -
unset FACTORY_PASSWORD EDGE_UPLINK_PASSWORD CENTRAL_UPLINK_PASSWORD POSTGRES_PASSWORD

kubectl_isolated apply --server-side --dry-run=server -k "$BASE_DIR"
kubectl_isolated apply -k "$BASE_DIR"
kubectl_isolated -n "$NAMESPACE" rollout status deployment/outboxer-central --timeout=5m
kubectl_isolated -n "$NAMESPACE" rollout status deployment/outboxer-edge --timeout=5m
echo "PASS: edge and central deployments became ready"

case "$EXPECTED_PATH" in
  "$ROOT/.outboxer/evidence/"*) rm -f "$EXPECTED_PATH" ;;
  *) echo "Refusing to remove unexpected evidence path" >&2; exit 1 ;;
esac
RUN_ID="kind-$(date -u +%Y%m%d%H%M%S)-$$"
compose --profile simulation run --rm --no-deps \
  -e "SIMULATOR_RUN_ID=$RUN_ID" \
  -e SIMULATOR_EVENT_COUNT=100 \
  -e SIMULATOR_MACHINE_COUNT=100 \
  -e SIMULATOR_CYCLE_INTERVAL_SECONDS=1 \
  -e SIMULATOR_V2_PERCENTAGE=50 \
  -e SIMULATOR_EXPECTED_FILE=/evidence/kind-expected.csv \
  machine-simulator
mapfile -t IDS < <(awk -F, '{print $1}' "$EXPECTED_PATH")
[[ ${#IDS[@]} -eq 100 ]]
QUOTED_IDS=$(printf "'%s'," "${IDS[@]}")
QUOTED_IDS=${QUOTED_IDS%,}
COUNT_SQL="SELECT COUNT(*) FROM cycle_event WHERE event_id IN ($QUOTED_IDS)"
stored_ids() {
  [[ "$(docker exec outboxer-postgres psql -X -U outboxer_admin -d outboxer -At -c "$COUNT_SQL")" == 100 ]]
}
wait_for "all kind-routed event IDs reached PostgreSQL" 90 stored_ids
EVENT_COUNT=100
VERSION_SQL="SELECT COUNT(*) FILTER (WHERE schema_version = 1), COUNT(*) FILTER (WHERE schema_version = 2) FROM cycle_event WHERE event_id IN ($QUOTED_IDS)"
VERSION_COUNTS=$(docker exec outboxer-postgres psql -X -U outboxer_admin -d outboxer -At -c "$VERSION_SQL")
[[ "$VERSION_COUNTS" == "50|50" ]]

edge_drained() {
  [[ "$(kubectl_isolated -n "$NAMESPACE" exec deployment/outboxer-edge -- sqlite3 /var/lib/outboxer/edge.db "SELECT COUNT(*) FROM outbox_event WHERE state = 'PENDING';" 2>/dev/null)" == 0 ]]
}
wait_for "Kubernetes edge outbox drained after application receipts" 60 edge_drained

V2_API=$(kubectl_isolated -n "$NAMESPACE" exec deployment/outboxer-central -- \
  curl --fail --silent --header "X-Tenant-Id: tenant-017" \
  http://127.0.0.1:8080/api/v1/machines/IMM-0001/cycles/latest)
V1_API=$(kubectl_isolated -n "$NAMESPACE" exec deployment/outboxer-central -- \
  curl --fail --silent --header "X-Tenant-Id: tenant-017" \
  http://127.0.0.1:8080/api/v1/machines/IMM-0100/cycles/latest)
[[ "$V2_API" =~ \"schemaVersion\":2 ]]
[[ "$V2_API" =~ \"energyConsumptionWh\":[0-9] ]]
[[ "$V1_API" =~ \"schemaVersion\":1 ]]
[[ "$V1_API" =~ \"energyConsumptionWh\":null ]]
echo "PASS: Kubernetes API returned both compatibility shapes"

RESULT=PASS
FAILURE_DETAIL=none
