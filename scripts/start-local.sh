#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${root_dir}/infra/compose.yaml"
environment_file="${root_dir}/.secrets/compose.env"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required but was not found on PATH." >&2
  exit 1
}

"${root_dir}/scripts/generate-secrets.sh"
docker compose \
  --project-name outboxer \
  --file "${compose_file}" \
  --env-file "${environment_file}" \
  --profile simulation \
  up --detach --build --wait

printf '%s\n' \
  "Outboxer is ready." \
  "Central API:  http://127.0.0.1:18080" \
  "Edge health:  http://127.0.0.1:18081/actuator/health/readiness" \
  "Stop safely:  ./scripts/stop-local.sh"
