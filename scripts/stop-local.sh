#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${root_dir}/infra/compose.yaml"
environment_file="${root_dir}/.secrets/compose.env"

if [[ ! -f "${environment_file}" ]]; then
  echo "No generated Outboxer environment was found; nothing to stop."
  exit 0
fi

docker compose \
  --project-name outboxer \
  --file "${compose_file}" \
  --env-file "${environment_file}" \
  --profile simulation \
  down --remove-orphans
