#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
secret_root="${root_dir}/.secrets"
environment_file="${secret_root}/compose.env"
image="eclipse-mosquitto:2.1.2-alpine"

if [[ -f "${environment_file}" && "${1:-}" != "--force" ]]; then
  echo "Local secrets already exist at ${environment_file}"
  exit 0
fi

random_secret() {
  openssl rand -base64 24 | tr '+/' 'AB' | tr -d '='
}

mkdir -p "${secret_root}/mqtt/factory" "${secret_root}/mqtt/uplink"

factory_simulator_password="$(random_secret)"
factory_edge_password="$(random_secret)"
factory_health_password="$(random_secret)"
uplink_edge_password="$(random_secret)"
uplink_central_password="$(random_secret)"
uplink_health_password="$(random_secret)"
postgres_superuser_password="$(random_secret)"
postgres_app_password="$(random_secret)"

password_file() {
  local directory="$1"
  shift
  local create_flag="-c"
  while (( "$#" )); do
    docker run --rm --user 0 --mount "type=bind,src=${directory},dst=/work" "${image}" \
      mosquitto_passwd -b ${create_flag} /work/passwords "$1" "$2"
    create_flag=""
    shift 2
  done
  docker run --rm --user 0 --mount "type=bind,src=${directory},dst=/work" "${image}" chmod 0644 /work/passwords
}

password_file "${secret_root}/mqtt/factory" \
  simulator "${factory_simulator_password}" \
  edge-factory "${factory_edge_password}" \
  health "${factory_health_password}"
password_file "${secret_root}/mqtt/uplink" \
  edge-uplink "${uplink_edge_password}" \
  central-uplink "${uplink_central_password}" \
  health "${uplink_health_password}"

umask 077
printf '%s\n' \
  "FACTORY_SIMULATOR_PASSWORD=${factory_simulator_password}" \
  "FACTORY_EDGE_PASSWORD=${factory_edge_password}" \
  "FACTORY_HEALTH_PASSWORD=${factory_health_password}" \
  "UPLINK_EDGE_PASSWORD=${uplink_edge_password}" \
  "UPLINK_CENTRAL_PASSWORD=${uplink_central_password}" \
  "UPLINK_HEALTH_PASSWORD=${uplink_health_password}" \
  "POSTGRES_SUPERUSER_PASSWORD=${postgres_superuser_password}" \
  "POSTGRES_APP_PASSWORD=${postgres_app_password}" > "${environment_file}"

echo "Generated local credentials under ${secret_root}"
