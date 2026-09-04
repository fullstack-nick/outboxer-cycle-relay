#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export OUTBOXER_OUTAGE_SECONDS="${OUTBOXER_OUTAGE_SECONDS:-600}"
export OUTBOXER_RESET="${OUTBOXER_RESET:-false}"
export OUTBOXER_CLEANUP="${OUTBOXER_CLEANUP:-false}"
exec "${root_dir}/gradlew" outageTest
