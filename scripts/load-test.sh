#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export OUTBOXER_LOAD_SECONDS="${OUTBOXER_LOAD_SECONDS:-60}"
export OUTBOXER_RESET="${OUTBOXER_RESET:-false}"
export OUTBOXER_CLEANUP="${OUTBOXER_CLEANUP:-false}"
exec "${root_dir}/gradlew" loadTest
