#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export OUTBOXER_CRASH_REPETITIONS="${OUTBOXER_CRASH_REPETITIONS:-3}"
export OUTBOXER_RESET="${OUTBOXER_RESET:-false}"
export OUTBOXER_CLEANUP="${OUTBOXER_CLEANUP:-false}"
exec "${root_dir}/gradlew" crashWindowTest
