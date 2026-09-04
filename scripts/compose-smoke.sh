#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
reset="${OUTBOXER_RESET:-false}"
cleanup="${OUTBOXER_CLEANUP:-false}"

while (( "$#" )); do
  case "$1" in
    --reset) reset=true ;;
    --cleanup) cleanup=true ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

export OUTBOXER_RESET="${reset}"
export OUTBOXER_CLEANUP="${cleanup}"
exec "${root_dir}/gradlew" composeSmokeTest
