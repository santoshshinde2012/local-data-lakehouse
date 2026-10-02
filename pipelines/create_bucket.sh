#!/usr/bin/env bash
# Re-run the idempotent bucket + Lakekeeper bootstrap + warehouse setup (docker/init/bootstrap.sh)
# against a running stack, e.g. after deleting the bucket by hand. Safe to run any number of times.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
docker compose --profile light run --rm --no-deps lakehouse-init --once
