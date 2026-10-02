#!/usr/bin/env bash
# Wait until the lakehouse is usable: postgres, lakekeeper, objectstore healthy and lakehouse-init done
# (healthy); with NEED_SPARK=1 (the default for the Spark pipelines) also ldl-spark healthy.
#   ./pipelines/wait_for_stack.sh              # NEED_SPARK=1
#   NEED_SPARK=0 ./pipelines/wait_for_stack.sh # light profile
# Idempotent and read-only: it never starts anything (make up-light / make up-full do).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NEED_SPARK="${NEED_SPARK:-1}"
services=(postgres lakekeeper objectstore lakehouse-init)
[[ "$NEED_SPARK" == "1" ]] && services+=(spark)
health() { docker compose --profile '*' ps -a "$1" --format '{{.Health}}' 2>/dev/null || true; }

echo "==> Waiting for: ${services[*]}"
for i in $(seq 1 60); do
  line="" ok=1
  for s in "${services[@]}"; do
    h="$(health "$s")"
    line+="$s=${h:-absent} "
    [[ "$h" == "healthy" ]] || ok=0
  done
  if [[ "$ok" == "1" ]]; then
    echo "Stack ready ($line)"
    exit 0
  fi
  if [[ "$i" == "1" && "$line" == *absent* ]]; then
    echo "  not started: run make up-full (Spark pipelines) or make up-light" >&2
  fi
  echo "  ... $line($i/60)"
  sleep 2
done
echo "Timed out. Run: docker compose --profile '*' ps -a" >&2
exit 1
