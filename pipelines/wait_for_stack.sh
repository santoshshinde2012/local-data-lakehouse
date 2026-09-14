#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
echo "==> Waiting for Compose services..."
for i in $(seq 1 60); do
  pg=$(docker compose ps postgres --format '{{.Health}}' 2>/dev/null || true)
  so=$(docker compose ps silo --format '{{.Health}}' 2>/dev/null || true)
  sp=$(docker compose ps spark --format '{{.State}}' 2>/dev/null || true)
  if [[ "$pg" == "healthy" && "$so" == "healthy" && "$sp" == "running" ]]; then
    echo "Stack ready (postgres=$pg silo=$so spark=$sp)"
    exit 0
  fi
  echo "  ... postgres=$pg silo=$so spark=$sp ($i/60)"
  sleep 2
done
echo "Timed out. Run: docker compose ps"
exit 1
