#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Compose reads AIRFLOW_API_PORT from .env; read it the same way when the shell does not set it.
if [[ -z "${AIRFLOW_API_PORT:-}" && -f "$ROOT/.env" ]]; then
  AIRFLOW_API_PORT="$(sed -n 's/^AIRFLOW_API_PORT=//p' "$ROOT/.env" | tail -n 1)"
fi
PORT="${AIRFLOW_API_PORT:-8080}"
echo "==> Waiting for the Airflow API server…"
for _ in $(seq 1 90); do
  if body="$(curl -fsS "http://localhost:${PORT}/api/v2/monitor/health" 2>/dev/null)"; then
    if python3 -c 'import json,sys; h=json.loads(sys.argv[1]); sys.exit(0 if all(h.get(k,{}).get("status")=="healthy" for k in ("metadatabase","scheduler","dag_processor")) else 1)' "$body"; then
      echo "Airflow healthy: http://localhost:${PORT}"
      exit 0
    fi
  fi
  sleep 3
done
echo "Timed out waiting for Airflow on port ${PORT}" >&2
echo "$body" >&2 || true
exit 1
