#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.airflow.yml)

echo "==> Waiting for Airflow webserver…"
for i in $(seq 1 60); do
  if curl -fsS "http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}/health" >/dev/null 2>&1; then
    echo "Airflow UI healthy: http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}"
    exit 0
  fi
  sleep 3
done
echo "Timed out waiting for Airflow on port ${AIRFLOW_WEBSERVER_PORT:-8080}" >&2
"${COMPOSE[@]}" ps
exit 1
