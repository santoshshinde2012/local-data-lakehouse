#!/usr/bin/env bash
# Usage: ./pipelines/airflow_trigger.sh <dag_id> [conf-json]
# Airflow 3 CLI inside the scheduler: unpause, trigger, poll the run until it ends.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DAG_ID="${1:?Usage: $0 <dag_id> [conf-json]}"
CONF="${2:-}"; [[ -n "$CONF" ]] || CONF="{}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.airflow.yml --profile full)
af() { "${COMPOSE[@]}" exec -T airflow-scheduler airflow "$@"; }

echo "==> Waiting for ${DAG_ID} to be parsed…"
for _ in $(seq 1 40); do
  af dags list -o json 2>/dev/null | grep -q "\"${DAG_ID}\"" && break
  sleep 3
done
af dags unpause "${DAG_ID}" >/dev/null
RUN_ID="manual__ldl_$(date -u +%Y%m%dT%H%M%S)"
echo "==> Triggering ${DAG_ID} (${RUN_ID})"
af dags trigger "${DAG_ID}" --run-id "${RUN_ID}" --conf "${CONF}" -o plain >/dev/null

STATE=""
for i in $(seq 1 360); do
  STATE="$(af dags list-runs "${DAG_ID}" -o json 2>/dev/null | python3 -c '
import json,sys
run_id=sys.argv[1]
runs=[r for r in json.loads(sys.stdin.read() or "[]") if r.get("run_id")==run_id]
print(runs[0].get("state","") if runs else "")' "${RUN_ID}" || true)"
  (( i % 6 == 1 )) && echo "  [$((i * 5))s] state=${STATE:-queued}"
  case "${STATE}" in
    success) echo "==> ${DAG_ID} succeeded"; exit 0 ;;
    failed) echo "==> ${DAG_ID} failed" >&2; af tasks states-for-dag-run "${DAG_ID}" "${RUN_ID}" -o plain >&2 || true; exit 1 ;;
  esac
  sleep 5
done
echo "Timed out waiting for ${DAG_ID}" >&2
exit 1
