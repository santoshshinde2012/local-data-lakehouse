#!/usr/bin/env bash
# Usage: ./pipelines/airflow_trigger.sh <dag_id>
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DAG_ID="${1:?Usage: $0 <dag_id>}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.airflow.yml)

echo "==> Unpausing + triggering ${DAG_ID}"
"${COMPOSE[@]}" exec -T airflow-scheduler airflow dags unpause "${DAG_ID}" >/dev/null || true
RUN_OUT="$("${COMPOSE[@]}" exec -T airflow-scheduler airflow dags trigger "${DAG_ID}" -o plain)"
echo "$RUN_OUT"

echo "==> Waiting for ${DAG_ID} to finish…"
for i in $(seq 1 180); do
  # Prefer JSON for stable parsing when available
  JSON="$("${COMPOSE[@]}" exec -T airflow-scheduler \
    airflow dags list-runs -d "${DAG_ID}" -o json 2>/dev/null || true)"
  if [[ -n "$JSON" && "$JSON" != "[]" ]]; then
    STATE="$(python3 -c '
import json,sys
runs=json.loads(sys.stdin.read() or "[]")
if not runs:
    print("")
else:
    runs=sorted(runs, key=lambda r: r.get("start_date") or r.get("execution_date") or "", reverse=True)
    print(runs[0].get("state") or "")
' <<<"$JSON")"
    echo "  [$i] state=${STATE:-unknown}"
    case "${STATE}" in
      success) echo "==> ${DAG_ID} succeeded"; exit 0 ;;
      failed|upstream_failed) echo "==> ${DAG_ID} failed (${STATE})" >&2; exit 1 ;;
    esac
  else
    PLAIN="$("${COMPOSE[@]}" exec -T airflow-scheduler \
      airflow dags list-runs -d "${DAG_ID}" -o plain 2>/dev/null | head -8 || true)"
    echo "  [$i] ${PLAIN}" | head -3
    if echo "$PLAIN" | awk 'NR==2' | grep -q success; then
      echo "==> ${DAG_ID} succeeded"
      exit 0
    fi
    if echo "$PLAIN" | awk 'NR==2' | grep -Eq 'failed|upstream_failed'; then
      echo "==> ${DAG_ID} failed" >&2
      exit 1
    fi
  fi
  sleep 5
done
echo "Timed out waiting for ${DAG_ID}" >&2
"${COMPOSE[@]}" exec -T airflow-scheduler airflow dags list-runs -d "${DAG_ID}" -o plain || true
exit 1
