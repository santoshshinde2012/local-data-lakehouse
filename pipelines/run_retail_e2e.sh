#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
jobs=(
  retail/01_smoke_test.py
  retail/02_ingest_bronze.py
  retail/03_transform_silver.py
  retail/04_publish_gold.py
  retail/05_query_timetravel.py
)
echo "==> Retail E2E ($(date -u +%Y-%m-%dT%H:%MZ))"
for job in "${jobs[@]}"; do
  echo; echo "======== ${job} ========"
  ./pipelines/run_job.sh "${job}"
done
echo; echo "==> Retail E2E complete."
