#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
jobs=(
  churn/01_ingest_bronze.py
  churn/02_transform_silver.py
  churn/03_publish_gold_features.py
  churn/04_export_features.py
)
echo "==> Churn features E2E ($(date -u +%Y-%m-%dT%H:%MZ))"
for job in "${jobs[@]}"; do
  echo; echo "======== ${job} ========"
  ./pipelines/run_job.sh "${job}"
done
echo; echo "==> Exports:"; ls -la data/export/ || true
echo "==> Churn E2E complete."
