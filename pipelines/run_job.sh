#!/usr/bin/env bash
# Usage: ./pipelines/run_job.sh retail/01_smoke_test.py
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
JOB="${1:?Usage: $0 <path-under-src/jobs/>}"
JOB="${JOB#/opt/jobs/}"
echo "==> spark-submit /opt/jobs/${JOB}"
docker compose exec spark /opt/spark/bin/spark-submit --master 'local[*]' "/opt/jobs/${JOB}"
