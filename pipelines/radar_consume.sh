#!/usr/bin/env bash
# Retention Radar consumes this repo's churn export (what CI's "Retention Radar consumes the export" runs).
#   ./pipelines/radar_consume.sh [export_dir] [checkout_dir]
# CI clones radar itself (so the lineage graph sees the clone) and passes RADAR_CHECKOUT_READY=1.
# Ref choice: RADAR_REF if set; else the radar branch named like this branch (paired PRs); else RADAR_V2_SHA,
# the last radar commit that reads the v2 export (hero_inference_record.json). Radar main still reads the
# v1 export (santosh_inference_record.json) until radar PR #21 (branch feat/local-first-stack-2026, the v2
# reader on top of main) merges, so falling back to main fails with FileNotFoundError.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXPORT_DIR="$(cd "${1:-$ROOT/data/export}" && pwd)"
DEST="${2:-${TMPDIR:-/tmp}/retention-radar}"
URL="${RADAR_URL:-https://github.com/santoshshinde2012/retention-radar.git}"
V2_SHA="${RADAR_V2_SHA:-953af3ab57b79bf6c22a1a4f6a4ae299de90b6d3}"
BRANCH="${GITHUB_HEAD_REF:-${GITHUB_REF_NAME:-$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)}}"
# Radar needs Python 3.12+ (its bundle is XGBoost 3.4.1 on 3.12); prefer python3.12 when present.
PY="${PYTHON:-$(command -v python3.12 || command -v python3)}"

for f in churn_user_features.csv hero_inference_record.json; do
  [[ -f "$EXPORT_DIR/$f" ]] || { echo "missing $EXPORT_DIR/$f (run the pandas or the Spark churn pipeline first)" >&2; exit 2; }
done

if [[ "${RADAR_CHECKOUT_READY:-0}" == 1 && -d "$DEST/.git" ]]; then
  echo "using the existing checkout at $DEST (RADAR_CHECKOUT_READY=1)"
else
  if [[ -n "${RADAR_REF:-}" ]]; then REF="$RADAR_REF"
  elif [[ "$BRANCH" != main ]] && git ls-remote --exit-code --heads "$URL" "$BRANCH" >/dev/null 2>&1; then REF="$BRANCH"
  else REF="$V2_SHA"; fi
  echo "retention-radar ref: $REF"
  rm -rf "$DEST"
  git init -q "$DEST"
  git -C "$DEST" remote add origin "$URL"
  git -C "$DEST" fetch -q --depth 1 origin "$REF"
  git -C "$DEST" checkout -q FETCH_HEAD
fi
echo "retention-radar commit: $(git -C "$DEST" rev-parse --short HEAD)"

cd "$DEST"
if [[ ! -x .venv/bin/python ]]; then "$PY" -m venv .venv; fi
.venv/bin/python -m pip install -q -r requirements.txt
.venv/bin/python -m pip install -q -e .
# macOS without `brew install libomp`: the XGBoost and LightGBM wheels only look in
# /opt/homebrew/opt/libomp, but scikit-learn ships its own libomp. Point them at it
# (venv-local, ad-hoc re-signed). No-op on Linux or when libomp is installed.
if [[ "$(uname)" == Darwin ]] && ! .venv/bin/python -c 'import xgboost, lightgbm' >/dev/null 2>&1; then
  site="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
  if [[ -f "$site/sklearn/.dylibs/libomp.dylib" ]]; then
    echo "XGBoost/LightGBM cannot load libomp; using scikit-learn's bundled copy (or: brew install libomp)"
    for lib in "$site/xgboost/lib/libxgboost.dylib" "$site/lightgbm/lib/lib_lightgbm.dylib"; do
      [[ -f "$lib" ]] || continue
      install_name_tool -add_rpath "@loader_path/../../sklearn/.dylibs" "$lib" 2>/dev/null || true
      codesign --force --sign - "$lib" >/dev/null 2>&1 || true
    done
  fi
  .venv/bin/python -c 'import xgboost, lightgbm' || { echo "XGBoost/LightGBM still fail to load: brew install libomp" >&2; exit 1; }
fi
# Copy the export into radar's data/external (what radar's own sync script does, called directly).
PYTHONPATH=src .venv/bin/python -c 'import sys; from pathlib import Path
from retention_radar.data.ingest import sync_lakehouse_exports
for p in sync_lakehouse_exports(Path(sys.argv[1])): print(f"Synced -> {p}")' "$EXPORT_DIR"
CHURN_DATA_SOURCE=lakehouse PYTHONPATH=src .venv/bin/python -m retention_radar.cli.ingest
CHURN_DATA_SOURCE=lakehouse PYTHONPATH=src .venv/bin/python -m retention_radar.cli.batch_score \
  --csv data/external/churn_user_features.csv --out "$DEST/scores.csv"
echo "==> radar scored $(($(wc -l < "$DEST/scores.csv") - 1)) rows -> $DEST/scores.csv"
