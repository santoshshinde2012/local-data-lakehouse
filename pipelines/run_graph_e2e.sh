#!/usr/bin/env bash
# Renewal graph, lakehouse-native path (the same chain as the lakehouse_graph DAG):
#   ldl-spark  spark-submit graph/01_publish_gold_graph.py: gold.graph_* + CREATE TAG graph_<build_id>
#              on gold.churn_renewal_features, the silver inputs and every graph table
#   ldl-graph  build --source iceberg (PyIceberg, pinned by tag + snapshot id) -> graph contract --strict
#              -> lineage -> lineage contract -> cohorts -> promote (the build just checked)
# Needs the full profile with the graph overlay, and the churn gold:
#   make up-full && make churn-e2e
#   mkdir -p data/graph && docker compose -f docker-compose.yml -f docker-compose.graph.yml --profile full up -d --build --wait
#   ./pipelines/run_graph_e2e.sh                    # or: make graph-e2e (does the two lines above too)
# OPENLINEAGE=1 ./pipelines/run_graph_e2e.sh: the publish job also writes OpenLineage runs, parents and
#   timing as JSON lines to data/graph/lineage/openlineage.jsonl. spark-submit --packages downloads
#   io.openlineage:openlineage-spark_2.13 from Maven Central on first use; the driver log (noisy) goes to data/graph/logs/ instead of the terminal.
# GRAPH_HOST_ROOT=<dir> (default data/graph): the host directory the overlay mounts at /opt/data/graph.
# GRAPH_E2E_SAMPLE_DIR / GRAPH_E2E_EXPORT_DIR (paths INSIDE ldl-graph; default: the repo's
#   data/sample/churn and data/export): the default profile's bronze CSVs and exports, e.g. the tiny
#   fixture /opt/lakehouse/data/sample/churn/fixtures/tiny after loading the lakehouse from it.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.graph.yml --profile full)
PROFILE=default   # the only profile that reads the lakehouse's own bronze and can be promoted
HOST_ROOT="${GRAPH_HOST_ROOT:-data/graph}"   # the host side of /opt/data/graph (docker-compose.graph.yml)
OPENLINEAGE_VERSION=1.53.0
OL_DIR=/opt/data/graph/lineage

graph_env=()
if [[ -n "${GRAPH_E2E_SAMPLE_DIR:-}" ]]; then
  graph_env+=(-e "CHURN_SAMPLE_DIR=${GRAPH_E2E_SAMPLE_DIR}")
fi
if [[ -n "${GRAPH_E2E_EXPORT_DIR:-}" ]]; then
  graph_env+=(-e "CHURN_EXPORT_DIR=${GRAPH_E2E_EXPORT_DIR}")
fi

spark_args=()
if [[ "${OPENLINEAGE:-0}" == "1" ]]; then
  spark_args+=(
    --packages "io.openlineage:openlineage-spark_2.13:${OPENLINEAGE_VERSION}"
    --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener"
    --conf "spark.openlineage.transport.type=file"
    --conf "spark.openlineage.transport.location=${OL_DIR}/openlineage.jsonl"
    --conf "spark.openlineage.namespace=lakehouse"
  )
fi

step() {
  echo
  echo "======== $* ========"
}

# Run a repo script inside ldl-graph. Every step is required: a missing script stops the chain, so a
# build that was never checked can never reach the promote step.
in_graph() {
  local script="$1"
  shift
  if [[ ! -f "$script" ]]; then
    echo "Graph E2E FAILED: $script is not in this checkout; stopping before any later step (nothing promoted)" >&2
    exit 1
  fi
  step "ldl-graph: python $script $*"
  local started=$SECONDS
  # ${arr[@]+"${arr[@]}"}: an empty array is an "unbound variable" under set -u in bash 3.2 (macOS).
  "${COMPOSE[@]}" exec -T ${graph_env[@]+"${graph_env[@]}"} graph python "$script" "$@"
  echo "    ($((SECONDS - started)) s)"
}

echo "==> Graph E2E ($(date -u +%Y-%m-%dT%H:%MZ)), profile ${PROFILE}"
for svc in spark graph; do
  state="$("${COMPOSE[@]}" ps "$svc" --format '{{.State}}' 2>/dev/null || true)"
  if [[ "$state" != "running" ]]; then
    echo "Service '$svc' is not running (state: ${state:-absent}). Run: make up-full && make churn-e2e," >&2
    echo "then: mkdir -p data/graph && ${COMPOSE[*]} up -d --build --wait" >&2
    exit 1
  fi
done
mkdir -p "$HOST_ROOT/logs"
started_all=$SECONDS

step "ldl-spark: spark-submit /opt/jobs/graph/01_publish_gold_graph.py${OPENLINEAGE:+ (OPENLINEAGE=$OPENLINEAGE)}"
started=$SECONDS
if [[ "${OPENLINEAGE:-0}" == "1" ]]; then
  log="$HOST_ROOT/logs/graph_publish_$(date -u +%Y%m%dT%H%M%SZ).log"
  "${COMPOSE[@]}" exec -T spark mkdir -p "$OL_DIR"
  # ${arr[@]+"${arr[@]}"}: an empty array is an "unbound variable" under set -u in bash 3.2 (macOS).
  if ! "${COMPOSE[@]}" exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' \
      ${spark_args[@]+"${spark_args[@]}"} /opt/jobs/graph/01_publish_gold_graph.py 2>"$log"; then
    echo "Graph publish FAILED; driver log: $log" >&2
    tail -n 40 "$log" >&2
    exit 1
  fi
  echo "    driver log: $log; OpenLineage events: $HOST_ROOT/lineage/openlineage.jsonl"
else
  "${COMPOSE[@]}" exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' \
    ${spark_args[@]+"${spark_args[@]}"} /opt/jobs/graph/01_publish_gold_graph.py
fi
echo "    ($((SECONDS - started)) s)"

in_graph scripts/build_graph_local.py build --source iceberg --profile "$PROFILE"
in_graph scripts/check_graph_contract.py --profile "$PROFILE" --strict
in_graph scripts/build_lineage_local.py --graph-profile "$PROFILE"
in_graph scripts/check_lineage_contract.py --graph-profile "$PROFILE"
in_graph scripts/build_graph_cohorts.py build --profile "$PROFILE"
# Promote exactly the build that was just checked (<profile>/latest), not "the newest passing one".
in_graph scripts/build_graph_local.py promote --profile "$PROFILE" --build "/opt/data/graph/${PROFILE}/latest"

echo
echo "==> Graph E2E complete in $((SECONDS - started_all)) s. Promoted build: $HOST_ROOT/current"
