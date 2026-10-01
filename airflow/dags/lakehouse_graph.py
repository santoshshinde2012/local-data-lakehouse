"""
Renewal graph on gold, lakehouse-native path (needs the graph overlay: docker-compose.graph.yml):

    publish gold.graph_* + tag graph_<build_id>  (Spark job, ldl-spark)
      >> build the graph from Iceberg, pinned by tag + snapshot id  (PyIceberg + LadybugDB, ldl-graph)
      >> graph contract (strict) >> lineage >> lineage contract >> cohorts >> promote

The same chain as pipelines/run_graph_e2e.sh. Run lakehouse_churn_features first: the publish job
reads gold.churn_renewal_features and fails with a clear message when it is missing (no sensor:
manual runs have unrelated logical dates). Manual trigger only.

Optional OpenLineage (runs, parents and timing as JSON lines under /opt/data/graph/lineage/):
trigger with --conf '{"openlineage": true}'. Only the boolean switch is read from the run conf;
spark-submit then downloads the openlineage-spark package from Maven Central on first use.

NOTE: this repo pins Airflow 2.10.4, and Airflow 2.x reached end of life on 2026-04-22. The DAG uses
only `airflow.DAG` and `airflow.operators.bash.BashOperator`; an Airflow 3 port changes those two
imports (airflow.sdk.DAG, airflow.providers.standard.operators.bash) and the overlay's services,
not the task chain.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG

from lakehouse_graph_operators import SPARK_CONTAINER, docker_exec_command, graph_exec_task, spark_submit_args_task

# Every graph task works on the `default` profile, the only one that reads the lakehouse's own bronze
# and can be promoted. The commands are plain string literals: the Tier-0 lineage extractor links a
# DAG task to the scripts it runs by reading its string arguments.

OPENLINEAGE_VERSION = "1.53.0"
OPENLINEAGE_DIR = "/opt/data/graph/lineage"
# Fixed blocks: the only run-time switch is the boolean `params.openlineage`.
OPENLINEAGE_ARGS = (
    "{% if params.openlineage is true %}"
    f"--packages io.openlineage:openlineage-spark_2.12:{OPENLINEAGE_VERSION} "
    "--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener "
    "--conf spark.openlineage.transport.type=file "
    f"--conf spark.openlineage.transport.location={OPENLINEAGE_DIR}/openlineage.jsonl "
    "--conf spark.openlineage.namespace=lakehouse "
    "{% endif %}"
)
# The file transport does not create its directory.
OPENLINEAGE_MKDIR = (
    "{% if params.openlineage is true %}"
    f"{docker_exec_command(SPARK_CONTAINER, ['mkdir', '-p', OPENLINEAGE_DIR])} && "
    "{% endif %}"
)

with DAG(
    dag_id="lakehouse_graph",
    description="gold.churn_renewal_features → gold.graph_* (+ tags) → renewal graph → contract → lineage → "
                "cohorts → promote",
    start_date=datetime(2024, 3, 1),
    schedule=None,  # manual trigger for local demos
    catchup=False,
    max_active_runs=1,  # one build at a time (the builder also holds $GRAPH_ROOT/.lock)
    params={"openlineage": os.environ.get("OPENLINEAGE", "0") == "1"},
    tags=["lakehouse", "graph", "churn"],
) as dag:
    publish = spark_submit_args_task(
        "publish_gold_graph", "graph/01_publish_gold_graph.py", template_prefix=OPENLINEAGE_ARGS,
        template_before=OPENLINEAGE_MKDIR,
    )
    build = graph_exec_task(
        "build_graph", "python scripts/build_graph_local.py build --source iceberg --profile default"
    )
    contract = graph_exec_task(
        "check_graph_contract", "python scripts/check_graph_contract.py --profile default --strict"
    )
    lineage = graph_exec_task("build_lineage", "python scripts/build_lineage_local.py --graph-profile default")
    lineage_check = graph_exec_task(
        "check_lineage_contract", "python scripts/check_lineage_contract.py --graph-profile default"
    )
    cohorts = graph_exec_task("build_cohorts", "python scripts/build_graph_cohorts.py build --profile default")
    # Promote exactly the build that was just checked (<profile>/latest), not "the newest passing one".
    promote = graph_exec_task(
        "promote",
        "python scripts/build_graph_local.py promote --profile default --build /opt/data/graph/default/latest",
    )

    publish >> build >> contract >> lineage >> lineage_check >> cohorts >> promote
