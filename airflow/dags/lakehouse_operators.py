"""Shared helpers: submit existing Spark jobs into the ldl-spark container."""
from __future__ import annotations

import os

from airflow.operators.bash import BashOperator


SPARK_CONTAINER = os.environ.get("LDL_SPARK_CONTAINER", "ldl-spark")


def spark_submit_task(task_id: str, job_path: str) -> BashOperator:
    """Run spark-submit for a job under /opt/jobs inside the lakehouse Spark container."""
    job = job_path.lstrip("/")
    if job.startswith("opt/jobs/"):
        job = job[len("opt/jobs/") :]
    if job.startswith("jobs/"):
        job = job[len("jobs/") :]
    container = SPARK_CONTAINER
    # -i keeps stdin closed; LocalExecutor runs this inside the Airflow container.
    cmd = (
        f'docker exec {container} /opt/spark/bin/spark-submit '
        f'--master "local[*]" "/opt/jobs/{job}"'
    )
    return BashOperator(
        task_id=task_id,
        bash_command=cmd,
    )
