"""Shared helpers for the graph DAG: run a command inside ldl-graph, or spark-submit a job with
extra spark-submit arguments inside ldl-spark.

lakehouse_operators.spark_submit_task (the user's helper, unchanged and reused for its container
name) takes no --packages / --conf, which the optional OpenLineage run needs; spark_submit_args_task
adds them. Every argument is shell-quoted; nothing from a DAG run's conf is interpolated into a
command except through the fixed Jinja block the caller passes in ``template_prefix``.
"""
from __future__ import annotations

import os
import shlex
from collections.abc import Mapping, Sequence

from airflow.providers.standard.operators.bash import BashOperator

from lakehouse_operators import SPARK_CONTAINER

GRAPH_CONTAINER = os.environ.get("LDL_GRAPH_CONTAINER", "ldl-graph")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def _not_a_template(cmd: str) -> str:
    """BashOperator treats a bash_command ending in .sh / .bash as a Jinja template FILE to load."""
    return cmd + " " if cmd.endswith((".sh", ".bash")) else cmd


def docker_exec_command(container: str, argv: Sequence[str], env: Mapping[str, str] | None = None) -> str:
    """`docker exec [-e K=V ...] <container> <argv...>` with every piece shell-quoted (no TTY)."""
    if not argv:
        raise ValueError("argv must not be empty")
    parts = ["docker", "exec"]
    for key, value in sorted((env or {}).items()):
        parts += ["-e", f"{key}={value}"]
    return _not_a_template(shlex.join([*parts, container, *argv]))


def graph_exec_task(task_id: str, command: str | Sequence[str], env: Mapping[str, str] | None = None) -> BashOperator:
    """Run ``command`` inside the ldl-graph container (working dir /opt/lakehouse, GRAPH_ROOT set by Compose).

    ``command`` is a plain string (split with shlex; the form the DAG uses, because the Tier-0
    lineage extractor reads a task's command from its string literals) or an argv list.
    """
    argv = shlex.split(command) if isinstance(command, str) else list(command)
    return BashOperator(
        task_id=task_id,
        bash_command=docker_exec_command(GRAPH_CONTAINER, argv, env),
        do_xcom_push=False,  # the last log line is not a result; keep it out of the metadata DB
    )


def spark_submit_args_task(task_id: str, job_path: str, job_args: Sequence[str] = (),
                           template_prefix: str = "", template_before: str = "") -> BashOperator:
    """spark-submit a job under /opt/jobs inside ldl-spark, with job arguments.

    ``template_prefix`` is inserted verbatim between `--master local[*]` and the job path, and
    ``template_before`` verbatim before the command (a guarded `... &&` step): the only places a
    Jinja block (the optional OpenLineage switch) may appear.
    """
    job = job_path.lstrip("/")
    for prefix in ("opt/jobs/", "jobs/"):
        if job.startswith(prefix):
            job = job[len(prefix):]
    head = shlex.join(["docker", "exec", SPARK_CONTAINER, SPARK_SUBMIT, "--master", "local[*]"])
    tail = shlex.join([f"/opt/jobs/{job}", *job_args])
    return BashOperator(
        task_id=task_id,
        bash_command=_not_a_template(f"{template_before}{head} {template_prefix}{tail}"),
        do_xcom_push=False,
    )
