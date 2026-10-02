"""Structure test for airflow/dags/lakehouse_graph.py that needs NO Airflow install.

The DAG files import exactly two Airflow 3 names: ``airflow.sdk.DAG`` and
``airflow.providers.standard.operators.bash.BashOperator``. When Airflow is not importable (the graph venvs, CI) this
test registers two small stand-ins, imports the DAG files from airflow/dags and checks the task
graph and every command; with a real Airflow importable the same assertions run on the real
objects. The repo's own ./airflow directory is a namespace package, so "importable" means a spec
WITH an origin. It also checks every flag a task passes against the script's own argparse
definitions, keeps pipelines/run_graph_e2e.sh in step with the DAG (statically, and by running it
against a fake `docker` that records its calls: the same chain in the same order, and a missing
script, a failing step or a stopped service ends the run before promote), and parses the user's
two existing DAGs through the same stand-ins.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import os
import re
import shlex
import shutil
import subprocess
import sys
import types
from pathlib import Path
from typing import ClassVar

import pytest

REPO = Path(__file__).resolve().parents[2]
DAGS = REPO / "airflow" / "dags"
_SPEC = importlib.util.find_spec("airflow")
HAVE_AIRFLOW = _SPEC is not None and _SPEC.origin is not None

EXPECTED_CHAIN = ["publish_gold_graph", "build_graph", "check_graph_contract", "build_lineage",
                  "check_lineage_contract", "build_cohorts", "promote"]


class _StubDAG:
    _stack: ClassVar[list[_StubDAG]] = []

    def __init__(self, dag_id: str, **kwargs):
        self.dag_id, self.kwargs, self.task_dict = dag_id, kwargs, {}

    def __enter__(self):
        _StubDAG._stack.append(self)
        return self

    def __exit__(self, *exc):
        _StubDAG._stack.pop()
        return False


class _StubOperator:
    def __init__(self, task_id: str, bash_command: str, **kwargs):
        if not _StubDAG._stack:
            raise RuntimeError(f"task {task_id!r} created outside a `with DAG(...)` block")
        dag = _StubDAG._stack[-1]
        if task_id in dag.task_dict:
            raise ValueError(f"duplicate task_id {task_id!r}")
        self.task_id, self.bash_command, self.kwargs = task_id, bash_command, kwargs
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()
        dag.task_dict[task_id] = self

    @staticmethod
    def _many(other):
        return list(other) if isinstance(other, (list, tuple)) else [other]

    def __rshift__(self, other):
        for o in self._many(other):
            self.downstream_task_ids.add(o.task_id)
            o.upstream_task_ids.add(self.task_id)
        return other

    def __rrshift__(self, other):
        for o in self._many(other):
            o.__rshift__(self)
        return self


def _install_stubs(monkeypatch) -> None:
    names = ("airflow", "airflow.sdk", "airflow.providers", "airflow.providers.standard",
             "airflow.providers.standard.operators", "airflow.providers.standard.operators.bash")
    mods = {n: types.ModuleType(n) for n in names}
    mods["airflow.sdk"].DAG = _StubDAG
    mods["airflow.providers.standard.operators.bash"].BashOperator = _StubOperator
    for name in names[1:]:
        parent, _, leaf = name.rpartition(".")
        setattr(mods[parent], leaf, mods[name])
    for name, mod in mods.items():
        monkeypatch.setitem(sys.modules, name, mod)


def _shape(dag) -> dict:
    if isinstance(dag, _StubDAG):
        k = dag.kwargs
        schedule, catchup, params, tags = k.get("schedule", "MISSING"), k.get("catchup"), k.get("params", {}), \
            k.get("tags", [])
        max_active_runs = k.get("max_active_runs")
    else:
        schedule = getattr(dag, "schedule_interval", None) if hasattr(dag, "schedule_interval") else dag.schedule
        catchup, tags, max_active_runs = dag.catchup, list(dag.tags), dag.max_active_runs
        params = {k: (v.value if hasattr(v, "value") else v) for k, v in dict(dag.params).items()}
    tasks = {tid: {"cmd": t.bash_command, "up": set(t.upstream_task_ids), "down": set(t.downstream_task_ids),
                   "xcom": t.kwargs.get("do_xcom_push", True) if isinstance(t, _StubOperator) else t.do_xcom_push}
             for tid, t in dag.task_dict.items()}
    return {"dag_id": dag.dag_id, "schedule": schedule, "catchup": catchup, "params": params, "tags": set(tags),
            "max_active_runs": max_active_runs, "tasks": tasks}


HELPERS = ("lakehouse_operators", "lakehouse_graph_operators")   # imported by name from airflow/dags


@pytest.fixture
def load_dag(monkeypatch):
    """Load a DAG file BY PATH under a private module name: airflow/dags/lakehouse_graph.py must never
    shadow the lakehouse_graph package in this process (Airflow itself names DAG modules by path)."""
    if not HAVE_AIRFLOW:
        _install_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS))   # how Airflow itself resolves lakehouse_operators
    for var in ("OPENLINEAGE", "LDL_GRAPH_CONTAINER", "LDL_SPARK_CONTAINER"):
        monkeypatch.delenv(var, raising=False)

    def _load(dag_file: str) -> dict:
        for name in HELPERS:
            monkeypatch.delitem(sys.modules, name, raising=False)
        mspec = importlib.util.spec_from_file_location(f"dag_under_test_{dag_file}", DAGS / f"{dag_file}.py")
        module = importlib.util.module_from_spec(mspec)
        prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
        try:
            mspec.loader.exec_module(module)
        finally:
            sys.dont_write_bytecode = prev
        dags = [v for v in vars(module).values() if type(v).__name__ in ("_StubDAG", "DAG")]
        assert len(dags) == 1, f"{dag_file}: expected exactly one DAG object, found {len(dags)}"
        return _shape(dags[0])

    yield _load
    for name in HELPERS:
        sys.modules.pop(name, None)


def _argv(cmd: str) -> list[str]:
    return shlex.split(cmd)


def test_graph_dag_shape(load_dag):
    d = load_dag("lakehouse_graph")
    assert d["dag_id"] == "lakehouse_graph"
    assert d["schedule"] is None, "manual trigger only"
    assert d["catchup"] is False and d["max_active_runs"] == 1
    assert {"lakehouse", "graph"} <= d["tags"]
    assert d["params"] == {"openlineage": False}
    assert list(d["tasks"]) == EXPECTED_CHAIN


def test_graph_dag_is_a_linear_chain(load_dag):
    tasks = load_dag("lakehouse_graph")["tasks"]
    for i, tid in enumerate(EXPECTED_CHAIN):
        assert tasks[tid]["up"] == ({EXPECTED_CHAIN[i - 1]} if i else set()), tid
        assert tasks[tid]["down"] == ({EXPECTED_CHAIN[i + 1]} if i + 1 < len(EXPECTED_CHAIN) else set()), tid


def test_graph_dag_commands(load_dag):
    tasks = load_dag("lakehouse_graph")["tasks"]
    publish = tasks["publish_gold_graph"]["cmd"]
    guarded = "{% if params.openlineage is true %}"
    assert publish.startswith(guarded + "docker exec ldl-spark mkdir -p /opt/data/graph/lineage && {% endif %}")
    assert "docker exec ldl-spark /opt/spark/bin/spark-submit --master 'local[*]' " + guarded in publish
    assert publish.endswith("/opt/jobs/graph/01_publish_gold_graph.py")
    assert publish.count("{%") == 4 and "{{" not in publish, "only the two fixed boolean-guarded blocks"
    assert "--packages io.openlineage:openlineage-spark_2.13:1.53.0" in publish
    assert "spark.openlineage.transport.location=/opt/data/graph/lineage/openlineage.jsonl" in publish
    for tid in EXPECTED_CHAIN[1:]:
        argv = _argv(tasks[tid]["cmd"])
        assert argv[:4] == ["docker", "exec", "ldl-graph", "python"], tid
        assert argv[4].startswith("scripts/") and argv[4].endswith(".py"), tid
        assert "{" not in tasks[tid]["cmd"], tid
        assert "-it" not in argv and "-t" not in argv, "no TTY under the scheduler"
    assert _argv(tasks["build_graph"]["cmd"])[5:] == ["build", "--source", "iceberg", "--profile", "default"]
    assert "--strict" in _argv(tasks["check_graph_contract"]["cmd"])
    assert _argv(tasks["promote"]["cmd"])[-2:] == ["--build", "/opt/data/graph/default/latest"]
    for tid, t in tasks.items():
        assert t["xcom"] is False, f"{tid}: do_xcom_push must be False"


def _script_flags(rel: str) -> set[str]:
    """Every --flag the script's argparse definitions accept (static; any subparser)."""
    tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
    return {a.value for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
            for a in n.args if isinstance(a, ast.Constant) and str(a.value).startswith("--")}


def test_every_task_names_an_existing_script_and_real_flags(load_dag):
    tasks = load_dag("lakehouse_graph")["tasks"]
    assert (REPO / "src/jobs/graph/01_publish_gold_graph.py").is_file()
    for tid in EXPECTED_CHAIN[1:]:
        argv = _argv(tasks[tid]["cmd"])[4:]
        script = argv[0]
        assert (REPO / script).is_file(), f"{tid}: {script} is not in the repo"
        unknown = sorted({a for a in argv[1:] if a.startswith("--")} - _script_flags(script))
        assert not unknown, f"{tid}: {script} has no {unknown}"
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        blg = importlib.import_module("build_graph_local")
        a = blg.parse(_argv(tasks["build_graph"]["cmd"])[5:])
        assert (a.cmd, a.source, a.profile) == ("build", "iceberg", "default")
        a = blg.parse(_argv(tasks["promote"]["cmd"])[5:])
        assert (a.cmd, a.build) == ("promote", "/opt/data/graph/default/latest")
    finally:
        sys.path.remove(str(REPO / "scripts"))
        sys.modules.pop("build_graph_local", None)


def test_e2e_script_runs_the_same_chain(load_dag):
    """pipelines/run_graph_e2e.sh and the DAG run the same scripts with the same arguments."""
    tasks = load_dag("lakehouse_graph")["tasks"]
    text = (REPO / "pipelines/run_graph_e2e.sh").read_text(encoding="utf-8")
    calls = [shlex.split(m.replace('"$PROFILE"', "default").replace('"/opt/data/graph/${PROFILE}/latest"',
                                                                        "/opt/data/graph/default/latest"))
             for m in re.findall(r"^in_graph (.+)$", text, re.M)]
    want = [_argv(tasks[tid]["cmd"])[4:] for tid in EXPECTED_CHAIN[1:]]
    assert calls == want
    assert "/opt/jobs/graph/01_publish_gold_graph.py" in text and "set -euo pipefail" in text
    assert "SKIP" not in text, "every step is required: a missing script must stop the chain, never be skipped"


# A stand-in for `docker compose -f ... -f ... <ps|exec> ...`: records each call; `ps` prints
# $FAKE_STATE (default running); an exec whose arguments contain $FAKE_FAIL exits 7.
FAKE_DOCKER = r"""#!/bin/bash
echo "$*" >> "$FAKE_DOCKER_LOG"
case " $* " in
  *" ps "*) echo "${FAKE_STATE:-running}"; exit 0 ;;
esac
if [[ -n "${FAKE_FAIL:-}" && " $* " == *" ${FAKE_FAIL} "* ]]; then
  exit 7
fi
exit 0
"""


@pytest.fixture
def e2e(tmp_path):
    """A repo-shaped copy of run_graph_e2e.sh with every script it runs (empty files) and a fake docker."""
    text = (REPO / "pipelines/run_graph_e2e.sh").read_text(encoding="utf-8")
    scripts = sorted({shlex.split(m)[0] for m in re.findall(r"^in_graph (.+)$", text, re.M)})
    root = tmp_path / "repo"
    (root / "pipelines").mkdir(parents=True)
    shutil.copy2(REPO / "pipelines/run_graph_e2e.sh", root / "pipelines/run_graph_e2e.sh")
    for rel in scripts:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text("")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "docker").write_text(FAKE_DOCKER)
    (bindir / "docker").chmod(0o755)
    log = tmp_path / "docker_calls.log"

    def run(**env: str) -> tuple[subprocess.CompletedProcess, list[str]]:
        e = {k: v for k, v in os.environ.items() if k not in ("GRAPH_HOST_ROOT", "OPENLINEAGE", "GRAPH_E2E_SAMPLE_DIR",
                                                              "GRAPH_E2E_EXPORT_DIR", "FAKE_STATE", "FAKE_FAIL")}
        e.update({"PATH": f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}", "FAKE_DOCKER_LOG": str(log), **env})
        log.write_text("")
        p = subprocess.run(["bash", str(root / "pipelines/run_graph_e2e.sh")], cwd=root, env=e, capture_output=True,
                           text=True, timeout=60, check=False)
        return p, [ln for ln in log.read_text().splitlines() if " exec -T " in ln]

    return root, scripts, run


def _graph_calls(execs: list[str]) -> list[list[str]]:
    return [ln.split(" exec -T graph ", 1)[1].split() for ln in execs if " exec -T graph " in ln]


def test_e2e_script_dry_run_issues_the_dag_chain_in_order(load_dag, e2e):
    tasks = load_dag("lakehouse_graph")["tasks"]
    _root, scripts, run = e2e
    p, execs = run()
    assert p.returncode == 0, p.stdout + p.stderr
    assert execs[0].endswith("exec -T spark /opt/spark/bin/spark-submit --master local[*] "
                             "/opt/jobs/graph/01_publish_gold_graph.py")
    assert _graph_calls(execs) == [_argv(tasks[tid]["cmd"])[3:] for tid in EXPECTED_CHAIN[1:]]   # python <script> ...
    assert len(execs) == len(EXPECTED_CHAIN) and "Graph E2E complete" in p.stdout
    assert {"scripts/check_graph_contract.py", "scripts/build_graph_local.py"} <= set(scripts)


@pytest.mark.parametrize("missing", ["scripts/check_graph_contract.py", "scripts/check_lineage_contract.py"])
def test_e2e_script_stops_when_a_script_is_missing(e2e, missing):
    root, _scripts, run = e2e
    (root / missing).unlink()
    p, execs = run()
    assert p.returncode == 1, p.stdout + p.stderr
    assert f"Graph E2E FAILED: {missing} is not in this checkout" in p.stderr
    calls = _graph_calls(execs)
    assert all(c[1] != missing for c in calls) and not any("promote" in c for c in calls), calls
    assert "Graph E2E complete" not in p.stdout


def test_e2e_script_stops_at_a_failing_step_and_when_a_service_is_down(e2e):
    _root, _scripts, run = e2e
    p, execs = run(FAKE_FAIL="scripts/check_graph_contract.py")
    assert p.returncode == 7
    calls = _graph_calls(execs)
    assert calls[-1][1] == "scripts/check_graph_contract.py" and not any("promote" in c for c in calls), calls
    p, execs = run(FAKE_STATE="exited")
    assert p.returncode == 1 and "Service 'spark' is not running (state: exited)" in p.stderr and execs == []


def test_container_names_follow_the_environment(load_dag, monkeypatch):
    monkeypatch.setenv("LDL_GRAPH_CONTAINER", "my-graph")
    monkeypatch.setenv("LDL_SPARK_CONTAINER", "my-spark")
    tasks = load_dag("lakehouse_graph")["tasks"]
    assert "docker exec my-spark /opt/spark/bin/spark-submit" in tasks["publish_gold_graph"]["cmd"]
    assert tasks["promote"]["cmd"].startswith("docker exec my-graph ")


def test_openlineage_default_follows_the_environment(load_dag, monkeypatch):
    monkeypatch.setenv("OPENLINEAGE", "1")
    assert load_dag("lakehouse_graph")["params"] == {"openlineage": True}


def test_operator_helper_quotes_arguments(load_dag):
    load_dag("lakehouse_graph")
    ops = sys.modules["lakehouse_graph_operators"]
    cmd = ops.docker_exec_command("ldl-graph", ["python", "-c", "print('a b'); $(id)"], {"K": "v w"})
    assert shlex.split(cmd) == ["docker", "exec", "-e", "K=v w", "ldl-graph", "python", "-c", "print('a b'); $(id)"]
    assert ops.docker_exec_command("c", ["bash", "pipelines/x.sh"]).endswith(".sh "), "no Jinja template lookup"
    with pytest.raises(ValueError, match="argv must not be empty"):
        ops.docker_exec_command("c", [])


@pytest.mark.parametrize(("module", "dag_id", "n_tasks"), [
    ("lakehouse_churn_features", "lakehouse_churn_features", 4),
    ("lakehouse_retail_medallion", "lakehouse_retail_medallion", 5),
])
def test_existing_dags_still_parse(load_dag, module, dag_id, n_tasks):
    d = load_dag(module)
    assert d["dag_id"] == dag_id and len(d["tasks"]) == n_tasks
    assert all(t["cmd"].startswith("docker exec ldl-spark /opt/spark/bin/spark-submit ") for t in d["tasks"].values())
