"""Shared fixtures for the graph tests (run with the graph venv: `.venv-graph/bin/python -m pytest tests/graph`).

* Every build goes into a temporary GRAPH_ROOT, never into data/graph.
* The tiny profile (committed fixture, 121 renewals) is built once per session; tests on
  it are fast. The seed-42 N=8000 profile is generated + built once per session and its
  tests carry ``@pytest.mark.slow``, as do the few tiny tests that chain several subprocess
  runs (make targets, repeated contract checks). Slow tests still run in CI; deselect them
  with ``-m "not slow"``.
* The inject profile (the tiny bronze with one poisoned user_name, spec.INJECT_*) is prepared and
  built once per session by ``inject_build``: the fixture for injection / output-hygiene tests.
* A session-wide guard asserts that no test changed data/sample/churn/* or data/export/*.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import build, spec  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import manifest as mf  # noqa: E402


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: seed-42 N=8000 builds and end-to-end runs that chain several "
                                       "subprocesses (make, contract); all run in CI, skip with -m 'not slow'")


def run_script(script: str, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run a repo script with the current interpreter; returns the completed process (no raise)."""
    full_env = dict(os.environ, **(env or {}))
    return subprocess.run([sys.executable, str(REPO / "scripts" / script), *args], env=full_env, capture_output=True,
                          text=True, cwd=REPO, check=False)


def make_exports(sample_dir: Path, export_dir: Path) -> None:
    """The user's pandas gold script, unchanged, writing into a scratch export dir."""
    p = run_script("build_churn_gold_local.py", env={"CHURN_SAMPLE_DIR": str(sample_dir),
                                                     "CHURN_EXPORT_DIR": str(export_dir)})
    assert p.returncode == 0, p.stderr


@pytest.fixture(scope="session", autouse=True)
def user_data_untouched():
    """Non-interference: the whole test session must leave the user's bronze and exports alone."""
    before = mf.guarded_hashes()
    yield before
    after = mf.guarded_hashes()
    changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    assert not changed, f"graph tests changed guarded user files: {changed}"


@pytest.fixture(scope="session")
def graph_root(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("graph_root")


@pytest.fixture(scope="session")
def tiny_build(graph_root) -> tuple[Path, dict]:
    make_exports(spec.sample_dir("tiny", graph_root), spec.export_dir("tiny", graph_root))
    return build.build_profile("tiny", graph_root=graph_root, log=lambda *_: None)


@pytest.fixture(scope="session")
def inject_build(graph_root) -> tuple[Path, dict]:
    """(build_dir, manifest) of the inject profile: tiny + spec.INJECT_USER_NAME on spec.INJECT_SUBSCRIPTION."""
    build.prepare_inject_profile(graph_root, log=lambda *_: None)
    return build.build_profile("inject", graph_root=graph_root, log=lambda *_: None)


@pytest.fixture(scope="session")
def s42_build(graph_root) -> tuple[Path, dict]:
    sdir, edir = spec.sample_dir("s42", graph_root), spec.export_dir("s42", graph_root)
    p = run_script("generate_churn_sample.py", env={"CHURN_SAMPLE_DIR": str(sdir), "CHURN_SEED": "42",
                                                    "N_USERS": "8000"})
    assert p.returncode == 0, p.stderr
    make_exports(sdir, edir)
    return build.build_profile("s42", graph_root=graph_root, log=lambda *_: None)
