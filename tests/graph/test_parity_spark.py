"""scripts/check_graph_parity.py end to end (local PySpark 3.5 + JDK 17, one JVM per subprocess).

* tiny: the hard gate passes (every node / edge table equal, SIMILAR_TO bit-identical with the
  persisted scaler on the same pandas input), the SQL-fitted scaler and the Spark-gold input are
  reported and tie-only, and the Spark-vs-pandas gold drift is measured and recorded;
* s42 (slow, N = 8000): the same at full scale, 80,010 SIMILAR_TO edges;
* the real job file under spark-submit (slow): publishes into a local Iceberg lakehouse, a second
  run finds the publish complete and writes nothing.

Skipped with the reason without pyspark / a JDK 17 (and the Iceberg jars for the lakehouse test);
GRAPH_REQUIRE_SPARK=1 turns the skip into a failure.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PARITY = REPO / "scripts/check_graph_parity.py"
sys.path.insert(0, str(REPO / "src"))


def _harness():
    mspec = importlib.util.spec_from_file_location("check_graph_parity_skip", PARITY)
    mod = importlib.util.module_from_spec(mspec)
    prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        mspec.loader.exec_module(mod)
    finally:
        sys.dont_write_bytecode = prev
    return mod


H = _harness()
REQUIRE = os.environ.get("GRAPH_REQUIRE_SPARK") == "1"


def _need(iceberg: bool) -> None:
    reason = H.skip_reason(iceberg=iceberg)
    if reason and REQUIRE:
        pytest.fail(f"GRAPH_REQUIRE_SPARK=1 but the spark harness is unavailable: {reason}")
    if reason:
        pytest.skip(f"spark harness unavailable: {reason}")


def _run(*args: str, timeout: int = 1800) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, str(PARITY), *args], cwd=REPO, env=env, capture_output=True, text=True,
                          timeout=timeout, check=False)


def _parity(tmp_path: Path, *args: str) -> dict:
    out = tmp_path / "parity.json"
    p = _run("parity", *args, "--json", str(out), "--scratch", str(tmp_path))
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-3000:]
    assert "Graph parity OK" in p.stdout
    return json.loads(out.read_text())


def _check_report(r: dict, n_similar: int) -> None:
    assert r["errors"] == [] and r["warnings"] == []
    assert all(not t["columns"] for t in r["hard_gate"]["tables"].values()), "hard gate: every table equal"
    gate = r["hard_gate"]["similar_to"]
    assert gate["edges"] == [n_similar, n_similar]
    assert (gate["only_local"], gate["only_twin"], gate["rank_differences"], gate["d2_bits_differ"]) == (0, 0, 0, 0)
    fitted = r["fitted_scaler"]
    assert fitted["similar_to"]["tie_only"] and fitted["scaler"]["features"] == 20
    assert fitted["similar_to"]["max_abs_d2_delta"] < 1e-9, "a refitted scaler moves d2 in the last bits only"
    drift = r["spark_gold"]["drift"]
    assert drift["label_cells_differ"] == {} and drift["max_abs_delta"] <= 1.0001e-4
    assert r["spark_gold"]["similar_to"]["tie_only"]


def test_parity_tiny(tmp_path):
    _need(iceberg=False)
    r = _parity(tmp_path, "--profile", "tiny")
    _check_report(r, 1182)
    assert r["spark_gold"]["drift"]["cells_differ"] == 0, "tiny: Spark gold is bit-identical to the pandas twin"


@pytest.mark.slow
def test_parity_s42(tmp_path, s42_build, graph_root):
    _need(iceberg=False)
    r = _parity(tmp_path, "--profile", "s42", "--graph-root", str(graph_root))
    _check_report(r, 80010)
    # Measured and recorded (not gated): Spark SQL gold vs the pandas twin at seed 42.
    print("seed-42 Spark-vs-pandas gold drift:", json.dumps(r["spark_gold"]["drift"]["by_feature"]))


@pytest.mark.slow
def test_spark_submit_job_file_publishes_and_is_idempotent(tmp_path):
    _need(iceberg=True)
    root = tmp_path / "lake"
    p = _run("lakehouse", "--root", str(root), "--profile", "tiny", "--spark-submit")
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr[-3000:]
    first = json.loads(next(ln for ln in p.stdout.splitlines() if ln.startswith("GRAPH_LAKEHOUSE ")).split(" ", 1)[1])
    assert first["publish"]["status"] == "published"
    assert sum(first["publish"]["nodes"].values()) == 616 and sum(first["publish"]["edges"].values()) == 1949
    p = _run("lakehouse", "--root", str(root), "--graph-only", "--spark-submit")
    assert p.returncode == 0, p.stderr[-3000:]
    again = json.loads(next(ln for ln in p.stdout.splitlines() if ln.startswith("GRAPH_LAKEHOUSE ")).split(" ", 1)[1])
    assert again["publish"]["status"] == "already_published"
    assert again["publish"]["tag"] == first["publish"]["tag"]
    assert again["publish"]["tables"] == first["publish"]["tables"]
