"""scripts/graph_leakage_demo.py: neighbour lapse-rate leakage as AUC, numpy only.

* the numpy pieces: AUC with tied scores, the L2 logistic regression, stratified folds;
* on the tiny build: every variant is reported (baseline, self-inclusive, as of today, temporally safe, random),
  the JSON is what scripts/graph_charts.py draws, no plan comparison off seed 42;
* on seed 42 (slow): the single-feature trio reproduces the plan exactly and every figure is within +-0.02
  (reported, not pinned: the test checks the tolerance the task sets, not a golden);
* a graph root without a build exits 3.
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys

import numpy as np
import pytest
from conftest import REPO
from test_tools_support import rich_tiny

from lakehouse_graph import charts


def _load():
    spec = importlib.util.spec_from_file_location("graph_leakage_demo", REPO / "scripts" / "graph_leakage_demo.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["graph_leakage_demo"] = mod
    spec.loader.exec_module(mod)
    return mod


ld = _load()


def test_auc_and_logistic_regression_in_numpy():
    y = np.array([0, 0, 1, 1])
    assert ld.auc(np.array([0.1, 0.2, 0.8, 0.9]), y) == 1.0
    assert ld.auc(np.array([0.9, 0.8, 0.2, 0.1]), y) == 0.0
    assert ld.auc(np.array([0.5, 0.5, 0.5, 0.5]), y) == 0.5, "ties count half"
    assert ld.auc(np.array([0.1, 0.5, 0.5, 0.9]), y) == 0.875
    rng = np.random.default_rng(0)
    x = rng.normal(size=(2000, 2))
    yy = (x[:, 0] + 0.2 * rng.normal(size=2000) > 0).astype(float)
    w = ld.fit_logistic(x, yy)
    assert w[1] > 2 and abs(w[2]) < 0.5 and ld.auc(ld.predict(w, x), yy) > 0.95
    folds = ld.stratified_folds(yy, 5, np.random.default_rng(1))
    assert sorted(np.concatenate(folds).tolist()) == list(range(2000))
    assert max(abs(yy[f].mean() - yy.mean()) for f in folds) < 0.01


@pytest.mark.slow
def test_demo_on_tiny_reports_every_variant(graph_root, tiny_build, tmp_path):
    root, build = rich_tiny(str(graph_root))
    out = tmp_path / "leak.json"
    p = subprocess.run([sys.executable, str(REPO / "scripts" / "graph_leakage_demo.py"), "--build", str(build),
                        "--json", str(out), "--md", str(tmp_path / "leak.md"), "--repeats", "1"], cwd=REPO,
                       capture_output=True, text=True, check=False, timeout=600)
    assert p.returncode == 0, p.stderr[-2000:]
    rep = json.loads(out.read_text())
    keys = [v["key"] for v in rep["variants"]]
    assert keys == ["baseline", "self_inclusive", "as_of_today", "temporally_safe", "random_graph"]
    for v in rep["variants"]:
        assert v["label"] and 0 <= v["lr"] <= 1
        assert v["single_feature"] is None if v["key"] == "baseline" else 0 <= v["single_feature"] <= 1
    assert rep["within_tolerance"]["all"] is None, "the plan figures are seed 42 only"
    assert charts.leakage_data(rep)["variants"] == rep["variants"]
    assert "self-inclusive" in (tmp_path / "leak.md").read_text()


@pytest.mark.slow
def test_seed_42_is_within_two_points_of_the_plan(s42_build):
    rep = ld.run(s42_build[0], folds=5, repeats=3, seed=42)
    single = {v["key"]: v["single_feature"] for v in rep["variants"]}
    assert single["self_inclusive"] == pytest.approx(0.8525, abs=1e-4)
    assert single["as_of_today"] == pytest.approx(0.6158, abs=1e-4)
    assert single["temporally_safe"] == pytest.approx(0.5487, abs=1e-4)
    assert rep["within_tolerance"]["all"] is True, rep["deltas"]
    lr = {v["key"]: v["lr"] for v in rep["variants"]}
    assert lr["self_inclusive"] - lr["baseline"] > 0.1, "the self-inclusive leak is large"
    assert abs(lr["temporally_safe"] - lr["baseline"]) < 0.01, "the temporally safe rate adds nothing"


def test_demo_without_a_build_exits_3(tmp_path):
    p = subprocess.run([sys.executable, str(REPO / "scripts" / "graph_leakage_demo.py"), "--graph-root",
                        str(tmp_path / "empty")], cwd=REPO, capture_output=True, text=True, check=False)
    assert p.returncode == 3 and "SKIP" in p.stderr
