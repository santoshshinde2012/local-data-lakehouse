"""scripts/graph_bench.py: build time, max RSS, load time, file sizes, server start, warm p50 / p95 per tool.

* on the tiny build: a fresh build in a scratch graph root (the served build is never touched), the four servers
  through the sandboxed launcher, every tool timed in process and over stdio;
* the JSON has the shape of docs/graph/results/tools-bench-<profile>.json, so scripts/graph_charts.py reads it;
* --no-build reads the build figures from the served build's manifest and says so;
* a graph root without a build exits 3 (nothing measured), like scripts/graph_charts.py.
"""
from __future__ import annotations

import json
import subprocess
import sys

import pytest
from conftest import REPO
from test_tools_support import rich_tiny

from lakehouse_graph import charts, tools


def _bench(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(REPO / "scripts" / "graph_bench.py"), *args], cwd=REPO,
                          capture_output=True, text=True, check=False, timeout=900)


@pytest.mark.slow
def test_bench_on_tiny_matches_the_tools_bench_schema(graph_root, tiny_build, tmp_path):
    root, build = rich_tiny(str(graph_root))
    before = sorted(p.name for p in (root / "tiny" / "builds").iterdir())
    out = tmp_path / "bench.json"
    p = _bench("--graph-root", str(root), "--profile", "tiny", "--calls", "3", "--json", str(out),
               "--md", str(tmp_path / "bench.md"), "--scratch", str(tmp_path / "scratch"))
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr[-2000:]
    rep = json.loads(out.read_text())
    assert {"bench", "bench_calls", "build_id", "commit", "generated_by", "platform", "profile"} <= set(rep)
    assert set(rep["bench"]) >= {"in_process", "stdio", "rss_kb"}
    assert set(rep["bench"]["stdio"]) == set(tools.SPECS), "every tool timed over stdio"
    assert all(v["p50"] <= v["p95"] for v in rep["bench"]["stdio"].values())
    assert set(rep["bench"]["rss_kb"]) == set(tools.TOOLSETS) and all(rep["bench"]["rss_kb"].values())
    assert set(rep["server_start_s"]) == set(tools.TOOLSETS)
    b = rep["build"]
    assert b["ok"] and b["source"].startswith("fresh build") and b["wall_s"] > 0 and b["builder_max_rss_mib"] > 0
    assert b["load_s"] is not None and b["graph_lbdb_bytes"] > 0 and b["nodes"] == 616 and b["edges"] == 1949
    assert rep["files"]["graph.lbdb"] > 0 and rep["files"]["parquet/"] > 0
    data = charts.latency_data(rep)
    assert data["mode"] == "stdio" and len(data["tools"]) == len(tools.SPECS)
    assert sorted(p.name for p in (root / "tiny" / "builds").iterdir()) == before, "the served root is untouched"
    assert "| graph_find |" in (tmp_path / "bench.md").read_text()


@pytest.mark.slow
def test_bench_no_build_reads_the_manifest(graph_root, tiny_build, tmp_path):
    root, build = rich_tiny(str(graph_root))
    out = tmp_path / "b.json"
    p = _bench("--graph-root", str(root), "--profile", "tiny", "--calls", "2", "--no-build", "--toolsets", "metrics",
               "--json", str(out))
    assert p.returncode == 0, p.stderr[-2000:]
    rep = json.loads(out.read_text())
    man = json.loads((build / "manifest.json").read_text())
    assert rep["build"]["source"].startswith("manifest") and rep["build"]["builder_s"] == man["builder"]["seconds"]
    assert set(rep["bench"]["stdio"]) == {s.name for s in tools.TOOLSETS["metrics"]}


def test_bench_without_a_build_exits_3(tmp_path):
    p = _bench("--graph-root", str(tmp_path / "empty"))
    assert p.returncode == 3 and "SKIP" in p.stderr
