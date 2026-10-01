"""The macOS OS sandbox around the servers: scripts/graph_sandbox_check.py run end to end on a tiny build
(COPY TO /tmp, LOAD FROM a fake ~/.ssh, outbound TCP and the rest denied while the tools answer; fail closed
without sandbox-exec). macOS only; the static profile / launcher lint runs everywhere (test_launchers.py).
"""
from __future__ import annotations

import json
import subprocess
import sys

import pytest
from conftest import REPO
from test_tools_support import rich_tiny

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="sandbox-exec is macOS only")


def test_check_script_skips_politely_off_macos(monkeypatch):
    import importlib.util

    spec = importlib.util.spec_from_file_location("graph_sandbox_check_under_test",
                                                  REPO / "scripts/graph_sandbox_check.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod.sys, "platform", "linux")
    assert mod.main([]) == 0


@pytest.mark.slow
def test_sandbox_holds_while_the_tools_answer(graph_root, tiny_build, tmp_path):
    root, build = rich_tiny(str(graph_root))
    report = tmp_path / "report.json"
    p = subprocess.run([sys.executable, str(REPO / "scripts/graph_sandbox_check.py"), "--build", str(build),
                        "--graph-root", str(root), "--json", str(report), "--control"],
                       capture_output=True, text=True, timeout=900, check=False, cwd=REPO)
    assert p.returncode == 0, p.stdout[-4000:] + p.stderr[-2000:]
    doc = json.loads(report.read_text())
    names = [c["name"] for c in doc["checks"]]
    assert all(c["ok"] for c in doc["checks"])
    for needle in ("writes outside the logs dir denied (Python, COPY TO /tmp",
                   "fake credentials outside the readable trees denied (Python + LOAD FROM ~/.ssh",
                   "outbound TCP (1.1.1.1, IPv6, loopback)", "runtime basics and the lakehouse tools still work",
                   "sandbox-exec missing (simulated): exit 78", "GRAPH_SANDBOX=0 is the only opt-out",
                   "graph/legacy:", "graph/2026-07-28:", "lineage/2026-07-28:", "cohorts/legacy:"):
        assert any(needle in n for n in names), needle
    works = {r["probe"]: r for r in doc["probe_a"]["results"] if r["group"] == "works"}
    assert works["lakehouse_graph tools answer"]["detail"].startswith("6/6 tools answered")
    net = {r["probe"]: r["outcome"] for r in doc["probe_a"]["results"] if r["group"] == "net"}
    assert net["tcp 1.1.1.1:53"] == "denied" and net["dns getaddrinfo example.com"] == "denied"
    # the unsandboxed control stays on this machine unless --control-network asks for the internet
    assert any("reaches for the internet only with --control-network" in n for n in names)
    control = [r["probe"] for r in doc["probe_control"]["results"]]
    assert "bind+listen 127.0.0.1:0" in control
    assert not [x for x in control if "example.com" in x or "1.1.1.1" in x or "2606:" in x], control
    assert not list((root / ".sandbox-check").glob("*")) and not list((root / "logs").glob("probe-*"))
