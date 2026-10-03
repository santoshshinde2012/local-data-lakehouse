"""Shared helpers for the agent-surface tests (no tests here).

``rich_tiny(graph_root)`` is the tiny build (built once by conftest's ``tiny_build``) copied into a graph root of its
own and completed the way the pipeline completes a served build: its exports, a strict graph-contract pass
(contract.json), the lineage graph (lineage.lbdb) and the feature cohorts (cohorts.parquet). It is made once per
process (cached), so test modules can share it without re-running the scripts. The conftest tiny build itself stays
untouched (no contract, no lineage, no cohorts): the "unavailable" paths are tested on it.
"""
from __future__ import annotations

import functools
import os
import shutil
import subprocess
import sys
from pathlib import Path

from conftest import REPO

LAUNCH = REPO / "scripts" / "graph_mcp.sh"
ASK = REPO / "scripts" / "graph_ask.sh"
HERO = "sub_santosh:2026-10-07"


def _run(*args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    p = subprocess.run([sys.executable, *args], cwd=REPO, env={**os.environ, **(env or {})}, capture_output=True,
                       text=True, check=False, timeout=600)
    assert p.returncode == 0, f"{args}: {p.stdout[-2000:]}\n{p.stderr[-2000:]}"
    return p


@functools.cache
def rich_tiny(graph_root: str) -> tuple[Path, Path]:
    """(graph root, build dir) of a strict-contract tiny build with lineage and cohorts, beside ``graph_root``."""
    src_root = Path(graph_root)
    src = (src_root / "tiny" / "builds").resolve()
    build_src = sorted(p for p in src.iterdir() if p.is_dir() and not p.name.startswith("."))[-1]
    root = src_root.parent / (src_root.name + "-rich")
    if root.exists():
        shutil.rmtree(root)
    build = root / "tiny" / "builds" / build_src.name
    shutil.copytree(build_src, build, ignore=shutil.ignore_patterns(".pids", "contract.json"))
    shutil.copytree(src_root / "tiny" / "export", root / "tiny" / "export")
    os.symlink(f"builds/{build.name}", root / "tiny" / "latest")
    env = {"GRAPH_ROOT": str(root), "PYTHONPATH": str(REPO / "src")}
    _run("scripts/check_graph_contract.py", "--profile", "tiny", "--graph-root", str(root), "--strict", env=env)
    _run("scripts/build_lineage_local.py", "--build", str(build), "--graph-root", str(root), env=env)
    _run("scripts/build_graph_cohorts.py", "build", "--build", str(build), env=env)
    return root.resolve(), build.resolve()


@functools.cache
def evidence_tiny(graph_root: str) -> tuple[Path, Path]:
    """(graph root, build dir) of a copy of the tiny build, beside ``graph_root``, with its evidence graph built
    (scripts/build_evidence_graph.py): evidence/, evidence.lbdb, evidence.json and manifest.json["evidence"]."""
    src_root = Path(graph_root)
    src = (src_root / "tiny" / "builds").resolve()
    build_src = sorted(p for p in src.iterdir() if p.is_dir() and not p.name.startswith("."))[-1]
    root = src_root.parent / (src_root.name + "-evidence")
    if root.exists():
        shutil.rmtree(root)
    build = root / "tiny" / "builds" / build_src.name
    shutil.copytree(build_src, build, ignore=shutil.ignore_patterns(".pids", "contract.json", "evidence*"))
    shutil.copytree(src_root / "tiny" / "export", root / "tiny" / "export")
    os.symlink(f"builds/{build.name}", root / "tiny" / "latest")
    (root / "logs").mkdir()
    _run("scripts/build_evidence_graph.py", "--build", str(build), env={"GRAPH_ROOT": str(root),
                                                                         "PYTHONPATH": str(REPO / "src")})
    return root.resolve(), build.resolve()


def unchecked_copy(build: Path, root: Path) -> Path:
    """A copy of ``build`` inside the graph root ``root`` WITHOUT contract.json (other tests may have run the graph
    contract on the shared conftest build, so its contract state depends on test order)."""
    dest = root / "tiny" / "builds" / build.name
    if not dest.exists():
        shutil.copytree(build, dest, ignore=shutil.ignore_patterns(".pids", "contract.json"))
    return dest


def launcher_env(graph_root: Path, **extra: str) -> dict[str, str]:
    """A minimal caller environment for scripts/graph_mcp.sh (it scrubs everything else itself)."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/"),
           "GRAPH_ROOT": str(graph_root), "GRAPH_PY": sys.executable}
    env.update(extra)
    return env


def write_stub_python(path: Path, env_dump: Path) -> Path:
    """An executable GRAPH_PY stand-in: answers the launcher's `-I -c` interpreter query with the real
    interpreter, otherwise writes its environment and argv to ``env_dump`` and exits 0 (never serves)."""
    path.write_text(
        "#!/bin/sh\n"
        f'if [ "$1" = "-I" ]; then exec "{sys.executable}" "$@"; fi\n'
        f'env > "{env_dump}"\n'
        f'printf "ARGV %s\\n" "$*" >> "{env_dump}"\n'
        f'printf "PID %s\\n" "$$" >> "{env_dump}"\n'
        "exit 0\n", encoding="utf-8")
    path.chmod(0o755)
    return path
