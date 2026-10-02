"""scripts/build_graph_local.py build --source iceberg without the Iceberg client installed.

The core venv (requirements-graph.txt) has no pyiceberg / SQLAlchemy: the CLI must fail with the
repo's one-line "Graph build FAILED: ..." naming the missing module and where it is installed, exit
1, and write nothing. Runs in every venv: the import is made to fail on purpose (sys.modules[name] =
None makes `import name` raise ModuleNotFoundError), so the spark venv checks the same path.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
CLI = REPO / "scripts/build_graph_local.py"
RUN_WITHOUT = ("import runpy, sys; sys.modules[{mod!r}] = None; sys.argv = {argv!r}; "
               "runpy.run_path({cli!r}, run_name='__main__')")


def _cli_without(module: str, graph_root: Path) -> subprocess.CompletedProcess:
    # An existing (empty) SQLite catalog file: the missing-file check runs before the catalog import,
    # so a nonexistent path would stop the build before it ever needs sqlalchemy.
    graph_root.mkdir(parents=True, exist_ok=True)
    db = graph_root.parent / "catalog.db"
    sqlite3.connect(db).close()
    argv = [str(CLI), "build", "--profile", "tiny", "--graph-root", str(graph_root), "--source", "iceberg",
            "--catalog-uri", f"sqlite:///{db}"]
    code = RUN_WITHOUT.format(mod=module, argv=argv, cli=str(CLI))
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False, cwd=REPO)


@pytest.mark.parametrize("module", ["pyiceberg", "sqlalchemy"])
def test_source_iceberg_without_the_client_fails_in_one_line(module, tmp_path):
    if module == "sqlalchemy" and importlib.util.find_spec("pyiceberg") is None:
        pytest.skip("without pyiceberg the build stops at pyiceberg first (the case above)")
    p = _cli_without(module, tmp_path / "graph")
    assert p.returncode == 1, p.stdout + p.stderr
    assert "Traceback" not in p.stderr, p.stderr
    lines = p.stderr.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith(f"Graph build FAILED: --source iceberg needs {module},"), p.stderr
    assert ".venv-graph-spark/bin/python" in lines[0] and "ldl-graph" in lines[0]
    assert not (tmp_path / "graph" / "tiny" / "builds").exists(), "nothing is built or written"


def test_the_core_venv_says_the_same_without_forcing_it(tmp_path):
    if importlib.util.find_spec("pyiceberg") is not None:
        pytest.skip("pyiceberg is installed in this venv (the forced case above covers it)")
    p = subprocess.run([sys.executable, str(CLI), "build", "--profile", "tiny", "--graph-root", str(tmp_path / "g"),
                        "--source", "iceberg", "--catalog-uri", "sqlite:////nonexistent/catalog.db"],
                       capture_output=True, text=True, check=False, cwd=REPO)
    assert p.returncode == 1 and p.stderr.startswith("Graph build FAILED: --source iceberg needs pyiceberg,"), \
        p.stderr
