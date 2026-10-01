"""The Spark SQL twin's static contract (no Spark, no Java): sql/graph/*.sql and the publish job.

* sql/graph/similar_to.sql is exactly what scripts/check_graph_parity.py generates from the spec;
* every node / edge section of sql/graph returns the spec's columns in spec order;
* the Tier-0 lineage extractor parses every sql/graph file and the Spark job without an
  unresolved name, and sees the job's inputs and the four gold.graph_* tables it writes;
* the job stays self-contained for the ldl-spark image (pyspark + stdlib only), its constants agree
  with lakehouse_graph.spec and its appName follows the graph_<stem> convention.
"""
from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from lakehouse_graph import spec  # noqa: E402
from lakehouse_graph.lineage import extract as lx  # noqa: E402

JOB = REPO / "src/jobs/graph/01_publish_gold_graph.py"
SQL = REPO / "sql/graph"
SECTION = re.compile(r"^-- (view|table): (\w+)\s*$", re.M)


def _parity():
    mspec = importlib.util.spec_from_file_location("check_graph_parity_static", REPO / "scripts/check_graph_parity.py")
    mod = importlib.util.module_from_spec(mspec)
    prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        mspec.loader.exec_module(mod)
    finally:
        sys.dont_write_bytecode = prev
    return mod


def _sections(name: str) -> list[tuple[str, str, str]]:
    text = (SQL / name).read_text(encoding="utf-8")
    marks = list(SECTION.finditer(text))
    return [(m.group(1), m.group(2), text[m.end():(marks[i + 1].start() if i + 1 < len(marks) else len(text))]
             .strip().rstrip(";")) for i, m in enumerate(marks)]


def _consts() -> dict:
    tree = ast.parse(JOB.read_text(encoding="utf-8"))
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            try:
                out[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                continue
    return out


def _projection(sql: str) -> list[str]:
    tree = sqlglot.parse_one(re.sub(r"\$(\w+)", r"lakehouse.\1", sql), read="spark")
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    return [e.alias_or_name for e in select.expressions]


def test_similar_to_sql_is_generated_from_the_spec():
    want = _parity().render_similar_to_sql()
    assert (SQL / "similar_to.sql").read_text(encoding="utf-8") == want, \
        "run: python scripts/check_graph_parity.py sql --write"
    for f in spec.FEATURES:
        assert f"z_{f}" in want
    assert f"rank <= {spec.K}" in want and f"{spec.QUANT}D" in want
    first, last = want.index(f"(a.z_{spec.FEATURES[0]} - "), want.index(f"(a.z_{spec.FEATURES[-1]} - ")
    assert first < last, "d2 is summed in feature order"


def test_every_graph_table_section_returns_the_spec_columns_in_order():
    tables = {name: sql for f in ("nodes.sql", "edges.sql", "similar_to.sql")
              for kind, name, sql in _sections(f) if kind == "table"}
    assert set(tables) == set(spec.NODE_SCHEMA) | set(spec.EDGE_SCHEMA)
    for label, ns in spec.NODE_SCHEMA.items():
        assert _projection(tables[label]) == [c for c, _ in ns.columns], label
    for rel, es in spec.EDGE_SCHEMA.items():
        assert _projection(tables[rel]) == [c for c, _ in es.all_columns], rel


def test_job_constants_agree_with_the_spec():
    c = _consts()
    assert c["NODE_LABELS"] == tuple(spec.NODE_SCHEMA)
    assert c["EDGE_TYPES"] == tuple(spec.EDGE_SCHEMA)
    assert c["SPEC_GRAPH"] == spec.GRAPH_SPEC_VERSION and c["SPEC_SIMILAR_TO"] == spec.SIMILAR_TO_SPEC_VERSION
    assert c["SCALER_COLUMNS"] == tuple(spec.SCALER_SCHEMA.names)
    used = set()
    for f in ("nodes.sql", "edges.sql"):
        used |= set(re.findall(r"\$(?:silver|gold)\.(\w+)", (SQL / f).read_text(encoding="utf-8")))
    assert used == set(c["INPUT_TABLES"]), "every $silver / $gold table the SQL reads is pinned by the job"
    assert all(v.endswith("." + k) for k, v in c["INPUT_TABLES"].items())


def test_job_is_self_contained_for_the_spark_image():
    tree = ast.parse(JOB.read_text(encoding="utf-8"))
    roots = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    roots |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    stdlib = {"__future__", "hashlib", "json", "os", "re", "sys", "datetime", "pathlib", "string"}
    assert roots <= stdlib | {"pyspark"}, roots
    app = [n.args[0].value for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
           and n.func.attr == "appName"]
    assert app == [f"graph_{JOB.stem}"]
    assert "as_of_timestamp" not in JOB.read_text() and "TIMESTAMP AS OF" not in JOB.read_text().upper()


def test_lineage_extractor_resolves_the_job_and_parses_the_sql():
    files = lx.SourceFiles(REPO)
    job = lx.extract_job(files, str(JOB.relative_to(REPO)))
    assert job["unresolved"] == [], job["unresolved"]
    assert job["app_name"] == "graph_01_publish_gold_graph"
    reads = {r[0] for r in job["reads"]}
    assert set(_consts()["INPUT_TABLES"].values()) <= reads
    writes = {w[0] for w in job["writes"]}
    assert writes == {"lakehouse.gold.graph_nodes", "lakehouse.gold.graph_edges",
                      "lakehouse.gold.graph_similar_to_scaler", "lakehouse.gold.graph_build_manifest"}
    for f in sorted(SQL.glob("*.sql")):
        text = re.sub(r"\$\{?(\w+)\}?", r"lakehouse.\1", f.read_text(encoding="utf-8"))
        tables = lx.sql_tables(text)
        assert tables is not None, f"{f.name}: sqlglot cannot parse it (the lineage graph would lose its edges)"
        assert all(t.count(".") >= 2 for t in tables[0]), f"{f.name}: a 2-part lakehouse name {sorted(tables[0])}"


def test_readers_never_read_by_timestamp():
    text = (REPO / "src/lakehouse_graph/iceberg_source.py").read_text(encoding="utf-8")
    assert "as_of_timestamp" not in text and "snapshot_as_of_timestamp" not in text
    assert "init_catalog_tables" in text and '"false"' in text


@pytest.mark.parametrize("name", ["nodes.sql", "edges.sql", "similar_to.sql"])
def test_sql_files_use_only_the_documented_placeholders(name):
    text = (SQL / name).read_text(encoding="utf-8")
    allowed = {"silver", "gold"} if name != "similar_to.sql" else set()
    assert set(re.findall(r"\$(\w+)", text)) <= allowed
