"""Tier-1 Iceberg lineage facts from a real local lakehouse, Docker-free (spark venv).

The lakehouse is the 2c harness's (scripts/check_graph_parity.py): local PySpark 3.5 + Iceberg
JdbcCatalog(SQLite) named ``lakehouse`` + a file warehouse. One Spark session for the module loads the
tiny fixture's bronze -> silver -> gold with the user's churn jobs (reused unchanged) and publishes the
graph twin with its tags; then the whole medallion is published again (every table createOrReplace'd:
a second snapshot with no parent id) and the graph job publishes a second time (new pins, new tag; the
manifest table is appended to, so its parent id chains). The business graph is built from the newest
publish with PyIceberg pinned by tag + snapshot. Then lakehouse_graph.lineage.iceberg_facts reads the
catalog's metadata and the lineage build adds the Tier-1 facts:

  * only ``.snapshots()`` and ``.refs()`` are read (history / inspect / scan raise if touched);
  * the catalog file is byte-identical and its schema unchanged afterwards (no ALTER, no write);
  * Snapshot / Ref nodes with HAS_SNAPSHOT / POINTS_TO / SUPERSEDES (by sequence number: the replaced
    tables' parents are cut) / PRODUCED_BY_RUN, and the business build's CONSUMED_SNAPSHOT edges to
    exactly the snapshots its manifest pins;
  * scripts/build_lineage_local.py --iceberg (explicit catalog, and the SQLite catalog the build
    records) and the strict lineage contract;
  * the safety rules refuse a schema_version, a missing catalog file (nothing created);
  * a new tag re-keys lineage_build_id.

Skipped (with the reason) without pyspark, a JDK 17 or 21 or the two jars; GRAPH_REQUIRE_SPARK=1 turns the
skip into a failure. Run under the heavy lock (one JVM, local[2], 1 GB driver).
"""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
pytest.importorskip("pyiceberg", reason="pyiceberg is in requirements-graph-spark*.txt (.venv-graph-spark)")

from lakehouse_graph import iceberg_source as ice  # noqa: E402 (after the sys.path line and the importorskip)
from lakehouse_graph import spec as gspec  # noqa: E402
from lakehouse_graph.lineage import build as lbuild  # noqa: E402
from lakehouse_graph.lineage import graph as lg  # noqa: E402
from lakehouse_graph.lineage import iceberg_facts, spec  # noqa: E402


def _load_parity():
    mspec = importlib.util.spec_from_file_location("check_graph_parity_lineage", REPO / "scripts/check_graph_parity.py")
    mod = importlib.util.module_from_spec(mspec)
    prev, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        mspec.loader.exec_module(mod)
    finally:
        sys.dont_write_bytecode = prev
    return mod


H = _load_parity()
SKIP = H.skip_reason(iceberg=True)
REQUIRE = os.environ.get("GRAPH_REQUIRE_SPARK") == "1"
pytestmark = pytest.mark.skipif(bool(SKIP) and not REQUIRE, reason=f"spark harness unavailable: {SKIP}")
TINY = REPO / gspec.TINY_FIXTURE
GOLD = spec.GOLD_TABLE
MANIFEST = "lakehouse.gold.graph_build_manifest"


def _quiet(*_args) -> None:
    return None


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sqlite_schema(path: Path) -> list:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return con.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
    finally:
        con.close()


class Lake:
    def __init__(self, root: Path, spark, first: dict, second: dict):
        self.root, self.spark, self.first, self.second = root, spark, first, second
        self.db, self.warehouse = root / "catalog.db", root / "warehouse"
        self.uri, self.wh = f"sqlite:///{self.db}", f"file://{self.warehouse}"

    def tables(self) -> list[str]:
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        try:
            return sorted(f"lakehouse.{ns}.{t}" for ns, t in con.execute(
                "SELECT table_namespace, table_name FROM iceberg_tables WHERE catalog_name = 'lakehouse'"))
        finally:
            con.close()


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    if SKIP:
        pytest.fail(f"GRAPH_REQUIRE_SPARK=1 but the spark harness is unavailable: {SKIP}")
    root = tmp_path_factory.mktemp("lineage_lakehouse")
    spark = H.build_session(root / "spark", root / "catalog.db", root / "warehouse",
                            app_name="pytest_lineage_iceberg", driver_memory="1g")
    try:
        first = H.publish_local(spark, TINY)["publish"]
        H.spark_silver_gold(spark, TINY, to_iceberg=True)   # every medallion table replaced: no parent id
        second = H.graph_job().publish(spark)               # new pins -> a second publish and tag
        yield Lake(root, spark, first, second)
    finally:
        spark.stop()


@pytest.fixture(scope="module")
def business(lake, tmp_path_factory) -> tuple[Path, dict]:
    """(build dir, business manifest) of the tiny graph built from the newest publish."""
    return ice.build_from_iceberg("tiny", tmp_path_factory.mktemp("graph_root_lineage"), catalog_uri=lake.uri,
                                  warehouse=lake.wh, log=_quiet)


@pytest.fixture(scope="module")
def facts(lake) -> dict:
    return iceberg_facts.load_facts(lake.uri, lake.wh, log=_quiet)


# --------------------------------------------------------------------------- reading
def test_facts_come_from_snapshots_and_refs_only_and_leave_the_catalog_alone(lake, monkeypatch):
    from pyiceberg.table import Table

    def forbidden(*_a, **_k):
        raise AssertionError("the Tier-1 reader must not read the snapshot log, inspect tables or scan data")

    monkeypatch.setattr(Table, "history", forbidden)
    monkeypatch.setattr(Table, "scan", forbidden)
    monkeypatch.setattr(Table, "inspect", property(forbidden))
    digest, schema = _sha(lake.db), _sqlite_schema(lake.db)
    facts = iceberg_facts.load_facts(lake.uri, lake.wh, log=_quiet)
    assert _sha(lake.db) == digest and _sqlite_schema(lake.db) == schema, "the catalog was written to"
    assert facts["catalog_schema_unchanged"] is True and facts["catalog_uri"].startswith("sqlite:///file:")
    assert sorted(facts["tables"]) == lake.tables() and len(facts["tables"]) == 25
    gold = facts["tables"][GOLD]
    first, second = gold["snapshots"]
    assert second["parent_id"] is None and second["sequence_number"] > first["sequence_number"]   # cut by REPLACE
    assert gold["current_snapshot_id"] == second["snapshot_id"] and gold["format_version"] == 2
    tags = {r["name"]: r["snapshot_id"] for r in gold["refs"] if r["kind"] == "tag"}
    assert tags == {lake.first["tag"]: lake.first["tables"][GOLD], lake.second["tag"]: lake.second["tables"][GOLD]}
    assert [r["name"] for r in gold["refs"] if r["kind"] == "branch"] == ["main"]
    man = facts["tables"][MANIFEST]["snapshots"]
    assert [s["operation"] for s in man] == ["append", "append"] and man[1]["parent_id"] == man[0]["snapshot_id"]
    assert all(s["summary"].get("spark.app.id", "").startswith("local-") for t in facts["tables"].values()
               for s in t["snapshots"])


def test_the_reader_has_no_path_to_the_snapshot_log_or_the_data():
    tree = ast.parse((REPO / "src/lakehouse_graph/lineage/iceberg_facts.py").read_text())
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not attrs & {"history", "snapshot_log", "metadata_log", "inspect", "scan", "to_arrow", "plan_files"}


# --------------------------------------------------------------------------- the overlay in a lineage build
def test_lineage_build_adds_snapshots_refs_supersedes_and_the_consumed_pins(lake, business, facts, tmp_path):
    bdir, bman = business
    _dir, man = lbuild.build_lineage(bdir, "core", overlays=[iceberg_facts.overlay(facts, bman)], rebuild=True,
                                     log=_quiet)
    assert man["unresolved"] == [] and man["environment_warnings"] == []   # every lakehouse table is in the code
    c = man["counts"]
    n_snaps = sum(len(t["snapshots"]) for t in facts["tables"].values())
    replaced = sum(len(t["snapshots"]) > 1 for t in facts["tables"].values())
    assert c["nodes"]["Snapshot"] == c["edges"]["HAS_SNAPSHOT"] == c["edges"]["PRODUCED_BY_RUN"] == n_snaps
    assert c["edges"]["SUPERSEDES"] == n_snaps - len(facts["tables"]) and replaced >= 21
    assert c["nodes"]["Ref"] == c["edges"]["POINTS_TO"] == sum(len(t["refs"]) for t in facts["tables"].values())
    pins = bman["iceberg"]["tables"]
    assert c["edges"]["CONSUMED_SNAPSHOT"] == len(pins) == 14
    g = lg.assemble(REPO, "core", overlays=[iceberg_facts.overlay(facts, bman)])
    sup = {e["src"]: e["props"] for e in g.edges if e["rel"] == "SUPERSEDES"}
    gold_now = f"snap:{GOLD}@{facts['tables'][GOLD]['current_snapshot_id']}"
    assert sup[gold_now]["parent_matches"] is False and sup[gold_now]["ordered_by"] == "sequence_number"
    last = f"snap:{MANIFEST}@{facts['tables'][MANIFEST]['current_snapshot_id']}"
    assert sup[last]["parent_matches"] is True                       # an append keeps its parent
    run = f"run:graph-build:{bman['business_build_id']}"
    consumed = {e["dst"]: e["props"] for e in g.edges if e["rel"] == "CONSUMED_SNAPSHOT"}
    assert consumed == {f"snap:{t}@{p['snapshot_id']}": {"role": p["role"], "tag": lake.second["tag"],
                                                         "n_rows": p["rows"]} for t, p in pins.items()}
    assert all(e["src"] == run for e in g.edges if e["rel"] == "CONSUMED_SNAPSHOT")
    tag_ref = f"ref:{GOLD}@{lake.second['tag']}"
    assert [e["dst"] for e in g.edges if e["rel"] == "POINTS_TO" and e["src"] == tag_ref] == \
        [f"snap:{GOLD}@{pins[GOLD]['snapshot_id']}"]                # the tag points at what the build read
    ran = {e["src"]: e["dst"] for e in g.edges if e["rel"] == "RAN_AS"}
    assert ran[run] == f"job:{spec.GRAPH_BUILD_SCRIPT}"
    assert ran[f"run:spark:{bman['iceberg']['spark_app_id']}"] == f"job:{spec.GRAPH_SPARK_JOB}"
    assert {g.props(e["dst"])["spark_app_id"] for e in g.edges if e["rel"] == "PRODUCED_BY_RUN"} == \
        {lake.spark.sparkContext.applicationId}                      # one in-process session wrote them all


def test_cli_iceberg_overlay_and_the_strict_contract(lake, business, tmp_path):
    bdir, _bman = business
    digest, schema = _sha(lake.db), _sqlite_schema(lake.db)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYICEBERG_")}
    env["PYTHONPATH"] = str(REPO / "src")
    build = subprocess.run([sys.executable, str(REPO / "scripts/build_lineage_local.py"), "--build", str(bdir),
                            "--iceberg", "--catalog-uri", lake.uri, "--warehouse", lake.wh, "--rebuild"],
                           env=env, capture_output=True, text=True, cwd=REPO, check=False)
    assert build.returncode == 0, build.stderr[-3000:]
    assert "Tier 1: Iceberg lakehouse: 25 tables" in build.stdout and "graph build read 14 snapshots at " \
        f"{lake.second['tag']}" in build.stdout
    out = tmp_path / "contract.json"
    check = subprocess.run([sys.executable, str(REPO / "scripts/check_lineage_contract.py"), "--build", str(bdir),
                            "--strict", "--json", str(out)], env=env, capture_output=True, text=True, cwd=REPO,
                           check=False)
    assert check.returncode == 0, check.stdout[-3000:] + check.stderr[-2000:]
    for line in ("tier facts: Cypher = oracle (25 tables", "its own schema was the same before and after",
                 "snapshots the graph build", "oracle = golden core.json"):
        assert line in check.stdout, line
    doc = json.loads(out.read_text())
    assert doc["status"] == "pass" and doc["overlays"] == ["iceberg"] and doc["warnings"] == []
    assert doc["tiers"]["tables"][GOLD]["parent_cut"] == 1 and doc["tiers"]["dangling_refs"] == []
    assert _sha(lake.db) == digest and _sqlite_schema(lake.db) == schema
    # without --catalog-uri / PYICEBERG_*: the SQLite catalog the business build records
    again = subprocess.run([sys.executable, str(REPO / "scripts/build_lineage_local.py"), "--build", str(bdir),
                            "--iceberg"], env=env, capture_output=True, text=True, cwd=REPO, check=False)
    assert again.returncode == 0 and "unchanged" in again.stdout, again.stderr[-2000:]


def test_safety_rules_refuse_before_reading(lake, tmp_path):
    with pytest.raises(ice.ProvenanceUnavailable, match="schema_version"):
        iceberg_facts.load_facts(lake.uri, lake.wh, {"schema_version": "1"}, log=_quiet)
    missing = tmp_path / "nope.db"
    with pytest.raises(ice.ProvenanceUnavailable, match="does not exist"):
        iceberg_facts.load_facts(f"sqlite:///{missing}", lake.wh, log=_quiet)
    assert not missing.exists()
    p = subprocess.run([sys.executable, str(REPO / "scripts/build_lineage_local.py"), "--build", str(tmp_path / "b"),
                        "--iceberg", "--catalog-uri", f"sqlite:///{missing}", "--warehouse", lake.wh],
                       env={**os.environ, "PYTHONPATH": str(REPO / "src")}, capture_output=True, text=True, cwd=REPO,
                       check=False)
    assert p.returncode == 1 and "Lineage build FAILED: --iceberg: provenance unavailable" in p.stderr
    assert not missing.exists() and not (tmp_path / "b" / spec.LINEAGE_DIR).exists()


def test_a_new_tag_re_keys_the_lineage_build_id(lake, facts):
    def lid(f) -> str:
        return lbuild.lineage_identity(lg.assemble(REPO, "core", overlays=[iceberg_facts.overlay(f)]))[
            "lineage_build_id"]

    before = lid(facts)
    lake.spark.sql(f"ALTER TABLE {GOLD} CREATE TAG lineage_probe")
    after = iceberg_facts.load_facts(lake.uri, lake.wh, log=_quiet)
    assert {r["name"] for r in after["tables"][GOLD]["refs"]} - {r["name"] for r in facts["tables"][GOLD]["refs"]} \
        == {"lineage_probe"}
    assert lid(after) != before and lid(facts) == before
