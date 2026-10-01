"""The lakehouse-native twin end to end, Docker-free: local PySpark 3.5 + Iceberg JdbcCatalog(SQLite)
named `lakehouse` + a file warehouse (scripts/check_graph_parity.py's harness).

One Spark session for the module: the tiny fixture's bronze -> silver -> gold is loaded into Iceberg
with the user's churn jobs (reused unchanged), then src/jobs/graph/01_publish_gold_graph.py publishes
gold.graph_* and tags every table graph_<build_id>. The tests then read it back with PyIceberg
(lakehouse_graph.iceberg_source) pinned by tag + snapshot id:

  * partitioned twin tables, tags on all 15 tables at the manifest's snapshot ids;
  * a second publish of the same inputs writes nothing and leaves every tag where it was;
  * build_graph_local --source iceberg: byte-identical to the local path, passes the strict
    contract, records table uuid / tag / snapshot ids; the catalog schema and file are unchanged;
    building it again keeps the build and records the same provenance keys (source + iceberg);
  * new gold (a drifted cell) -> a new build id and tag; the old tag still reads the old build;
    the drifted build gets its own identity over the Iceberg pins, records the drift, and
    verify_build reproduces it; the source-aware contract passes it under --strict with the drift
    as info, the golden derived and the pins re-read byte-identically (the CLI, real catalog);
  * a missing tag and a moved tag fail loudly ("provenance unavailable"), never a fallback.

Skipped (with the reason) without pyspark, a JDK 17 or the two jars; GRAPH_REQUIRE_SPARK=1 turns
the skip into a failure. One JVM (local[2], 1 GB driver); do not run these under -W error (py4j).
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
pytest.importorskip("pyiceberg", reason="pyiceberg is in requirements-graph-spark*.txt (.venv-graph-spark)")

from lakehouse_graph import build, spec  # noqa: E402 (after the sys.path line and the importorskip)
from lakehouse_graph import iceberg_source as ice  # noqa: E402
from lakehouse_graph import manifest as mf  # noqa: E402


def _load_parity():
    mspec = importlib.util.spec_from_file_location("check_graph_parity_harness", REPO / "scripts/check_graph_parity.py")
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
TINY = REPO / spec.TINY_FIXTURE


class Lake:
    def __init__(self, root: Path, spark, first: dict):
        self.root, self.spark, self.first = root, spark, first
        self.db, self.warehouse = root / "catalog.db", root / "warehouse"
        self.uri, self.wh = f"sqlite:///{self.db}", f"file://{self.warehouse}"

    def catalog(self):
        return ice.open_catalog(self.uri, self.wh)

    def build(self, graph_root: Path, **kw):
        return ice.build_from_iceberg("tiny", graph_root, catalog_uri=self.uri, warehouse=self.wh,
                                      log=lambda *_: None, **kw)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    if SKIP:
        pytest.fail(f"GRAPH_REQUIRE_SPARK=1 but the spark harness is unavailable: {SKIP}")
    root = tmp_path_factory.mktemp("lakehouse")
    spark = H.build_session(root / "spark", root / "catalog.db", root / "warehouse", app_name="pytest_graph_twin",
                            driver_memory="1g")
    try:
        first = H.publish_local(spark, TINY)
        yield Lake(root, spark, first)
    finally:
        spark.stop()


@pytest.fixture(scope="module")
def tiny_exports(tmp_path_factory) -> Path:
    """A private GRAPH_ROOT whose tiny profile has its exports (the strict contract needs them)."""
    root = tmp_path_factory.mktemp("graph_root_iceberg")
    p = build.run_user_script(mf.GOLD_SCRIPT, {"CHURN_SAMPLE_DIR": str(TINY),
                                               "CHURN_EXPORT_DIR": str(spec.export_dir("tiny", root))})
    assert "Wrote" in p
    return root


def _refs(lake: Lake, table: str) -> dict:
    return {r["name"]: (r["type"], int(r["snapshot_id"]))
            for r in lake.spark.sql(f"SELECT name, type, snapshot_id FROM {table}.refs").collect()}


def test_publish_writes_partitioned_twin_and_tags_every_table(lake):
    pub = lake.first["publish"]
    assert pub["status"] == "published"
    assert pub["nodes"] == {"Subscription": 121, "Renewal": 121, "Plan": 3, "Incident": 3, "PricingChange": 2,
                            "LimitHit": 150, "OverageChange": 12, "OverageCharge": 6, "Ticket": 33, "BillingEvent": 165}
    assert sum(pub["nodes"].values()) == 616 and sum(pub["edges"].values()) == 1949
    assert pub["edges"]["SIMILAR_TO"] == 1182
    cat = lake.catalog()
    for table, col in ((ice.NODES_TABLE, "label"), (ice.EDGES_TABLE, "rel_type")):
        t = cat.load_table(table)
        assert [t.schema().find_field(f.source_id).name for f in t.spec().fields] == [col]
    tag = pub["tag"]
    assert ice.TAG_RE.match(tag)
    tagged = {**pub["tables"]}
    assert len(tagged) == 14   # 11 inputs + 3 twin tables (+ the manifest table itself below)
    for table, sid in tagged.items():
        assert _refs(lake, table)[tag] == ("TAG", sid), table
    assert _refs(lake, "lakehouse.gold.graph_build_manifest")[tag][0] == "TAG"
    cat.engine.dispose()


def test_second_publish_is_a_no_op_and_tags_survive(lake):
    before = {t: _refs(lake, t) for t in lake.first["publish"]["tables"]}
    again = H.graph_job().publish(lake.spark)
    assert again["status"] == "already_published" and again["build_id"] == lake.first["publish"]["build_id"]
    assert {t: _refs(lake, t) for t in lake.first["publish"]["tables"]} == before


def test_build_from_iceberg_equals_the_local_path_and_passes_the_strict_contract(lake, tiny_exports):
    digest = hashlib.sha256(lake.db.read_bytes()).hexdigest()
    bdir, man = lake.build(tiny_exports)
    assert hashlib.sha256(lake.db.read_bytes()).hexdigest() == digest, "PyIceberg wrote to the catalog"
    icb = man["iceberg"]
    assert man["source"] == "iceberg" and icb["tag"] == lake.first["publish"]["tag"]
    assert icb["identity"].startswith("bronze") and icb["local_path"]["byte_identical"] is True
    assert man["business_build_id"] == mf.build_identity(TINY)["business_build_id"]
    gold = icb["tables"]["lakehouse.gold.churn_renewal_features"]
    assert gold["snapshot_id"] == lake.first["publish"]["tables"]["lakehouse.gold.churn_renewal_features"]
    assert len(gold["table_uuid"]) == 36 and gold["tag"] == icb["tag"] and gold["rows"] == 121
    assert icb["twin"]["ok"] and icb["twin"]["tables_equal"] == 20
    assert icb["twin"]["similar_to"]["tie_only"] and icb["catalog_schema_unchanged"]
    p = subprocess.run([sys.executable, str(REPO / "scripts/check_graph_contract.py"), "--build", str(bdir),
                          "--strict"], capture_output=True, text=True, check=False)
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-2000:]
    assert "Graph contract OK" in p.stdout


def test_cli_source_iceberg(lake, tiny_exports):
    p = subprocess.run([sys.executable, str(REPO / "scripts/build_graph_local.py"), "build", "--profile", "tiny",
                          "--graph-root", str(tiny_exports), "--source", "iceberg", "--catalog-uri", lake.uri,
                          "--warehouse", lake.wh], capture_output=True, text=True, check=False)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "from Iceberg" in p.stdout and lake.first["publish"]["tag"] in p.stdout
    # The build of the previous test is byte-identical: kept, with the same provenance keys as a fresh one.
    assert "unchanged: the Parquet from" in p.stdout
    man = mf.read_manifest(spec.latest_link("tiny", tiny_exports).resolve())
    assert man["source"] == "iceberg" and man["iceberg"]["tag"] == lake.first["publish"]["tag"]
    assert man["iceberg"]["identity"].startswith("bronze") and "repinned_at" in man
    p = subprocess.run([sys.executable, str(REPO / "scripts/build_graph_local.py"), "build", "--profile", "tiny",
                          "--graph-root", str(tiny_exports), "--source", "iceberg", "--catalog-uri", lake.uri,
                          "--warehouse", lake.wh, "--iceberg-tag", "graph_000000000000"],
                         capture_output=True, text=True, check=False)
    assert p.returncode == 1 and "provenance unavailable" in p.stderr


def test_missing_tag_fails_loudly(lake):
    cat = lake.catalog()
    with pytest.raises(ice.ProvenanceUnavailable, match="tag graph_000000000000 is not on gold.graph_nodes"):
        ice.read_pinned(cat, ice.NODES_TABLE, "graph_000000000000")
    tag = lake.first["publish"]["tag"]
    sid = lake.first["publish"]["tables"]["lakehouse.gold.graph_nodes"]
    with pytest.raises(ice.ProvenanceUnavailable, match="but the build pins 123"):
        ice.read_pinned(cat, ice.NODES_TABLE, tag, 123)
    with pytest.raises(ice.ProvenanceUnavailable, match="the build manifest says 7"):
        ice.read_pinned(cat, ice.NODES_TABLE, tag, sid, expected_rows=7)
    with pytest.raises(ice.ProvenanceUnavailable, match="not a graph build tag"):
        ice.resolve_publish(cat, "main")
    cat.engine.dispose()


def test_new_gold_gives_a_new_build_the_old_tag_survives_and_drift_is_recorded(lake, tiny_exports, tmp_path):
    old = lake.first["publish"]
    # A drifted gold cell (like Spark vs pandas rounding): a new gold snapshot, so a new build id.
    lake.spark.sql("UPDATE lakehouse.gold.churn_renewal_features SET accept_rate_change = accept_rate_change + 0.0001 "
                   "WHERE user_id = 'sub_00001'")
    new = H.graph_job().publish(lake.spark)
    assert new["status"] == "published" and new["build_id"] != old["build_id"]
    # The old publish still reads by its tag after the graph tables were replaced.
    root_old = tmp_path / "old"
    shutil.copytree(tiny_exports / "tiny" / "export", spec.export_dir("tiny", root_old))
    bdir_old, man_old = lake.build(root_old, tag=old["tag"])
    assert man_old["iceberg"]["local_path"]["byte_identical"] is True
    # The new publish: not the local path's bytes -> its own identity, the drift recorded.
    root_new = tmp_path / "new"
    shutil.copytree(tiny_exports / "tiny" / "export", spec.export_dir("tiny", root_new))
    bdir_new, man_new = lake.build(root_new)
    icb = man_new["iceberg"]
    assert icb["tag"] == new["tag"] and icb["identity"] == "iceberg inputs"
    assert man_new["business_build_id"] != man_old["business_build_id"]
    assert icb["local_path"]["byte_identical"] is False
    assert icb["local_path"]["gold_drift"]["by_feature"] == {"accept_rate_change": {
        "cells": 1, "max_abs_delta": pytest.approx(1e-4, rel=1e-6)}}
    assert icb["local_path"]["cells_differ"]["Renewal"] == {"accept_rate_change": 1}
    assert icb["twin"]["ok"], "the twin was published from the same drifted gold"
    v = ice.verify_build(bdir_new, lake.uri, lake.wh)
    assert v == {"tag": new["tag"], "byte_identical": True, "files_differ": [], "fresh": True}
    # The source-aware contract (CLI, the real SQLite catalog): fresh over the Iceberg pins, the rounding-size
    # drift is info, the golden derived, and the determinism check re-reads the pins (no --no-determinism).
    doc = tmp_path / "contract_new.json"
    p = subprocess.run([sys.executable, str(REPO / "scripts/check_graph_contract.py"), "--build", str(bdir_new),
                        "--strict", "--catalog-uri", lake.uri, "--warehouse", lake.wh, "--json", str(doc)],
                       capture_output=True, text=True, check=False)
    out = p.stdout + p.stderr
    assert p.returncode == 0, "the source-aware contract must pass a lakehouse build whose only difference from " \
                              "the bronze is a rounding-size gold drift:\n" + out[-4000:]
    assert "Graph contract OK" in out and "stale build" not in out
    assert f"golden tiny (derived); source Iceberg {new['tag']}; gold drift 1 cell(s), info; strict" in out
    assert "info  gold drift vs the pandas twin on " in out and "1 cell(s) differ (accept_rate_change 1)" in out
    assert "1 within rounding, 0 beyond" in out
    assert f"re-read the 11 inputs at {new['tag']}" in out and "and rebuilt: byte-identical Parquet" in out
    assert "the identity recomputed from the pins" in out
    rec = json.loads(doc.read_text())
    assert rec["status"] == "pass" and rec["strict"] and rec["errors"] == [] and rec["warnings"] == []
    assert rec["source"] == {"kind": "iceberg", "tag": new["tag"], "identity": "iceberg inputs",
                             "lakehouse_build_id": new["build_id"]}
    assert rec["golden"] == "tiny" and rec["golden_mode"] == "derived"
    drift = rec["summary"]["gold_drift"]
    assert drift["cells"] == 1 and drift["columns"] == {"accept_rate_change": 1} and drift["beyond_rounding"] == 0
    assert any("gold drift vs the pandas twin" in i for i in rec["info"])
    assert rec["summary"]["iceberg_verify"] == {"tag": new["tag"], "byte_identical": True, "files_differ": [],
                                                "fresh": True}


def test_a_moved_tag_fails_loudly(lake, tmp_path):
    old = lake.first["publish"]
    old_sid = old["tables"]["lakehouse.gold.graph_nodes"]
    newest = int(lake.spark.sql("SELECT snapshot_id FROM lakehouse.gold.graph_nodes.refs WHERE name = 'main'")
                 .collect()[0]["snapshot_id"])
    assert newest != old_sid
    lake.spark.sql(f"ALTER TABLE lakehouse.gold.graph_nodes REPLACE TAG {old['tag']} AS OF VERSION {newest}")
    with pytest.raises(ice.ProvenanceUnavailable, match=f"tag {old['tag']} on gold.graph_nodes points at snapshot"):
        lake.build(tmp_path / "moved", tag=old["tag"])
    job = H.graph_job()
    with pytest.raises(job.PublishError, match="refusing to move it"):
        job.ensure_tag(lake.spark, "lakehouse.gold.graph_nodes", old["tag"], old_sid)
