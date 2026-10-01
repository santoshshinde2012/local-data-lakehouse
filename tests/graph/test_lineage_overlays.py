"""Tier-1 (Iceberg) and Tier-2 (OpenLineage) overlays of the lineage graph, in the core venv.

No Spark and no PyIceberg here: the Iceberg overlay is fed a facts document of the shape
``iceberg_facts.read_facts`` returns (tests/graph/test_lineage_iceberg.py reads a real local
JdbcCatalog(SQLite) lakehouse with PyIceberg in the spark venv), and the OpenLineage overlay reads
tests/graph/fixtures/openlineage_spark_local.jsonl.

That fixture is a REAL recording, trimmed and anonymised. It was produced by five spark-submit runs
(PySpark 3.5.3, Iceberg 1.6.1 JdbcCatalog on SQLite, ``--packages
io.openlineage:openlineage-spark_2.12:1.53.0`` resolved from Maven Central, file transport):
src/jobs/churn/01_ingest_bronze.py, 02_transform_silver.py, 03_publish_gold_features.py (twice) on the
committed tiny fixture, then src/jobs/graph/01_publish_gold_graph.py, each with Airflow-style
``spark.openlineage.parentJobName / parentRunId / rootParent*`` settings (task runs under the DAG runs
lakehouse_churn_features and lakehouse_graph). The recording had 322 events; the fixture keeps the 10
APPLICATION events (START + COMPLETE of the 5 applications) and the START / COMPLETE events of 6
action runs, in their original order. Only host-specific strings were changed (userName, driverHost,
uiWebUrl, the home and checkout path prefixes) and the SQL text of the job ``sql`` facet was replaced by
a marker; run ids, application ids, times and every other facet are as recorded.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from conftest import REPO, run_script

from lakehouse_graph import manifest as mf
from lakehouse_graph.lineage import build as lbuild
from lakehouse_graph.lineage import graph as lg
from lakehouse_graph.lineage import iceberg_facts, openlineage, oracle, queries, spec, tools

FIXTURE = REPO / "tests/graph/fixtures/openlineage_spark_local.jsonl"
GOLD = spec.GOLD_TABLE
NODES = "lakehouse.gold.graph_nodes"
INCIDENTS = "lakehouse.silver.churn_incidents"
UNKNOWN = "lakehouse.gold.not_in_the_code"
TAG = "graph_aaaaaaaaaaaa"
GRAPH_APP = "local-1790860111148"   # the graph job's applicationId in the fixture


def _quiet(*_args) -> None:
    return None


def snap(sid, parent, seq, ts, op, app=None, total=None) -> dict:
    summary = {"spark.app.id": app} if app else {}
    if total is not None:
        summary.update({"total-records": str(total), "added-records": str(total)})
    return {"snapshot_id": sid, "parent_id": parent, "sequence_number": seq, "timestamp_ms": ts, "operation": op,
            "summary": summary}


def ref(name, kind, sid) -> dict:
    return {"name": name, "kind": kind, "snapshot_id": sid, "max_ref_age_ms": None, "min_snapshots_to_keep": None,
            "max_snapshot_age_ms": None}


def facts() -> dict:
    """A catalog with every case: a replaced table (no parent) then an append, a sequence number expiry
    removed, a format-v1 table (sequence number 0), a table no code declares, a ref to a missing snapshot."""
    return {"catalog": "lakehouse", "catalog_uri": "sqlite:///file:/lake/catalog.db?mode=ro&uri=true",
            "warehouse": "file:///lake/warehouse", "catalog_schema_unchanged": True, "read_s": 0.01, "tables": {
                GOLD: {"table_uuid": "uuid-gold", "format_version": 2, "current_snapshot_id": 30,
                       "snapshots": [snap(10, None, 1, 1_000, "overwrite", "local-1", 121),
                                     snap(20, None, 2, 2_000, "overwrite", "local-2", 121),
                                     snap(30, 20, 4, 3_000, "append", "local-2", 122)],
                       "refs": [ref("main", "branch", 30), ref(TAG, "tag", 20), ref("gone", "tag", 99)]},
                INCIDENTS: {"table_uuid": "uuid-inc", "format_version": 1, "current_snapshot_id": 8,
                            "snapshots": [snap(7, None, 0, 700, "append", "local-1"), snap(8, 7, 0, 800, "append",
                                                                                          "local-1")],
                            "refs": [ref("main", "branch", 8)]},
                UNKNOWN: {"table_uuid": "uuid-x", "format_version": 2, "current_snapshot_id": 5,
                          "snapshots": [snap(5, None, 1, 500, "append")], "refs": [ref("main", "branch", 5)]}}}


def business() -> dict:
    """An Iceberg-sourced business build: it read gold at the tag, and a twin snapshot since expired."""
    return {"business_build_id": "abcdefabcdef", "built_at": "2026-10-01T00:00:00Z", "iceberg": {
        "tag": TAG, "lakehouse_build_id": "aaaaaaaaaaaa", "spark_app_id": "local-2", "identity": "iceberg inputs",
        "tables": {GOLD: {"table": GOLD, "snapshot_id": 20, "sequence_number": 2, "timestamp_ms": 2_000, "rows": 121,
                          "role": "input", "tag": TAG, "operation": "overwrite", "spark_app_id": "local-2",
                          "table_uuid": "uuid-gold"},
                   NODES: {"table": NODES, "snapshot_id": 41, "sequence_number": 1, "timestamp_ms": 2_100, "rows": 616,
                           "role": "output", "tag": TAG, "operation": "overwrite", "spark_app_id": "local-2",
                           "table_uuid": "uuid-nodes"}}}}


@pytest.fixture(scope="module")
def ice_graph() -> lg.LineageGraph:
    return lg.assemble(REPO, "core", overlays=[iceberg_facts.overlay(facts(), business())])


@pytest.fixture(scope="module")
def ol_graph() -> lg.LineageGraph:
    return lg.assemble(REPO, "core", overlays=[openlineage.overlay(FIXTURE)])


def edges(g: lg.LineageGraph, rel: str) -> list[tuple]:
    return sorted((e["src"], e["dst"], tuple(sorted(e["props"].items()))) for e in g.edges if e["rel"] == rel)


# --------------------------------------------------------------------------- Tier 1: Iceberg
def test_iceberg_overlay_snapshots_refs_and_their_datasets(ice_graph):
    g = ice_graph
    assert g.unresolved == []
    want = {f"snap:{GOLD}@{i}" for i in (10, 20, 30)} | {f"snap:{INCIDENTS}@{i}" for i in (7, 8)} | \
        {f"snap:{UNKNOWN}@5", f"snap:{NODES}@41"}
    assert set(g.ids("Snapshot")) == want
    assert sorted((s, d) for s, d, _p in edges(g, "HAS_SNAPSHOT")) == sorted((f"ds:{x[5:].rsplit('@', 1)[0]}", x)
                                                                              for x in want)
    s30 = g.props(f"snap:{GOLD}@30")
    assert (s30["sequence_number"], s30["parent_id"], s30["operation"], s30["is_current"], s30["in_catalog"]) == \
        (4, "20", "append", True, True)
    assert (s30["total_records"], s30["added_records"], s30["spark_app_id"], s30["table_uuid"]) == \
        (122, 122, "local-2", "uuid-gold")
    assert s30["committed_at"] == "1970-01-01T00:00:03.000Z" and json.loads(s30["summary"])["spark.app.id"] == "local-2"
    assert g.props(f"snap:{GOLD}@10")["is_current"] is False
    # a table only the catalog declares is kept, as a Dataset that says so
    assert g.props(f"ds:{UNKNOWN}")["declared_by"] == "iceberg catalog"
    assert any(UNKNOWN in w and "no code of the repo declares" in w for w in g.environment_warnings)
    # refs: a tag and the branches point at their snapshots; a ref to a snapshot the table does not list is named
    assert sorted(g.ids("Ref")) == sorted([f"ref:{GOLD}@main", f"ref:{GOLD}@{TAG}", f"ref:{GOLD}@gone",
                                          f"ref:{INCIDENTS}@main", f"ref:{UNKNOWN}@main"])
    assert (f"ref:{GOLD}@{TAG}", f"snap:{GOLD}@20") in [(s, d) for s, d, _p in edges(g, "POINTS_TO")]
    assert not [d for s, d, _p in edges(g, "POINTS_TO") if s == f"ref:{GOLD}@gone"]
    assert any("tag gone of lakehouse.gold.churn_renewal_features points at snapshot 99" in w
               for w in g.environment_warnings)


def test_supersedes_follows_sequence_numbers_not_parent_ids(ice_graph):
    sup = {(s, d): dict(p) for s, d, p in edges(ice_graph, "SUPERSEDES")}
    assert sup == {
        (f"snap:{GOLD}@20", f"snap:{GOLD}@10"): {"ordered_by": "sequence_number", "parent_matches": False,
                                                  "sequence_gap": 0},   # createOrReplace: no parent id
        (f"snap:{GOLD}@30", f"snap:{GOLD}@20"): {"ordered_by": "sequence_number", "parent_matches": True,
                                                  "sequence_gap": 1},   # number 3 was expired
        (f"snap:{INCIDENTS}@8", f"snap:{INCIDENTS}@7"): {"ordered_by": "timestamp_ms (no distinct sequence numbers: "
                                                                       "format v1)", "parent_matches": True,
                                                         "sequence_gap": None}}


def test_runs_produced_by_and_the_graph_build_that_consumed_the_pins(ice_graph):
    g = ice_graph
    produced = {s: d for s, d, _p in edges(g, "PRODUCED_BY_RUN")}
    assert produced[f"snap:{GOLD}@10"] == "run:spark:local-1" and produced[f"snap:{GOLD}@30"] == "run:spark:local-2"
    assert f"snap:{UNKNOWN}@5" not in produced   # no spark.app.id in its summary: no run claimed
    assert produced[f"snap:{NODES}@41"] == "run:spark:local-2"   # known from the pins only, still attributed
    build = "run:graph-build:abcdefabcdef"
    assert g.props(build)["kind"] == "graph_build" and g.props(build)["job"] == spec.GRAPH_BUILD_SCRIPT
    assert {(s, d, p) for s, d, p in edges(g, "CONSUMED_SNAPSHOT")} == {
        (build, f"snap:{GOLD}@20", (("n_rows", 121), ("role", "input"), ("tag", TAG))),
        (build, f"snap:{NODES}@41", (("n_rows", 616), ("role", "output"), ("tag", TAG)))}
    expired = g.props(f"snap:{NODES}@41")
    assert expired["in_catalog"] is False and expired["is_current"] is None
    assert any("1 snapshot(s) the catalog no longer lists" in w and "graph_nodes@41" in w
               for w in g.environment_warnings)
    ran = {s: d for s, d, _p in edges(g, "RAN_AS")}
    assert ran == {build: f"job:{spec.GRAPH_BUILD_SCRIPT}", "run:spark:local-2": f"job:{spec.GRAPH_SPARK_JOB}"}
    info = g.overlays["iceberg"]
    assert {k: info[k] for k in ("tables", "snapshots", "tags", "branches", "supersedes", "parent_cut", "spark_runs",
                                 "undeclared_tables")} == {"tables": 3, "snapshots": 6, "tags": 2, "branches": 3,
                                                           "supersedes": 3, "parent_cut": 1, "spark_runs": 2,
                                                           "undeclared_tables": [UNKNOWN]}
    assert info["graph_build"]["consumed"] == 2 and info["graph_build"]["not_in_catalog"] == [f"{NODES}@41"]
    assert set(k for k in g.inputs if ":" in k) == {"iceberg:lakehouse", "iceberg-build:abcdefabcdef"}


def test_a_csv_business_build_consumes_no_snapshot():
    g = lg.assemble(REPO, "core", overlays=[iceberg_facts.overlay(facts(), {"business_build_id": "csvcsvcsvcsv"})])
    assert edges(g, "CONSUMED_SNAPSHOT") == [] and g.overlays["iceberg"]["graph_build"] is None
    assert any("built from the bronze CSVs, not from Iceberg" in w for w in g.environment_warnings)


def test_lineage_build_id_covers_the_iceberg_facts_and_the_pins():
    def lid(*layers) -> str:
        return lbuild.lineage_identity(lg.assemble(REPO, "core", overlays=list(layers)))["lineage_build_id"]

    core, base = lid(), lid(iceberg_facts.overlay(facts(), business()))
    assert base != core and base == lid(iceberg_facts.overlay(facts(), business()))   # deterministic
    moved = facts()
    moved["tables"][GOLD]["refs"][1]["snapshot_id"] = 30   # the tag moved
    assert lid(iceberg_facts.overlay(moved, business())) != base
    other = business()
    other["iceberg"]["tables"][GOLD]["snapshot_id"] = 10   # the build read another snapshot
    assert lid(iceberg_facts.overlay(facts(), other)) != base


# --------------------------------------------------------------------------- Tier 2: OpenLineage
def test_fixture_is_the_trimmed_anonymised_real_recording():
    text = FIXTURE.read_text()
    events, problems = openlineage.read_events(FIXTURE)
    assert problems == [] and len(events) == 22 and len(text) < 64_000
    assert "/Users/" not in text and str(Path.home()) not in text and '"userName":"user"' in text
    assert {e["producer"] for e in events} == {"https://github.com/OpenLineage/OpenLineage/tree/1.53.0/integration/spark"}
    apps, stats = openlineage.collapse(events)
    assert (stats["application_runs"], stats["action_runs_collapsed"], stats["action_events_collapsed"]) == (5, 6, 12)
    assert stats["rebuilt_from_parent_facet"] == [] and len(apps) == 5


def test_openlineage_runs_ran_as_the_job_files_with_parents(ol_graph):
    g = ol_graph
    assert g.unresolved == [] and g.environment_warnings == []
    apps = [r for r in g.ids("Run") if g.props(r)["kind"] == "spark_application"]
    assert len(apps) == 5 and all(r.startswith("run:spark:local-") for r in apps)
    ran = {g.props(s)["app_name"]: d for s, d, _p in edges(g, "RAN_AS")}
    assert sorted(ran.items()) == [("churn_01_ingest_bronze", "job:src/jobs/churn/01_ingest_bronze.py"),
                                   ("churn_02_transform_silver", "job:src/jobs/churn/02_transform_silver.py"),
                                   ("churn_03_publish_gold_features", "job:src/jobs/churn/03_publish_gold_features.py"),
                                   ("graph_01_publish_gold_graph", "job:src/jobs/graph/01_publish_gold_graph.py")]
    assert len(edges(g, "RAN_AS")) == 5   # gold was published twice: two runs of one job
    graph_run = g.props(f"run:spark:{GRAPH_APP}")
    assert (graph_run["state"], graph_run["engine"], graph_run["engine_version"], graph_run["n_actions"],
            graph_run["n_events"]) == ("COMPLETE", "spark", "3.5.3", 2, 6)
    assert json.loads(graph_run["actions"]) == {"atomic_replace_table_as_select": 1, "create_or_replace_tag": 1}
    assert graph_run["started_at"] < graph_run["ended_at"] and graph_run["namespace"] == "lakehouse"
    # PARENT: application -> its Airflow task run -> the DAG run
    parents = {(s, dict(p)["kind"]): d for s, d, p in edges(g, "PARENT")}
    task = parents[(f"run:spark:{GRAPH_APP}", "parent")]
    dag = parents[(task, "root")]
    assert (g.props(task)["kind"], g.props(task)["job"], g.props(task)["engine"]) == \
        ("parent_run", "lakehouse_graph.publish_gold_graph", "airflow")
    assert (g.props(dag)["kind"], g.props(dag)["job"]) == ("root_run", "lakehouse_graph")
    assert graph_run["airflow_run_id"] == g.props(dag)["ol_run_id"]
    assert len(edges(g, "PARENT")) == 10 and len([r for r in g.ids("Run") if g.props(r)["kind"] == "root_run"]) == 2
    bronze = next(r for r in apps if g.props(r)["app_name"] == "churn_01_ingest_bronze")
    inputs = json.loads(g.props(bronze)["datasets"])["inputs"]
    assert inputs and all(x.startswith("file:") and x.endswith(".csv") for x in inputs)
    info = g.overlays["openlineage"]
    assert (info["events"], info["application_runs"], info["ran_as"], info["parent_edges"]) == (22, 5, 5, 10)
    assert g.inputs["openlineage:openlineage_spark_local.jsonl"] == mf.sha256_file(FIXTURE)


def _event(run_id, name, kind, at, *, job_type="SQL_JOB", parent=None, app_id=None, app_name=None) -> dict:
    facets: dict = {}
    if parent:
        facets["parent"] = {"run": {"runId": parent[0]}, "job": {"namespace": "lakehouse", "name": parent[1]}}
    if app_id:
        facets["spark_applicationDetails"] = {"applicationId": app_id, "appName": app_name or name}
    return {"eventTime": at, "eventType": kind, "run": {"runId": run_id, "facets": facets},
            "job": {"namespace": "lakehouse", "name": name, "facets": {"jobType": {"jobType": job_type}}},
            "inputs": [], "outputs": [], "producer": "test"}


def test_openlineage_rules_collapse_rebuild_unmapped_and_bad_lines(tmp_path):
    lines = [
        json.dumps(_event("app-1", "churn_01_ingest_bronze", "START", "2026-10-01T10:00:00Z", job_type="APPLICATION",
                          app_id="local-11")),
        "",
        "{not json",
        json.dumps({"eventTime": "2026-10-01T10:00:00Z", "run": {}, "job": {"name": "x"}}),
        json.dumps(_event("a-1", "churn_01_ingest_bronze.append_data.x", "START", "2026-10-01T10:00:01Z",
                          parent=("app-1", "churn_01_ingest_bronze"))),
        json.dumps(_event("a-2", "churn_01_ingest_bronze.collect_limit", "RUNNING", "2026-10-01T10:00:02Z",
                          parent=("a-1", "churn_01_ingest_bronze.append_data.x"))),   # nested: still app-1
        json.dumps(_event("app-1", "churn_01_ingest_bronze", "RUNNING", "2026-10-01T10:00:03Z",
                          job_type="APPLICATION")),
        json.dumps(_event("app-1", "churn_01_ingest_bronze", "FAIL", "2026-10-01T10:00:04Z",
                          job_type="APPLICATION")),
        # a truncated file: actions of an application whose own events are missing
        json.dumps(_event("b-1", "churn_02_transform_silver.append_data.y", "START", "2026-10-01T11:00:00Z",
                          parent=("app-2", "churn_02_transform_silver"), app_id="local-22",
                          app_name="churn_02_transform_silver")),
        # an in-process harness run keeps the harness's appName: no job of the repo
        json.dumps(_event("app-3", "harness_s2", "START", "2026-10-01T12:00:00Z", job_type="APPLICATION",
                          app_id="local-33")),
        json.dumps(_event("app-3", "harness_s2", "COMPLETE", "2026-10-01T12:00:09Z", job_type="APPLICATION")),
        json.dumps(_event("bad-time", "churn_01_ingest_bronze", "START", "yesterday", job_type="APPLICATION")),
        # an action-type run with no parent has nothing to fold into: it is a run of its own
        json.dumps(_event("lone", "graph_01_publish_gold_graph.create_table", "COMPLETE", "2026-10-01T13:00:00Z")),
    ]
    path = tmp_path / "openlineage.jsonl"
    path.write_text("\n".join(lines) + "\n")
    g = lg.assemble(REPO, "core", overlays=[openlineage.overlay(path)])
    one = g.props("run:spark:local-11")
    assert (one["state"], one["started_at"], one["ended_at"], one["n_actions"], one["n_events"]) == \
        ("FAIL", "2026-10-01T10:00:00Z", "2026-10-01T10:00:04Z", 2, 5)
    assert json.loads(one["actions"]) == {"append_data": 1, "collect_limit": 1}
    rebuilt = g.props("run:spark:local-22")   # rebuilt from the ParentRunFacet; the appName still maps
    assert (rebuilt["state"], rebuilt["started_at"], rebuilt["job"]) == (None, None, "churn_02_transform_silver")
    ran = {s: d for s, d, _p in edges(g, "RAN_AS")}
    assert ran == {"run:spark:local-11": "job:src/jobs/churn/01_ingest_bronze.py",
                   "run:spark:local-22": "job:src/jobs/churn/02_transform_silver.py"}
    warnings = " | ".join(g.environment_warnings)
    assert "3 line(s) of openlineage.jsonl are not RunEvents" in warnings and "line 3: not JSON" in warnings
    assert "line 4: not an OpenLineage RunEvent" in warnings and "line 12: eventTime 'yesterday'" in warnings
    lone = g.props("run:ol:lone")
    assert (lone["kind"], lone["state"], lone["n_actions"]) == ("spark_application", "COMPLETE", 0)
    assert not any(r.endswith(":None") for r in g.ids("Run"))
    assert "run app-2 (churn_02_transform_silver) has action events but no event of its own" in warnings
    assert "run run:spark:local-33 (appName harness_s2) maps to no job of the repo" in warnings
    assert g.overlays["openlineage"]["unmapped_app_names"] == ["graph_01_publish_gold_graph.create_table",
                                                                "harness_s2"] and edges(g, "PARENT") == []


def test_an_offset_less_time_or_a_wrong_shaped_facet_is_skipped_or_ignored_never_fatal(tmp_path):
    """A naive eventTime next to an offset one could not be compared (TypeError): it is a problem line.
    A facet of the wrong shape (a list or a string where an object belongs) is ignored."""
    weird = _event("app-9", "churn_01_ingest_bronze", "START", "2026-10-01T10:00:00+02:00", job_type="APPLICATION",
                   app_id="local-99")
    weird["run"]["facets"].update({"parent": "not an object", "processing_engine": ["spark"],
                                   "environment-properties": {"environment-properties": "x"}})
    weird["job"]["facets"] = []
    weird["inputs"], weird["outputs"] = 7, {"name": "x"}
    def done(at: str) -> str:
        return json.dumps(_event("app-9", "churn_01_ingest_bronze", "COMPLETE", at, job_type="APPLICATION"))

    lines = [
        json.dumps(weird),
        done("2026-10-01T10:00:05"),
        done("2026-10-01"),
        done("2026-10-01T08:00:09Z"),
        json.dumps(_event({"id": 1}, "x", "START", "2026-10-01T10:00:00Z")),          # runId not a string
        json.dumps(_event("act-1", "churn_01_ingest_bronze.append_data", "START", "2026-10-01T10:00:01Z",
                          parent=("app-9", "churn_01_ingest_bronze")) | {"eventType": 3}),
        json.dumps([1, 2]),
    ]
    path = tmp_path / "openlineage.jsonl"
    path.write_text("\n".join(lines) + "\n")
    events, problems = openlineage.read_events(path)
    assert [p.split(":", 1)[0] for p in problems] == ["line 2", "line 3", "line 5", "line 7"]
    assert "line 2: eventTime '2026-10-01T10:00:05' has no UTC offset" in problems[0]
    assert "has no UTC offset" in problems[1] and "not an OpenLineage RunEvent" in problems[2]
    apps, stats = openlineage.collapse(events)
    (app,) = apps.values()
    # +02:00 and Z compare as instants: 10:00+02:00 = 08:00Z starts, 08:00:09Z ends
    assert (app["started_at"], app["ended_at"], app["state"]) == ("2026-10-01T10:00:00+02:00",
                                                                   "2026-10-01T08:00:09Z", "COMPLETE")
    assert (app["parent"], app["engine"], app["env"], app["n_events"], stats["action_runs_collapsed"]) == \
        (None, None, None, 3, 1)
    g = lg.assemble(REPO, "core", overlays=[openlineage.overlay(path)])
    run = g.props("run:spark:local-99")
    assert (run["state"], run["n_actions"]) == ("COMPLETE", 1) and json.loads(run["actions"]) == {"append_data": 1}
    assert "4 line(s) of openlineage.jsonl are not RunEvents" in " | ".join(g.environment_warnings)


def test_both_overlays_meet_on_the_spark_application_id():
    joined = facts()
    joined["tables"][GOLD]["snapshots"][2]["summary"]["spark.app.id"] = GRAPH_APP
    both = [iceberg_facts.overlay(joined, business()), openlineage.overlay(FIXTURE)]
    g = lg.assemble(REPO, "core", overlays=both)
    run = f"run:spark:{GRAPH_APP}"
    assert (f"snap:{GOLD}@30", run) in [(s, d) for s, d, _p in edges(g, "PRODUCED_BY_RUN")]
    assert g.props(run)["app_name"] == "graph_01_publish_gold_graph" and g.props(run)["state"] == "COMPLETE"
    assert g.overlays["openlineage"]["runs_joined_to_snapshots"] == 1
    reverse = lg.assemble(REPO, "core", overlays=both[::-1])   # the order of the overlays does not matter
    assert {(e["rel"], e["src"], e["dst"]) for e in reverse.edges} == {(e["rel"], e["src"], e["dst"]) for e in g.edges}
    def written(graph: lg.LineageGraph) -> dict:   # what reaches the Parquet: a None property is a null either way
        return {i: (n["label"], {k: v for k, v in n["props"].items() if v is not None}) for i, n in graph.nodes.items()}
    assert written(reverse) == written(g)


# --------------------------------------------------------------------------- build, contract, CLI
@pytest.mark.slow
def test_overlay_build_passes_the_strict_contract_with_cypher_equal_to_python(tmp_path):
    bdir = tmp_path / "build"
    layers = [iceberg_facts.overlay(facts(), business()), openlineage.overlay(FIXTURE)]
    _dir, man = lbuild.build_lineage(bdir, "core", overlays=layers, log=_quiet)
    assert sorted(man["overlays"]) == ["iceberg", "openlineage"] and man["unresolved"] == []
    assert man["counts"]["nodes"]["Snapshot"] == 7 and man["counts"]["edges"]["CONSUMED_SNAPSHOT"] == 2
    out = tmp_path / "contract.json"
    p = run_script("check_lineage_contract.py", "--build", str(bdir), "--strict", "--json", str(out))
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr
    for line in ("Tier 1 / Tier 2 facts (overlays iceberg, openlineage; as read at build time)",
                 "every snapshot belongs to its table, every ref points at its snapshot", "tier facts: Cypher = oracle",
                 "Iceberg snapshots in the graph = 6 (the facts read)",
                 "snapshots the graph build abcdefabcdef CONSUMED at graph_aaaaaaaaaaaa = 2",
                 "OpenLineage: 5 application runs are Run nodes", "oracle = golden core.json"):
        assert line in p.stdout, line
    doc = json.loads(out.read_text())
    assert doc["status"] == "pass" and doc["overlays"] == ["iceberg", "openlineage"]
    assert doc["tiers"]["tables"][GOLD] == {"snapshots": 3, "current": ["30"], "not_in_catalog": 0, "supersedes": 2,
                                            "parent_cut": 1, "tags": ["gone", TAG], "branches": ["main"]}
    assert doc["tiers"]["dangling_refs"] == [f"ref:{GOLD}@gone"]
    t = oracle.load_tables(bdir)
    assert oracle.tier_invariants(t) == []
    with tools.LineageContext(bdir) as ctx:
        assert queries.tier_section(queries.tier_rows(ctx.lineage_conn)) == queries.tier_section(oracle.tier_rows(t))


@pytest.mark.slow
def test_contract_fails_on_tier_rows_without_an_overlay_and_on_a_broken_chain(tmp_path):
    def stray(g: lg.LineageGraph) -> None:   # Tier-1 rows, but no overlay summary in the manifest
        g.node("Snapshot", f"snap:{GOLD}@1", table_name=GOLD, snapshot_id="1", sequence_number=1, in_catalog=True)
        g.edge("HAS_SNAPSHOT", f"ds:{GOLD}", f"snap:{GOLD}@1")

    _dir, _man = lbuild.build_lineage(tmp_path / "stray", "core", overlays=[stray], log=_quiet)
    p = run_script("check_lineage_contract.py", "--build", str(tmp_path / "stray"), "--no-ladybug")
    assert p.returncode == 1 and "rows without an overlay in the lineage manifest" in p.stdout

    def broken(g: lg.LineageGraph) -> None:   # a SUPERSEDES across two tables, recorded as an overlay
        iceberg_facts.overlay(facts())(g)
        g.edge("SUPERSEDES", f"snap:{INCIDENTS}@7", f"snap:{GOLD}@30", ordered_by="sequence_number",
               parent_matches=False)

    _dir, _man = lbuild.build_lineage(tmp_path / "broken", "core", overlays=[broken], log=_quiet)
    p = run_script("check_lineage_contract.py", "--build", str(tmp_path / "broken"), "--no-ladybug")
    assert p.returncode == 1 and "SUPERSEDES a snapshot of another table" in p.stdout, p.stdout[-2000:]


@pytest.mark.slow
def test_cli_openlineage_default_path_and_explicit_file(tmp_path):
    root = tmp_path / "root"
    p = run_script("build_lineage_local.py", "--build", str(tmp_path / "b"), "--graph-root", str(root),
                   "--openlineage")
    assert p.returncode == 1 and "--openlineage:" in p.stderr and "does not exist" in p.stderr
    (root / "lineage").mkdir(parents=True)
    shutil.copy2(FIXTURE, root / spec.OPENLINEAGE_FILE)
    p = run_script("build_lineage_local.py", "--build", str(tmp_path / "b"), "--graph-root", str(root),
                   "--openlineage")
    assert p.returncode == 0, p.stderr[-2000:]
    assert "Tier 2: OpenLineage" in p.stdout and "5 application runs (6 action runs / 12 events collapsed), " \
                                                  "5 RAN_AS, 10 PARENT" in p.stdout
    man = lbuild.read_manifest(tmp_path / "b")
    assert man["overlays"]["openlineage"]["sha256"] == mf.sha256_file(FIXTURE)
    assert man["inputs_sha256"]["openlineage:openlineage.jsonl"] == mf.sha256_file(FIXTURE)


def test_cli_iceberg_without_pyiceberg_says_which_venv(tmp_path):
    pytest.importorskip("lakehouse_graph")   # always there; the real condition is below
    try:
        import pyiceberg  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("pyiceberg is installed here (tests/graph/test_lineage_iceberg.py covers --iceberg)")
    p = run_script("build_lineage_local.py", "--build", str(tmp_path / "b"), "--iceberg",
                   "--catalog-uri", f"sqlite:///{tmp_path}/catalog.db")
    assert p.returncode == 1 and "--iceberg needs pyiceberg" in p.stderr and ".venv-graph-spark" in p.stderr
    assert not (tmp_path / "catalog.db").exists() and not (tmp_path / "b" / spec.LINEAGE_DIR).exists()


def test_overlay_modules_never_touch_the_snapshot_log_or_data():
    """Static: iceberg_facts reads .snapshots() / .refs() only (no history / snapshot_log / inspect / scan)."""
    import ast

    tree = ast.parse((REPO / "src/lakehouse_graph/lineage/iceberg_facts.py").read_text())
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not attrs & {"history", "snapshot_log", "metadata_log", "inspect", "scan", "to_arrow", "plan_files"}
    assert {"snapshots", "refs", "load_table", "list_tables", "list_namespaces"} <= attrs
    doc, pins = facts(), business()   # the overlay never mutates the documents it is given
    iceberg_facts.overlay(doc, pins)(lg.LineageGraph())
    assert doc == facts() and pins == business()
