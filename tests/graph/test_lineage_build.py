"""Lineage build: deterministic Parquet, the Ladybug projection, identity, profiles, offline.

``core_build`` (built once per session into a temp directory) is also the fixture of the
contract and tool tests, which import it from here.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from conftest import REPO, run_script

from lakehouse_graph import manifest as mf
from lakehouse_graph import spec as gspec
from lakehouse_graph import store
from lakehouse_graph.lineage import build as lbuild
from lakehouse_graph.lineage import extract as ex
from lakehouse_graph.lineage import graph as lg
from lakehouse_graph.lineage import spec, tools

_BUILDS: dict[str, tuple[Path, dict]] = {}   # one build per session, shared by the lineage test modules


def _quiet(*_args) -> None:
    return None


@pytest.fixture(scope="session")
def core_build(tmp_path_factory) -> tuple[Path, dict]:
    """(build_dir, lineage manifest) of a core-profile build in a scratch directory."""
    if "core" not in _BUILDS:
        bdir = tmp_path_factory.mktemp("lineage_core") / "build"
        _dir, man = lbuild.build_lineage(bdir, "core", log=_quiet)
        _BUILDS["core"] = (bdir, man)
    return _BUILDS["core"]


def fake_radar(root: Path) -> Path:
    """A minimal retention-radar checkout: the three files the full profile reads, with the
    same ranges as check_churn_export.py (so only the severity differs)."""
    contract = ex.extract_export_contract(ex.SourceFiles(REPO))
    serve = [c for c in contract["train_columns"] if c != "churned"]
    (root / "configs/schemas").mkdir(parents=True)
    (root / "src/retention_radar/data").mkdir(parents=True)
    (root / spec.RADAR_SCHEMA).write_text(json.dumps({
        "required": serve, "additionalProperties": False,
        "properties": {"plan_tier": {"enum": ["pro", "pro_plus", "ultra"]}}}))
    ranges = {c: (lo, hi) for c, lo, hi in contract["ranges"]}
    (root / spec.RADAR_CONFIG).write_text(f"FEATURE_RANGES = {ranges!r}\n")
    (root / spec.RADAR_INGEST).write_text('SERVE = "hero_inference_record.json"\n')
    return root


# --------------------------------------------------------------------------- files
def test_build_writes_one_typed_table_per_label_and_type(core_build):
    bdir, man = core_build
    ldir = bdir / spec.LINEAGE_DIR
    assert sorted(p.name for p in ldir.glob("*.parquet")) == sorted(f for _n, f, _s in lbuild.table_files())
    assert len(man["files"]) == 24 + 48 and (bdir / spec.DB_FILE).is_file()
    for name, file, schema in lbuild.table_files():
        table = pq.read_table(ldir / file)
        assert table.schema.equals(schema), name
        assert table.num_rows == man["files"][file]["rows"]
        assert mf.sha256_file(ldir / file) == man["files"][file]["sha256"]
        assert table.schema.metadata is None or b"pandas" not in table.schema.metadata
    nodes, edges = lbuild.read_tables(ldir)
    assert [r["id"] for r in nodes["DataColumn"]] == sorted(r["id"] for r in nodes["DataColumn"])
    for label in ("Snapshot", "Ref", "Run"):
        assert nodes[label] == []
    for rel in ("HAS_SNAPSHOT", "POINTS_TO", "PRODUCED_BY_RUN", "RAN_AS", "PARENT", "SUPERSEDES", "CONSUMED_SNAPSHOT"):
        assert edges[rel] == []
    ids = {r["id"] for rows in nodes.values() for r in rows}
    for rel, rows in edges.items():
        pairs = set(spec.EDGE_SCHEMA[rel].pairs)
        assert all(e["src"] in ids and e["dst"] in ids and (e["src_label"], e["dst_label"]) in pairs for e in rows), rel
    assert not list(bdir.glob(".lineage-*")), "temp / staging directories must not be left behind"


def test_manifest_records_identity_inputs_and_counts(core_build):
    bdir, man = core_build
    assert man == lbuild.read_manifest(bdir)
    assert man["spec"] == spec.SPEC_VERSION and man["profile"] == "core" and len(man["lineage_build_id"]) == 12
    assert man["unresolved"] == [] and man["ladybug"]["counts_equal_parquet"] is True
    assert set(spec.CONTENT_CODE) == set(man["code_sha256"]) and "missing" not in man["code_sha256"].values()
    for rel in (spec.GOLD_SQL, spec.MAKEFILE, spec.CI_WORKFLOW, spec.README, spec.BRONZE_JOB, spec.GRAPH_SPEC,
                spec.LINEAGE_BUILD_SCRIPT, "airflow/dags/lakehouse_churn_features.py", "pipelines/run_job.sh"):
        assert len(man["inputs_sha256"][rel]) == 64, rel
    for rel in (spec.GOLD_SQL, spec.BRONZE_JOB, spec.SILVER_JOB, spec.EXPORT_CONTRACT):   # the user's own files
        assert man["inputs_sha256"][rel] == mf.sha256_file(REPO / rel), rel
    assert man["counts"]["nodes"]["DataColumn"] == 322 and man["counts"]["edges"]["DERIVED_FROM"] == 346
    assert man["counts"]["total_nodes"] == sum(man["counts"]["nodes"].values())
    assert set(man["versions"]) >= {"sqlglot", "pyarrow", "ladybug", "python"}


def test_rebuild_is_byte_identical_and_kept(core_build):
    bdir, man = core_build
    lines: list[str] = []
    _dir, again = lbuild.build_lineage(bdir, "core", log=lines.append)
    assert again["lineage_build_id"] == man["lineage_build_id"] and again["built_at"] == man["built_at"]
    assert any("unchanged" in x and "byte-identical" in x for x in lines), lines
    fresh = lbuild.write_tables(lg.assemble(REPO, "core"), bdir.parent / "rewrite")
    assert {k: v["sha256"] for k, v in fresh.items()} == {k: v["sha256"] for k, v in man["files"].items()}


def test_ladybug_projection_equals_parquet_and_opens_read_only(core_build):
    bdir, man = core_build
    db, conn = store.open_readonly(bdir / spec.DB_FILE)
    try:
        counts = lbuild.count_all(conn)
        with pytest.raises(RuntimeError):
            conn.execute("CREATE (:`Job` {id: 'job:nope'})")
    finally:
        conn.close()
        db.close()
    assert counts == {**man["counts"]["nodes"], **man["counts"]["edges"]}
    assert len(lbuild.ddl_statements()) == 24 + 48
    with pytest.raises(FileExistsError):
        lbuild.load_ladybug(bdir / spec.LINEAGE_DIR, bdir / spec.DB_FILE)


def test_replacing_a_held_build_is_refused(core_build, tmp_path):
    bdir, _man = core_build
    copy = tmp_path / "held"
    shutil.copytree(bdir, copy)
    store.write_pidfile(copy)   # this test process "serves" the copy
    try:
        with pytest.raises(lbuild.LineageBuildError, match="held by live pid"):
            lbuild.build_lineage(copy, "core", rebuild=True, log=_quiet)
    finally:
        store.remove_pidfile(copy)
    assert lbuild.read_manifest(copy)["lineage_build_id"] == lbuild.read_manifest(bdir)["lineage_build_id"]
    assert not list(copy.glob(".lineage-*"))


def test_overlays_fill_the_tier1_placeholders(tmp_path):
    def overlay(g: lg.LineageGraph) -> None:
        g.node("Snapshot", "snap:gold@1", table_name=spec.GOLD_TABLE, snapshot_id="1", sequence_number=1)
        g.edge("HAS_SNAPSHOT", f"ds:{spec.GOLD_TABLE}", "snap:gold@1")

    _dir, man = lbuild.build_lineage(tmp_path / "overlay", "core", overlays=[overlay], log=_quiet)
    assert man["counts"]["nodes"]["Snapshot"] == 1 and man["counts"]["edges"]["HAS_SNAPSHOT"] == 1
    with tools.LineageContext(tmp_path / "overlay") as ctx:
        rows = store.rows(ctx.lineage_conn.execute(
            "MATCH (d:`Dataset`)-[:HAS_SNAPSHOT]->(s:`Snapshot`) RETURN d.ref, s.sequence_number "
            "ORDER BY d.ref LIMIT 5"))
    assert rows == [["gold.churn_renewal_features", 1]]


# --------------------------------------------------------------------------- inside a business build
@pytest.fixture(scope="module")
def business_copy(tiny_build, tmp_path_factory) -> tuple[Path, Path, dict]:
    """(graph root, build dir, manifest): a private copy of the tiny business build, laid out as
    <root>/tiny/builds/<id> with its latest link, so the shared session build is never modified."""
    src, business = tiny_build
    root = tmp_path_factory.mktemp("lineage_graph_root")
    bdir = gspec.builds_dir("tiny", root) / src.name
    shutil.copytree(src, bdir, ignore=shutil.ignore_patterns(store.PID_DIR, spec.LINEAGE_DIR, spec.DB_FILE + "*"))
    store.update_link(gspec.latest_link("tiny", root), bdir)
    return root, bdir, business


def test_lineage_lands_in_the_business_build_without_touching_its_identity(business_copy):
    _root, bdir, business = business_copy
    before = mf.read_manifest(bdir)
    files_before = {p.name: mf.sha256_file(p) for p in (bdir / "parquet").glob("*.parquet")}
    assert lbuild.graph_root_of(bdir) == bdir.resolve().parents[2]   # so the build lock of that root applies
    _dir, man = lbuild.build_lineage(bdir, "core", log=_quiet)
    after = mf.read_manifest(bdir)
    assert after["business_build_id"] == before["business_build_id"] == business["business_build_id"]
    assert after["files"] == before["files"] and after["counts"] == before["counts"]
    assert {p.name: mf.sha256_file(p) for p in (bdir / "parquet").glob("*.parquet")} == files_before
    summary = after["lineage"]
    assert summary["lineage_build_id"] == man["lineage_build_id"] != after["business_build_id"]
    assert summary["spec"] == spec.SPEC_VERSION and summary["manifest"] == "lineage/manifest.json"
    assert (bdir / spec.DB_FILE).is_file() and (bdir / store.DB_FILE).is_file()
    # an existing strict contract record of the business graph still describes the same Parquet
    contract = store.read_contract(bdir)
    if contract:
        assert store.is_strict_pass(contract, after) == store.is_strict_pass(contract, before)
    # the same code gives the same lineage id whatever bronze the business build was made from
    assert man["lineage_build_id"] == lbuild.lineage_identity(lg.assemble(REPO, "core"))["lineage_build_id"]


def test_build_script_targets_the_latest_build_of_a_graph_profile(business_copy):
    graph_root, bdir, _ = business_copy
    p = run_script("build_lineage_local.py", "--graph-profile", "tiny", "--graph-root", str(graph_root))
    assert p.returncode == 0, p.stderr
    assert "Lineage build OK (metadata-graph/0.1, profile core)" in p.stdout and str(bdir) in p.stdout
    missing = run_script("build_lineage_local.py", "--graph-profile", "s999", "--graph-root", str(graph_root))
    assert missing.returncode == 1 and "no graph build for profile s999" in missing.stderr
    bad = run_script("build_lineage_local.py", "--profile", "everything", "--graph-root", str(graph_root))
    assert bad.returncode == 2   # argparse: not a lineage profile


# --------------------------------------------------------------------------- offline
def test_core_build_never_opens_a_socket(monkeypatch, tmp_path):
    def refuse(*_a, **_k):
        raise AssertionError("the lineage build opened a network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    real_run = subprocess.run

    def no_gh(cmd, *a, **k):
        assert cmd[0] not in ("gh", "curl", "wget"), cmd
        return real_run(cmd, *a, **k)

    monkeypatch.setattr(subprocess, "run", no_gh)
    g = lg.assemble(REPO, "core", radar_public=True)   # core ignores the public-radar flag
    assert g.unresolved == [] and "radar-public:main" not in g.inputs
    lbuild.write_tables(g, tmp_path / "offline")


@pytest.mark.skipif(sys.platform != "darwin" or shutil.which("sandbox-exec") is None, reason="macOS sandbox-exec only")
def test_build_and_contract_run_with_the_network_denied(tmp_path):
    """The whole script, loader subprocess included, under a kernel-enforced deny network* profile."""
    profile = "(version 1)(allow default)(deny network*)"
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    probe = subprocess.run(["sandbox-exec", "-p", profile, sys.executable, "-c",
                            "import socket; socket.create_connection(('127.0.0.1', 9), 1)"],
                           capture_output=True, text=True, check=False)
    if "sandbox_apply" in probe.stderr or "Operation not permitted" not in probe.stderr:
        pytest.skip(f"sandbox-exec cannot be applied here (nested sandbox?): {probe.stderr.strip()[-120:]}")
    out = tmp_path / "sandboxed"
    p = subprocess.run(["sandbox-exec", "-p", profile, sys.executable, str(REPO / "scripts/build_lineage_local.py"),
                        "--build", str(out)], capture_output=True, text=True, cwd=REPO, env=env, check=False)
    assert p.returncode == 0, p.stderr
    assert "Lineage build OK" in p.stdout and (out / spec.DB_FILE).is_file()


# --------------------------------------------------------------------------- full profile
def test_full_profile_without_radar_dir_adds_file_snapshots_and_warns(tiny_build, tmp_path):
    bdir, business = tiny_build
    sample, export = mf.resolve_path(business["inputs"]["sample_dir"]), mf.resolve_path(business["exports"]["dir"])
    g = lg.assemble(REPO, "full", sample_dir=sample, export_dir=export)
    core = lg.assemble(REPO, "core")
    snaps = g.ids("FileSnapshot")
    assert len(snaps) == len(gspec.BRONZE_FILES) + len(spec.RETAIL_CSVS) + len(gspec.EXPORT_FILES) == 16
    assert g.counts()["total_nodes"] == core.counts()["total_nodes"] + 16
    # about this machine, not the repo: recorded apart from the warnings that --strict turns into failures
    assert any("RADAR_DIR is not set" in w for w in g.environment_warnings) and g.warnings == []
    assert not [i for i in g.ids("Contract") if i.startswith("contract:radar_")]
    audit = next(g.props(i) for i in snaps if "churn_renewals_audit" in i)
    assert audit["deterministic"] is False and audit["n_rows"] == 121
    train = next(g.props(i) for i in snaps if "churn_user_features" in i)
    assert train["sha256"] == mf.sha256_file(export / "churn_user_features.csv") and train["deterministic"] is True
    assert g.props("assert:readme_churn#route_score_today")["holds"] is True          # 1 scored today, also on tiny
    assert g.props("assert:readme_churn#route_model")["holds"] is False               # the README states seed 42
    assert g.props("metric:churn.voluntary_lapse_rate")["golden_detail"] == "9/108"
    assert [k for k in g.inputs if k.startswith("data:")] and not [k for k in core.inputs if k.startswith("data:")]
    assert lbuild.lineage_identity(g)["lineage_build_id"] != lbuild.lineage_identity(core)["lineage_build_id"]


def test_full_profile_without_radar_dir_passes_the_strict_contract(tiny_build, tmp_path):
    """make lineage-local LINEAGE_PROFILE=full on a machine with no radar checkout: a note, not a failure."""
    _bdir, business = tiny_build
    out = tmp_path / "full"
    env = {k: v for k, v in os.environ.items() if k != "RADAR_DIR"}
    built = subprocess.run(
        [sys.executable, str(REPO / "scripts/build_lineage_local.py"), "--profile", "full", "--build", str(out),
         "--sample-dir", str(mf.resolve_path(business["inputs"]["sample_dir"])),
         "--export-dir", str(mf.resolve_path(business["exports"]["dir"]))],
        capture_output=True, text=True, cwd=REPO, env=env, check=False)
    assert built.returncode == 0, built.stderr
    assert "WARN (environment): full profile: RADAR_DIR is not set" in built.stderr
    man = lbuild.read_manifest(out)
    assert man["warnings"] == [] and len(man["environment_warnings"]) == 1
    assert man["counts"]["nodes"]["FileSnapshot"] == 16 and man["counts"]["nodes"]["DownstreamRepo"] == 1
    p = run_script("check_lineage_contract.py", "--build", str(out), "--strict")
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr
    assert "note  environment (not gated): full profile: RADAR_DIR is not set" in p.stdout
    assert "WARN" not in p.stdout and "Whole-graph totals (reported, not gated)" in p.stdout
    assert "Lineage contract OK (metadata-graph/0.1, profile full" in p.stdout and "; strict" in p.stdout
    # a wrong RADAR_DIR is a mistake, not an environment: it stays a warning and fails --strict
    wrong = lg.assemble(REPO, "full", radar_dir=tmp_path)
    assert any("has no configs/schemas/user_record.schema.json" in w for w in wrong.warnings)


def test_full_profile_without_exports_names_what_it_did_not_snapshot(tiny_build, tmp_path):
    """Bronze present, exports never generated on this machine: an environment note (never gated) that names
    the missing files and what goes unobserved, instead of silently fewer FileSnapshot nodes."""
    _bdir, business = tiny_build
    empty = tmp_path / "export"
    empty.mkdir()
    g = lg.assemble(REPO, "full", sample_dir=mf.resolve_path(business["inputs"]["sample_dir"]), export_dir=empty)
    assert len(g.ids("FileSnapshot")) == len(gspec.BRONZE_FILES) + len(spec.RETAIL_CSVS) == 13
    missing = [w for w in g.environment_warnings if "not found under" in w]
    assert len(missing) == 1 and len(g.environment_warnings) == 2 and g.warnings == []   # + the RADAR_DIR note
    assert f"{len(gspec.EXPORT_FILES)} data file(s) not found" in missing[0]
    assert all(name in missing[0] for name in gspec.EXPORT_FILES) and "README route counts" in missing[0]
    assert g.props("assert:readme_churn#route_model").get("holds") is None         # not observed, not "false"
    assert g.props("metric:churn.voluntary_lapse_rate").get("golden") is None


def test_full_profile_with_a_radar_checkout_links_the_same_rules(tiny_build, tmp_path):
    _bdir, business = tiny_build
    radar = fake_radar(tmp_path / "radar")
    out = tmp_path / "full"
    _dir, man = lbuild.build_lineage(out, "full", sample_dir=mf.resolve_path(business["inputs"]["sample_dir"]),
                                     export_dir=mf.resolve_path(business["exports"]["dir"]), radar_dir=radar,
                                     log=_quiet)
    assert man["warnings"] == [] and man["environment_warnings"] == [] and man["unresolved"] == []
    assert man["counts"]["edges"]["SAME_RULE_AS"] == 18 and man["counts"]["edges"]["CONSUMES_VIA"] == 2
    assert man["counts"]["edges"]["CONSUMES"] == 2   # the CI-derived and the radar-derived edges are merged
    assert [k for k in man["inputs_sha256"] if k.startswith("radar:")]
    with tools.LineageContext(out) as ctx:
        data, caveats = tools.lineage_guards(ctx, "gold.churn_renewal_features.limit_hits_14d")
        trace, trace_caveats = tools.lineage_trace(ctx, "bronze.churn_limit_events_raw.hit_at", "downstream")
    ours = next(a for a in data["assertions"] if a["assertion"] == "assert:churn_export#range:limit_hits_14d")
    assert ours["same_rule_as"] == [{"assertion": "assert:radar_user_record@local#range:limit_hits_14d",
                                     "contract": "radar_user_record@local", "severity": "error",
                                     "severity_differs": True, "bounds_equal": True}]
    assert len(data["summary"]["severity_differs"]) == 2 and not any("No retention-radar rule" in c for c in caveats)
    assert "contract:radar_user_record@local" in trace["reached"]["contracts"]
    assert not any("not in this build" in c for c in trace_caveats)


def test_radar_public_reads_github_only_through_gh_when_asked(monkeypatch, tiny_build):
    import base64

    _bdir, business = tiny_build
    calls: list[list[str]] = []
    real_run = subprocess.run

    def fake_run(cmd, *a, **k):
        if cmd[0] != "gh":
            return real_run(cmd, *a, **k)
        calls.append(cmd)
        path = cmd[2]
        if path.endswith("commits/main"):
            body = {"sha": "bc4fa90aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
        elif path.endswith(spec.RADAR_SCHEMA):
            old_schema = json.dumps({"required": ["user_id"], "properties": {}})
            body = {"content": base64.b64encode(old_schema.encode()).decode()}
        else:
            body = {"content": base64.b64encode(b'"maya_inference_record.json"').decode()}
        return subprocess.CompletedProcess(cmd, 0, json.dumps(body), "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    export = mf.resolve_path(business["exports"]["dir"])
    sample = mf.resolve_path(business["inputs"]["sample_dir"])
    assert lg.assemble(REPO, "full", sample_dir=sample, export_dir=export).inputs.get("radar-public:main") is None
    assert calls == []                                          # no flag, no call
    g = lg.assemble(REPO, "full", sample_dir=sample, export_dir=export, radar_public=True)
    assert [c[:2] for c in calls] == [["gh", "api"]] * 3 and g.inputs["radar-public:main"] == "bc4fa90"
    public = g.props("contract:radar_user_record@public-main")
    assert json.loads(public["detail"])["compatible"] is False  # the public schema is the old contract
    assert g.props(f"repo:{spec.RADAR_URL}")["public_main_head"] == "bc4fa90"
