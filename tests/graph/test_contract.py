"""Graph contract tests: the contract passes on real builds, catches planted defects, and the
committed goldens equal the numbers measured in the plan (PLAN 6.6)."""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime

import pandas as pd
import pytest
from conftest import REPO, make_exports, run_script

from lakehouse_graph import build, oracle, spec, store
from lakehouse_graph import manifest as mf

MAYA = "sub_maya:2026-10-07"
MAYA_EVIDENCE = [  # (event_date, relation, target_id, in_feature_window): PLAN 6.6, identical on tiny and seed 42
    ("2026-08-15", "CUT_CAP", "cap-cut-2026-08", True),
    ("2026-08-25", "EXPOSED_TO", "inc-002", False),
    ("2026-09-09", "EXPOSED_TO", "inc-003", True),
    ("2026-09-20", "CUT_CAP", "cap-cut-2026-09", True),
    ("2026-09-20", "FIRST_RENEWAL_AFTER", "cap-cut-2026-09", True),
    ("2026-09-24", "HIT_LIMIT", "lh:sub_maya:001", True),
    ("2026-09-25", "HIT_LIMIT", "lh:sub_maya:002", True),
    ("2026-09-27", "HIT_LIMIT", "lh:sub_maya:003", True),
]


def _contract(bdir, *flags):
    return run_script("check_graph_contract.py", "--build", str(bdir), *flags)


def _golden(name):
    return json.loads((REPO / "src/lakehouse_graph/goldens" / f"{name}.json").read_text())["values"]


def _evidence_rows(hero):
    return [(e["event_date"], e["relation"], e["target_id"], e["in_feature_window"]) for e in hero["evidence"]]


# --------------------------------------------------------------------------- tiny: contract behaviour
def test_contract_strict_passes_on_tiny(tiny_build):
    bdir, man = tiny_build
    p = _contract(bdir, "--strict")
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr
    assert "Graph contract OK" in p.stdout and "golden tiny" in p.stdout and "FAIL" not in p.stdout
    for phrase in ("Ladybug node counts", "PIT parity mismatches limit_hits_14d over 121 renewals = 0",
                   "naive limit_hits_14d wrong = 16", "naive incident_exposed_28d wrong = 12",
                   "naive support_tickets_90d wrong = 4", "IF as_of-filtered = 5", "BILLED on/before as_of = 4",
                   "for 121 rows x 30 columns", "byte-identical Parquet", "unchanged by this check",
                   "Cypher templates have ORDER BY and LIMIT", "leak lint: 17 tool templates",
                   "15 templates are contract-only", "max RSS"):
        assert phrase in p.stdout, phrase
    rec = store.read_contract(bdir)
    assert rec["status"] == "pass" and rec["strict"] is True and rec["golden"] == "tiny"
    assert rec["business_build_id"] == man["business_build_id"] and rec["errors"] == [] and rec["warnings"] == []


def test_oracle_print_golden_reproduces_the_committed_file(tiny_build):
    bdir, _ = tiny_build
    p = subprocess.run([sys.executable, "-m", "lakehouse_graph.oracle", "--build", str(bdir), "--print-golden"],
                       capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(REPO / "src")},
                       check=False)
    assert p.returncode == 0, p.stderr
    doc = json.loads(p.stdout)
    committed = json.loads((REPO / "src/lakehouse_graph/goldens/tiny.json").read_text())
    assert doc["values"] == committed["values"]
    assert doc["key"]["inputs_combined_sha256"] == committed["key"]["inputs_combined_sha256"]
    assert (committed["key"]["seed"], committed["key"]["n_users"]) == (42, 120)


def test_refresh_golden_never_overwrites_silently(tiny_build, tmp_path):
    """oracle.refresh_golden (make graph-golden): shows the diff, writes only when told to."""
    bdir, _ = tiny_build
    gdir = tmp_path / "goldens"
    path = gdir / "tiny.json"
    logs: list[str] = []
    assert oracle.refresh_golden("tiny", bdir, golden_dir=gdir, log=logs.append) == "differs"
    assert not path.exists() and any("NOT written: rerun with CONFIRM=1" in m for m in logs)
    assert oracle.refresh_golden("tiny", bdir, write=True, golden_dir=gdir, log=logs.append) == "written"
    text = path.read_text()
    assert text == oracle.golden_text(bdir) and json.loads(text)["values"] == _golden("tiny")
    assert oracle.refresh_golden("tiny", bdir, golden_dir=gdir, log=logs.append) == "unchanged"
    assert oracle.refresh_golden("tiny", bdir, write=True, golden_dir=gdir, log=logs.append) == "unchanged"

    doc = json.loads(text)
    doc["values"]["counts"]["nodes"]["Ticket"] += 1         # a real difference
    doc["values"]["goldens"]["hero"]["top10"][0]["d2_q"] += 1   # within the +-1 quantum tolerance: text only
    doc["measured_on"]["platform"] = "somewhere_else"
    stale = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    path.write_text(stale)
    logs.clear()
    assert oracle.refresh_golden("tiny", bdir, golden_dir=gdir, log=logs.append) == "differs"
    assert path.read_text() == stale                        # untouched without write=True
    out = "\n".join(logs)
    assert "DIFFERS: 1 golden value(s) and 1 key / platform / version field(s)" in out
    assert "counts.nodes.Ticket: golden 34 != 33" in out and "measured_on.platform: golden 'somewhere_else'" in out
    assert "--- tiny.json (committed)" in out and "+++ tiny.json (fresh build)" in out and '-    "Ticket": 34' in out
    assert "NOT written: rerun with CONFIRM=1" in out and not list(gdir.glob(".*"))
    assert oracle.refresh_golden("tiny", bdir, write=True, golden_dir=gdir, log=logs.append) == "written"
    assert path.read_text() == text and any("WRITTEN" in m for m in logs)
    assert set(oracle.GOLDEN_PROFILES) == {p.stem for p in (REPO / "src/lakehouse_graph/goldens").glob("*.json")}
    assert oracle.GOLDEN_PROFILES == {"tiny": ("tiny", 42, 120), "s42": ("s42", 42, 8000)}


def _same_platform_as(golden_doc) -> bool:
    """True when a fresh build here would record the same `measured_on` as the committed golden."""
    import platform

    here = {"platform": mf.platform_tag(), "versions": {**mf.package_versions(), "python": platform.python_version()}}
    return golden_doc["measured_on"] == here


@pytest.mark.slow
def test_graph_golden_command_diffs_and_needs_confirmation(tmp_path, user_data_untouched):
    """build_graph_local.py golden (make graph-golden): fresh build in a scratch GRAPH_ROOT; the committed
    golden is reproduced; nothing is written without --write (CONFIRM=1)."""
    committed_path = REPO / "src/lakehouse_graph/goldens/tiny.json"
    committed = committed_path.read_bytes()
    gdir = tmp_path / "goldens"
    scratch = tmp_path / "scratch"
    common = ("golden", "--only", "tiny", "--golden-dir", str(gdir), "--scratch", str(scratch))
    p = run_script("build_graph_local.py", *common)
    assert p.returncode == 1 and "Nothing was written: rerun with CONFIRM=1" in p.stderr, p.stdout + p.stderr
    assert "diff only; CONFIRM=1 writes" in p.stdout and not (gdir / "tiny.json").exists()
    p = run_script("build_graph_local.py", *common, "--write")
    assert p.returncode == 0 and "graph-golden OK: tiny.json written" in p.stdout, p.stdout + p.stderr
    fresh = json.loads((gdir / "tiny.json").read_text())
    doc = json.loads(committed)
    assert oracle.diff(doc["values"], fresh["values"]) == [] and fresh["key"] == doc["key"]
    assert fresh["key"]["seed_n_status"] == "verified"
    if _same_platform_as(doc):                              # the committed file, byte for byte
        assert (gdir / "tiny.json").read_bytes() == committed
    p = run_script("build_graph_local.py", *common)
    assert p.returncode == 0 and "graph-golden OK: tiny.json unchanged" in p.stdout
    assert mf.read_manifest(spec.latest_link("tiny", scratch))["profile"] == "tiny"    # --scratch is kept
    p = run_script("build_graph_local.py", "golden", "--only", "s7")
    assert p.returncode == 2 and "no such golden" in p.stderr
    assert committed_path.read_bytes() == committed and mf.guarded_hashes() == user_data_untouched


def test_tiny_golden_equals_plan_numbers():
    v = _golden("tiny")
    inv = v["invariants"]
    assert (v["counts"]["total_nodes"], v["counts"]["total_edges"]) == (616, 1949)
    assert v["routes"] == {"cancel_flow": 4, "dunning": 8, "model": 108, "score_today": 1} and v["model_lapses"] == 9
    assert set(inv["parity_mismatches"].values()) == {0} and len(inv["parity_mismatches"]) == 6
    assert inv["naive_mismatches"] == {"limit_hits_14d": 16, "incident_exposed_28d": 12, "support_tickets_90d": 4}
    assert inv["first_renewal_after_as_of_filtered_mismatches"] == 5
    assert inv["billed"]["on_or_before_as_of"] == 4 and inv["billed"]["cancel_flow_iff_mismatches"] == 0
    ls = inv["leak_surface"]
    assert (ls["post_as_of_total"], ls["event_edges"]) == (235, 519)
    assert ls["by_type"]["FIRST_RENEWAL_AFTER"] == {"edges": 38, "post_as_of": 5, "renewals": 5}
    s = inv["similar_to"]
    assert s["out_degree_histogram"] == {"10": 117, "3": 4} and s["distinct_dst"] == 105
    assert (s["mutual_pairs"], s["undirected_edges"], s["weak_components_all_nodes"]) == (294, 888, 3)
    assert s["reference_components"] == [89, 15, 4]
    assert (s["max_in_degree"], s["max_in_degree_renewal"]) == (42, "sub_00036:2026-07-23")
    assert s["cut_quantised_ties_broken_by_dst"] == 0 and s["exact_halves"] == 0
    hero = v["goldens"]["hero"]
    assert (hero["renewal_id"], hero["as_of"], hero["route"], hero["plan_tier"], hero["city"]) == \
        (MAYA, "2026-09-30", "score_today", "pro", "Pune")
    assert _evidence_rows(hero) == MAYA_EVIDENCE and hero["evidence_hidden_after_as_of"] == 0
    assert [x["renewal_id"] for x in hero["top10"]] == [
        "sub_00052:2026-09-09", "sub_00020:2026-09-03", "sub_00088:2026-07-20", "sub_00076:2026-08-17",
        "sub_00051:2026-08-23", "sub_00014:2026-08-23", "sub_00045:2026-09-12", "sub_00118:2026-06-21",
        "sub_00019:2026-09-10", "sub_00053:2026-08-03"]
    assert {x["outcome"] for x in hero["top10"]} == {"renewed"}
    inc = v["goldens"]["exposure_incident"]["inc-002"]
    assert inc["by_plan"]["pro"] == {"exposed": 12, "model": 12, "voluntary_lapses": 1, "cancel_flow": 0, "dunning": 0}
    assert inc["naive_additional"] == 2


def test_oracle_catches_a_planted_leak_and_a_wrong_feature(tiny_build):
    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    assert set(oracle.pit_parity(t)["parity_mismatches"].values()) == {0}
    # 1. a gold feature that read one event after as_of (what a naive traversal would produce)
    leaky = dict(t)
    ren = t["Renewal"].copy()
    ren.loc[ren.index[0], "limit_hits_14d"] += 1
    leaky["Renewal"] = ren
    assert oracle.pit_parity(leaky)["parity_mismatches"]["limit_hits_14d"] == 1
    # 2. an event moved from after as_of into the window must show up as a parity mismatch
    moved = dict(t)
    hl = t["HIT_LIMIT"].copy()
    as_of = hl["src"].map(t["Renewal"].set_index("subscription_id")["as_of"])
    post = hl.index[hl["event_date"] > as_of][0]
    hl.loc[post, "event_date"] = as_of[post]
    moved["HIT_LIMIT"] = hl
    assert oracle.pit_parity(moved)["parity_mismatches"]["limit_hits_14d"] == 1
    # 3. a BILLED outcome edge mislabelled as non-evidence is caught
    bad = dict(t)
    b = t["BILLED"].copy()
    b.loc[b.index[b["outcome_evidence"]][0], "outcome_evidence"] = False
    bad["BILLED"] = b
    assert oracle.billed(bad)["outcome_evidence_flag_mismatches"] == 1
    # 4. a cross-plan or self SIMILAR_TO edge is caught
    s = t["SIMILAR_TO"].copy()
    s.loc[s.index[0], "dst"] = s.loc[s.index[0], "src"]
    assert oracle.similar_to_invariants({**t, "SIMILAR_TO": s})["self_loops"] == 1


def test_contract_fails_on_a_tampered_build(tiny_build, tmp_path):
    bdir, _ = tiny_build
    copy = tmp_path / "builds" / bdir.name
    shutil.copytree(bdir, copy)
    path = copy / "parquet/edges_HIT_LIMIT.parquet"
    df = pd.read_parquet(path)
    build.write_parquet(df.iloc[1:].assign(event_date=lambda d: pd.to_datetime(d["event_date"])), path,
                        spec.EDGE_SCHEMA["HIT_LIMIT"].schema)
    p = _contract(copy)
    assert p.returncode == 1
    assert "Graph contract FAILED" in p.stderr and "Parquet files match the manifest sha256" in p.stderr
    assert "count(LimitHit) = count(HIT_LIMIT)" in p.stderr and "Ladybug edge counts" in p.stderr
    assert store.read_contract(copy)["status"] == "fail"


def _contract_module():
    mspec = importlib.util.spec_from_file_location("check_graph_contract_under_test",
                                                   REPO / "scripts/check_graph_contract.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    return mod


def test_contract_record_is_written_under_the_build_lock_after_a_recheck(tmp_path, capsys):
    """contract.json is written under the BuildLock, and only while the directory still holds the
    build that was checked: a concurrent --rebuild can neither drop a record nor receive one that
    describes other bytes."""
    chk = _contract_module()
    root = tmp_path / "g"
    bdir, man = build.build_profile("tiny", graph_root=root, log=lambda *_: None)
    shas = {k: v["sha256"] for k, v in man["files"].items()}
    doc = {"status": "pass", "strict": True, "business_build_id": man["business_build_id"], "files_sha256": shas,
           "exports_sha256": man["exports"]["sha256"], "checked_at": "2026-09-30T12:00:00Z"}
    with store.BuildLock(root, timeout=5), pytest.raises(TimeoutError, match="holds"):
        chk.record(bdir, man, doc, lock_timeout=0.3)        # a build holds the lock: the record waits for it
    assert store.read_contract(bdir) is None
    assert chk.record(bdir, man, doc) == "written" and store.read_contract(bdir) == doc
    assert store.is_strict_pass(store.read_contract(bdir), man)
    with store.BuildLock(root, timeout=1):                  # and the lock is released again
        pass
    assert chk.record(bdir, man, {**doc, "strict": False, "checked_at": "later"}) == "kept"
    assert store.read_contract(bdir) == doc                 # a casual pass never replaces a valid strict one

    first = next(iter(man["files"]))
    other = {**man, "files": {**man["files"], first: {**man["files"][first], "sha256": "0" * 64}}}
    mf.write_manifest(bdir, other)                          # the directory now holds a build with other Parquet
    for status in ("pass", "fail"):
        assert chk.record(bdir, man, {**doc, "status": status, "checked_at": "later"}) == "changed"
    assert store.read_contract(bdir) == doc
    mf.write_manifest(bdir, {**man, "business_build_id": "0" * 12})
    assert chk.record(bdir, man, {**doc, "checked_at": "later"}) == "changed"
    (bdir / mf.MANIFEST_FILE).unlink()                      # or no readable build at all
    assert chk.record(bdir, man, {**doc, "checked_at": "later"}) == "changed" and store.read_contract(bdir) == doc
    mf.write_manifest(bdir, man)

    original = (bdir / first).read_bytes()
    (bdir / first).write_bytes(original + b"tampered")      # bytes changed under a passing check: no pass on record
    assert chk.record(bdir, man, {**doc, "checked_at": "later"}) == "changed" and store.read_contract(bdir) == doc
    failed = {**doc, "status": "fail", "checked_at": "later"}
    assert chk.record(bdir, man, failed) == "written" and store.read_contract(bdir) == failed   # a failure is recorded
    (bdir / first).write_bytes(original)

    loose = tmp_path / "loose-copy"                         # outside a builds tree nothing is ever swapped: no lock
    shutil.copytree(bdir, loose)
    with store.BuildLock(tmp_path, timeout=5):
        assert chk.record(loose, man, doc, lock_timeout=0.2) == "written" and store.read_contract(loose) == doc
    assert capsys.readouterr().out.count("waiting for the build lock") == 1     # only the first, refused, record waited


@pytest.mark.slow
def test_contract_check_waits_for_the_build_lock_end_to_end(tmp_path):
    root = tmp_path / "g"
    bdir, man = build.build_profile("tiny", graph_root=root, log=lambda *_: None)
    with store.BuildLock(root, timeout=5):
        p = _contract(bdir, "--lock-timeout", "0.5")
    assert p.returncode == 1 and f"{store.CONTRACT_FILE} not written" in p.stderr and "holds" in p.stderr
    assert "Graph contract OK" not in p.stdout and store.read_contract(bdir) is None
    p = _contract(bdir)                                     # lock free again; no exports: a plain pass with a warning
    assert p.returncode == 0 and "Graph contract OK" in p.stdout
    rec = store.read_contract(bdir)
    assert rec["status"] == "pass" and rec["business_build_id"] == man["business_build_id"]
    with store.BuildLock(root, timeout=1):                  # the check released the lock
        pass


@pytest.mark.slow
def test_exports_are_optional_unless_strict(tiny_build, tmp_path):
    fixture = REPO / spec.TINY_FIXTURE
    bdir, _ = build.build_profile("tiny", graph_root=tmp_path / "root", sample_dir=fixture,
                                  export_dir=tmp_path / "no_exports", log=lambda *_: None)
    p = _contract(bdir)
    assert p.returncode == 0 and "export cross-check skipped" in p.stdout + p.stderr
    p = _contract(bdir, "--strict")
    assert p.returncode == 1 and "fatal with --strict" in p.stderr


def test_export_crosscheck_detects_a_changed_cell(tiny_build, tmp_path):
    bdir, man = tiny_build
    t = oracle.load_tables(bdir)
    edir = mf.resolve_path(man["exports"]["dir"])
    assert oracle.export_crosscheck(t, edir)["status"] == "ok"
    other = tmp_path / "export"
    shutil.copytree(edir, other)
    audit = pd.read_csv(other / "churn_renewals_audit.csv", dtype=str, keep_default_na=False)
    audit.loc[3, "limit_hits_14d"] = "99"
    audit.to_csv(other / "churn_renewals_audit.csv", index=False)
    res = oracle.export_crosscheck(t, other)
    assert res["status"] == "mismatch" and res["mismatches"]["limit_hits_14d"]["rows"] == 1
    assert res["cells"] == [[audit.at[3, "user_id"], "limit_hits_14d"]] and res["cells_complete"]
    assert oracle.export_crosscheck(t, tmp_path / "missing")["status"] == "missing"


def test_export_numbers_compare_by_value_and_mismatches_name_their_cells(tiny_build, tmp_path):
    """The Spark export writes decimal(31,4) as '0.8000' where pandas writes '0.8': the same number,
    not a mismatch. A real difference still is one, and the cross-check names its (user_id, column)
    cells, so the source-aware contract can tell a recorded gold drift from anything else."""
    bdir, man = tiny_build
    t = oracle.load_tables(bdir)
    other = tmp_path / "export"
    shutil.copytree(mf.resolve_path(man["exports"]["dir"]), other)
    path = other / "churn_renewals_audit.csv"
    audit = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in ("engagement_trend", "accept_rate_change", "overage_usd_28d"):
        audit[col] = audit[col].map(lambda s: s + "00" if "." in s else s)      # same value, Spark's padding
    audit.to_csv(path, index=False)
    res = oracle.export_crosscheck(t, other)
    assert res["status"] == "ok" and res["cells"] == [] and res["cells_complete"], res["mismatches"]
    audit.loc[5, "engagement_trend"] = str(float(audit.at[5, "engagement_trend"]) + 0.5)
    audit.loc[7, "plan_tier"] = "not_a_plan"
    audit.to_csv(path, index=False)
    res = oracle.export_crosscheck(t, other)
    assert res["status"] == "mismatch" and set(res["mismatches"]) == {"engagement_trend", "plan_tier"}
    assert sorted(map(tuple, res["cells"])) == sorted([(audit.at[5, "user_id"], "engagement_trend"),
                                                       (audit.at[7, "user_id"], "plan_tier")])


def test_golden_is_selected_by_input_hash(tiny_build):
    _, man = tiny_build
    name, golden, note, exact = oracle.find_golden(man)
    assert name == "tiny" and exact and golden["key"]["inputs_combined_sha256"] == man["inputs"]["combined_sha256"]
    # same seed / N_USERS but different bronze bytes: still compared, flagged as not exact
    other = {**man, "inputs": {**man["inputs"], "combined_sha256": "0" * 64}}
    name, golden, note, exact = oracle.find_golden(other)
    assert name == "tiny" and not exact and "bronze sha256 differs" in note
    name, golden, note, exact = oracle.find_golden({**other, "seed": 7, "n_users": 50})
    assert name is None and golden is None and not exact and "no committed golden" in note
    # tie-aware: d2_q may move by one quantum across platforms; anything else is exact
    assert oracle.diff({"d2_q": 10, "n": 1}, {"d2_q": 11, "n": 1}) == []
    assert oracle.diff({"d2_q": 10}, {"d2_q": 12}) and oracle.diff({"n": 1}, {"n": 2}) and oracle.diff([1], [1, 2])


@pytest.mark.slow
def test_golden_fallback_by_seed_when_bronze_bytes_differ(tmp_path):
    """Same data, different bytes (as another platform might write them): goldens still compared."""
    sample = tmp_path / "sample"
    shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
    with open(sample / "incidents.csv", "a") as f:
        f.write("\n")                                       # a blank line: same rows, new sha256
    make_exports(sample, tmp_path / "export")
    bdir, man = build.build_profile("tiny", graph_root=tmp_path / "root", sample_dir=sample,
                                    export_dir=tmp_path / "export", log=lambda *_: None)
    assert oracle.find_golden(man)[3] is False
    p = _contract(bdir, "--strict")
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr
    assert "bronze sha256 differs" in p.stdout and "every golden value matches" in p.stdout

    # different data under the same seed / N_USERS: a warning with the differing values (strict fails)
    lines = (sample / "support_tickets.csv").read_text().splitlines()
    (sample / "support_tickets.csv").write_text("\n".join(lines[:-1]) + "\n")
    make_exports(sample, tmp_path / "export")
    bdir, man = build.build_profile("tiny", graph_root=tmp_path / "root", sample_dir=sample,
                                    export_dir=tmp_path / "export", log=lambda *_: None)
    p = _contract(bdir)
    assert p.returncode == 0 and "golden value(s) differ" in p.stderr and "counts.nodes.Ticket" in p.stderr
    assert _contract(bdir, "--strict").returncode == 1


@pytest.mark.slow
def test_make_targets_never_touch_user_data(tmp_path, user_data_untouched):
    """graph-sample + graph-local through the Makefile with a scratch GRAPH_ROOT (tiny profile),
    twice: regenerated exports must not break graph-local (the build is kept and re-pinned)."""
    if shutil.which("make") is None:
        pytest.skip("make not available")
    root = tmp_path / "make_root"
    common = ["make", "--no-print-directory", f"GRAPH_ROOT={root}", f"GRAPH_PY={sys.executable}"]

    def make(*args):
        return subprocess.run([*common, *args], cwd=REPO, capture_output=True, text=True, check=False)

    p = make("graph-sample", "PROFILE=default")
    assert p.returncode != 0 and "PROFILE=default is refused" in p.stderr
    p = make("graph-sample", "PROFILE=tiny")
    assert p.returncode == 0, p.stdout + p.stderr
    assert "EXPORTS ONLY" in p.stdout and "never regenerated" in p.stdout and "exports only" in p.stdout
    assert sorted(x.name for x in (root / "tiny/export").iterdir()) == sorted(spec.EXPORT_FILES)
    assert not (root / "tiny/sample").exists()              # tiny bronze stays the committed fixture
    meta = json.loads((root / "tiny/sample_meta.json").read_text())
    assert (meta["seed"], meta["n_users"]) == (42, 120) and "sha256 match" in meta["method"]
    p = make("graph-local", "PROFILE=tiny")
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr
    assert "Graph contract OK" in p.stdout and "Repo contracts OK: 0 errors, 2 warnings" in p.stdout
    man = mf.read_manifest(spec.latest_link("tiny", root))
    assert man["seed_n_status"] == "verified" and man["guarded_sha256"] == user_data_untouched

    # graph-local is idempotent: regenerated exports (new built_at) re-pin the kept build
    audit = root / "tiny/export/churn_renewals_audit.csv"
    first_built_at = pd.read_csv(audit)["built_at"].iloc[0]
    while datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S") == first_built_at:
        time.sleep(0.2)                                     # built_at has one-second resolution
    p = make("graph-sample", "PROFILE=tiny")
    assert p.returncode == 0, p.stdout + p.stderr
    assert mf.export_hashes(root / "tiny/export") != man["exports"]["sha256"]
    p = make("graph-check", "PROFILE=tiny")                 # checked alone: says how to fix it
    assert p.returncode != 0 and "run make graph-build PROFILE=tiny to re-pin" in p.stderr
    p = make("graph-local", "PROFILE=tiny")
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr
    assert "unchanged" in p.stdout and "re-pinned in manifest.json: exports sha256" in p.stdout
    assert "Graph contract OK" in p.stdout
    man2 = mf.read_manifest(spec.latest_link("tiny", root))
    assert man2["business_build_id"] == man["business_build_id"] and man2["built_at"] == man["built_at"]
    assert man2["exports"]["sha256"] == mf.export_hashes(root / "tiny/export") and man2["files"] == man["files"]
    p = make("graph-local", "PROFILE=tiny")                 # and again, with nothing changed
    assert p.returncode == 0 and "re-pinned" not in p.stdout, p.stdout[-3000:] + p.stderr
    p = make("graph-build", "PROFILE=tiny", "GRAPH_BUILD_FLAGS=--rebuild")   # a forced replacement via make
    assert p.returncode == 0 and "unchanged" not in p.stdout, p.stdout + p.stderr
    assert mf.read_manifest(spec.latest_link("tiny", root))["built_at"] >= man["built_at"]

    p = make("graph-sample", "PROFILE=current")
    assert p.returncode != 0 and "reserved" in p.stderr and "must be integers" not in p.stderr
    p = make("graph-promote")
    assert p.returncode != 0 and "no strict contract pass recorded for any default-profile build" in p.stderr
    p = make("graph-promote", "PROFILE=tiny")               # a passing tiny build is still not promotable
    assert p.returncode != 0 and "only the default profile can be promoted (got 'tiny')" in p.stderr
    p = make("graph-clean")
    assert p.returncode == 0 and "Graph gc tiny: kept 1" in p.stdout

    # explicit messages: a name no seed can be derived from, and SEED / N_USERS that tiny cannot honour
    p = make("graph-sample", "PROFILE=staging")
    assert p.returncode != 0 and "no seed can be derived" in p.stderr and "s<digits>" in p.stderr
    assert "must be integers" not in p.stderr and "SEED=" not in p.stderr    # no leftover "SEED='taging'"
    p = make("graph-build", "PROFILE=staging")
    assert p.returncode != 0 and "no seed can be derived" in p.stderr
    for var, shown in (("SEED=7", "SEED=7"), ("N_USERS=500", "N_USERS=500")):
        p = make("graph-sample", "PROFILE=tiny", var)
        assert p.returncode != 0 and f"{shown} cannot apply" in p.stderr and "EXPORTS ONLY" in p.stderr
    p = make("graph-sample", "PROFILE=s7", "SEED=x")
    assert p.returncode != 0 and "SEED and N_USERS must be integers" in p.stderr
    assert not (root / "staging").exists() and not (root / "s7").exists()
    p = make("help")
    for phrase in ("EXPORTS ONLY", "PROFILE=inject", "ONE poisoned user_name", "PROFILE=default refused",
                   "make graph-golden", "CONFIRM=1"):
        assert phrase in p.stdout, phrase

    # the inject profile through make: its own bronze copy + exports, strict contract, never promotable
    p = make("graph-sample", "PROFILE=inject")
    assert p.returncode == 0 and "1 poisoned user_name (sub_00052)" in p.stdout, p.stdout + p.stderr
    snaps = (root / "inject/sample/subscription_snapshots.csv").read_text()
    assert snaps.count(spec.INJECT_USER_NAME) == 1
    p = make("graph-local", "PROFILE=inject")
    assert p.returncode == 0 and "Graph contract OK" in p.stdout, p.stdout[-3000:] + p.stderr

    # graph-golden: a fresh tiny build in a scratch GRAPH_ROOT reproduces the committed golden; no CONFIRM, no write
    golden = REPO / "src/lakehouse_graph/goldens/tiny.json"
    before = golden.read_bytes()
    p = make("graph-golden", "ONLY=tiny")
    if _same_platform_as(json.loads(before)):
        assert p.returncode == 0 and "graph-golden OK: tiny.json unchanged" in p.stdout, p.stdout + p.stderr
    else:                                                   # another platform: same values, different header
        assert p.returncode != 0 and "DIFFERS: 0 golden value(s)" in p.stdout, p.stdout + p.stderr
        assert "Nothing was written: rerun with CONFIRM=1" in p.stderr
    assert golden.read_bytes() == before and "diff only; CONFIRM=1 writes" in p.stdout
    assert mf.guarded_hashes() == user_data_untouched
    assert not (root / "current").exists()


# --------------------------------------------------------------------------- seed 42 (slow)
@pytest.mark.slow
def test_contract_strict_passes_on_seed_42(s42_build):
    bdir, _ = s42_build
    p = _contract(bdir, "--strict")
    assert p.returncode == 0, p.stdout[-4000:] + p.stderr
    assert "golden s42" in p.stdout and "FAIL" not in p.stdout
    for phrase in ("40,204 nodes", "130,366 edges", "over 8,001 renewals = 0", "naive limit_hits_14d wrong = 1,165",
                   "naive incident_exposed_28d wrong = 681", "naive support_tickets_90d wrong = 114",
                   "IF as_of-filtered = 495", "BILLED on/before as_of = 287", "for 8,001 rows x 30 columns",
                   "byte-identical Parquet"):
        assert phrase in p.stdout, phrase
    assert store.read_contract(bdir)["golden"] == "s42"


@pytest.mark.slow
def test_s42_oracle_equals_committed_golden(s42_build):
    bdir, man = s42_build
    name, golden, _, exact = oracle.find_golden(man)
    assert name == "s42" and exact
    assert oracle.diff(golden["values"], oracle.compute(bdir)) == []


def test_s42_golden_equals_plan_numbers():
    """The committed seed-42 golden (generated by the oracle) against PLAN 6.6, number by number."""
    v = _golden("s42")
    inv = v["invariants"]
    assert v["counts"]["nodes"] == {"Subscription": 8001, "Renewal": 8001, "Plan": 3, "Incident": 3,
                                    "PricingChange": 2, "LimitHit": 10602, "OverageChange": 849,
                                    "OverageCharge": 470, "Ticket": 2134, "BillingEvent": 10139}
    assert v["counts"]["edges"] == {"HAS_RENEWAL": 8001, "ON_PLAN": 8001, "HIT_LIMIT": 10602, "CHANGED_OVERAGE": 849,
                                    "CHARGED_OVERAGE": 470, "OPENED": 2134, "BILLED": 10139, "EXPOSED_TO": 7651,
                                    "FIRST_RENEWAL_AFTER": 2503, "CUT_CAP": 6, "SIMILAR_TO": 80010}
    assert (v["counts"]["total_nodes"], v["counts"]["total_edges"]) == (40204, 130366)
    assert v["routes"] == {"cancel_flow": 287, "dunning": 326, "model": 7387, "score_today": 1}
    assert v["model_lapses"] == 548
    assert v["model_lapses_by_plan"] == {"pro": {"n": 5815, "lapses": 464}, "pro_plus": {"n": 1258, "lapses": 73},
                                         "ultra": {"n": 314, "lapses": 11}}
    assert inv["subscriptions_without_exactly_one_renewal"] == 0
    assert inv["parity_mismatches"] == {f: 0 for f in ("limit_hits_14d", "support_tickets_90d", "overage_usd_28d",
                                                       "overage_toggled_off", "incident_exposed_28d",
                                                       "first_renewal_after_pricing_change")}
    assert inv["first_renewal_after_as_of_filtered_mismatches"] == 495
    assert inv["naive_mismatches"] == {"limit_hits_14d": 1165, "incident_exposed_28d": 681,
                                       "support_tickets_90d": 114}
    b = inv["billed"]
    assert (b["on_or_before_as_of"], b["on_or_before_all_cancel_scheduled"], b["cancel_flow_iff_mismatches"],
            b["cancel_flow_renewals"]) == (287, True, 0, 287)
    ls = inv["leak_surface"]
    assert {k: x["post_as_of"] for k, x in ls["by_type"].items()} == {
        "HIT_LIMIT": 3000, "OPENED": 114, "EXPOSED_TO": 1401, "BILLED": 9852, "FIRST_RENEWAL_AFTER": 495,
        "CHANGED_OVERAGE": 0, "CHARGED_OVERAGE": 0}
    assert {k: x["renewals"] for k, x in ls["by_type"].items()} == {
        "HIT_LIMIT": 1165, "OPENED": 114, "EXPOSED_TO": 985, "BILLED": 8000, "FIRST_RENEWAL_AFTER": 495,
        "CHANGED_OVERAGE": 0, "CHARGED_OVERAGE": 0}
    assert (ls["post_as_of_total"], ls["event_edges"]) == (14862, 34348)
    assert ls["declared_exception_by_change"] == {"cap-cut-2026-08": 495}
    assert ls["declared_exception_on_model_rows"] == 455
    s = inv["similar_to"]
    assert s["out_degree_histogram"] == {"10": 8001} and s["out_degree_not_k_eff"] == 0
    assert (s["cross_plan_edges"], s["dst_not_model"], s["self_loops"]) == (0, 0, 0)
    assert (s["distinct_dst"], s["reference_never_chosen"], s["reference_rows"]) == (7281, 106, 7387)
    assert (s["mutual_pairs"], s["undirected_edges"]) == (19569, 60441)
    assert s["weak_components_all_nodes"] == 4 and s["reference_components"] == [5760, 1258, 314, 55]
    assert (s["max_in_degree"], s["max_in_degree_renewal"]) == (49, "sub_00440:2026-06-16")
    assert s["cut_quantised_ties_broken_by_dst"] == 37 and s["exact_halves"] == 0
    assert s["spot_check_mismatches"] == 0 and s["d2_q_key_mismatches"] == 0

    g = v["goldens"]
    hero = g["hero"]
    assert (hero["renewal_id"], hero["as_of"], hero["route"], hero["plan_tier"], hero["city"]) == \
        (MAYA, "2026-09-30", "score_today", "pro", "Pune")
    assert _evidence_rows(hero) == MAYA_EVIDENCE and hero["evidence_hidden_after_as_of"] == 0
    assert [e["detail"] for e in hero["evidence"] if e["relation"] == "HIT_LIMIT"] == ["weekly"] * 3
    fra = next(e for e in hero["evidence"] if e["relation"] == "FIRST_RENEWAL_AFTER")
    assert fra["known_by_as_of"] is True and fra["declared_exception"] is False
    assert [(x["rank"], x["renewal_id"], x["d2_q"], x["outcome"]) for x in hero["top10"]] == [
        (1, "sub_07200:2026-08-17", 5099099315, "voluntary_lapse"),
        (2, "sub_06614:2026-08-21", 5593304415, "renewed"),
        (3, "sub_01541:2026-08-16", 5616850275, "renewed"),
        (4, "sub_04760:2026-09-09", 6300541441, "renewed"),
        (5, "sub_01355:2026-09-05", 6604184020, "voluntary_lapse"),
        (6, "sub_05564:2026-08-20", 7250182869, "renewed"),
        (7, "sub_01475:2026-08-19", 7362875678, "renewed"),
        (8, "sub_04856:2026-08-22", 7371318997, "renewed"),
        (9, "sub_01888:2026-08-23", 7506448249, "renewed"),
        (10, "sub_00228:2026-08-03", 7713654324, "renewed")]
    assert {x["route"] for x in hero["top10"]} == {"model"} and hero["top10_voluntary_lapses"] == 2
    assert [(x["renewal_id"], x["path_dist"]) for x in hero["nearest_lapses"]] == [
        ("sub_07200:2026-08-17", 2.2581), ("sub_01355:2026-09-05", 2.5699), ("sub_05762:2026-09-11", 4.6438)]
    assert hero["sharing_neighbours_by_route"] == {"model": 84, "cancel_flow": 9, "dunning": 6}
    inc = g["exposure_incident"]["inc-002"]
    assert inc["by_plan"] == {
        "pro": {"exposed": 606, "model": 545, "voluntary_lapses": 57, "cancel_flow": 33, "dunning": 28},
        "pro_plus": {"exposed": 185, "model": 162, "voluntary_lapses": 10, "cancel_flow": 11, "dunning": 12},
        "ultra": {"exposed": 46, "model": 44, "voluntary_lapses": 5, "cancel_flow": 0, "dunning": 2}}
    assert inc["naive_additional"] == 329
    assert g["exposure_pricing_change"] == {"cap-cut-2026-08": {"total": 2502, "known_by_as_of_false": 495},
                                            "cap-cut-2026-09": {"total": 1, "known_by_as_of_false": 0}}
    assert g["motif_limit_hit_then_overage_off"] == {"renewals": 238, "lapses": 39, "rate": 0.1639}
    fa = g["first_renewal_after_by_plan"]
    assert [(p, fa[p]["without"]["n"], fa[p]["without"]["rate"], fa[p]["with"]["n"], fa[p]["with"]["rate"])
            for p in ("pro", "pro_plus", "ultra")] == [("pro", 4016, 0.0685, 1799, 0.1051),
                                                       ("pro_plus", 871, 0.0494, 387, 0.0775),
                                                       ("ultra", 208, 0.0144, 106, 0.0755)]
