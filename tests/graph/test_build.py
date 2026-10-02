"""Builder tests: schema, counts, determinism, build identity, SIMILAR_TO rule, housekeeping."""
from __future__ import annotations

import csv
import errno
import importlib.metadata
import io
import json
import os
import shutil
import subprocess
import sys
import threading
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from conftest import REPO, make_exports, run_script

from lakehouse_graph import build, oracle, queries, spec, store
from lakehouse_graph import manifest as mf

TINY_NODES = {"Subscription": 121, "Renewal": 121, "Plan": 3, "Incident": 3, "PricingChange": 2, "LimitHit": 150,
              "OverageChange": 12, "OverageCharge": 6, "Ticket": 33, "BillingEvent": 165}
TINY_EDGES = {"HAS_RENEWAL": 121, "ON_PLAN": 121, "HIT_LIMIT": 150, "CHANGED_OVERAGE": 12, "CHARGED_OVERAGE": 6,
              "OPENED": 33, "BILLED": 165, "EXPOSED_TO": 115, "FIRST_RENEWAL_AFTER": 38, "CUT_CAP": 6,
              "SIMILAR_TO": 1182}
S42_NODES = {"Subscription": 8001, "Renewal": 8001, "Plan": 3, "Incident": 3, "PricingChange": 2, "LimitHit": 10602,
             "OverageChange": 849, "OverageCharge": 470, "Ticket": 2134, "BillingEvent": 10139}
S42_EDGES = {"HAS_RENEWAL": 8001, "ON_PLAN": 8001, "HIT_LIMIT": 10602, "CHANGED_OVERAGE": 849,
             "CHARGED_OVERAGE": 470, "OPENED": 2134, "BILLED": 10139, "EXPOSED_TO": 7651,
             "FIRST_RENEWAL_AFTER": 2503, "CUT_CAP": 6, "SIMILAR_TO": 80010}
# GRAPH_REQUIRE_EXCHANGE=1 (set it in CI): a filesystem without the atomic exchange call fails the
# replace tests instead of silently exercising only the two-rename fallback (macOS renamex_np and
# Linux renameat2 are different code paths in store.exchange_paths).
REQUIRE_EXCHANGE = os.environ.get("GRAPH_REQUIRE_EXCHANGE") == "1"


# --------------------------------------------------------------------------- spec
def test_spec_feature_lists():
    assert len(spec.GOLD_FEATURES) == 22 and len(spec.NUMERIC_FEATURES) == 21 and len(spec.FEATURES) == 20
    assert "agent_requests_28d" not in spec.FEATURES and "plan_tier" not in spec.FEATURES
    assert list(spec.FEATURE_CARDS) == spec.GOLD_FEATURES
    exceptions = [f for f, c in spec.FEATURE_CARDS.items() if c["pit_status"] == "declared_exception"]
    assert exceptions == ["renewals_completed", "first_renewal_after_pricing_change"]
    verified = {f for f, c in spec.FEATURE_CARDS.items() if c["verification"] == "graph-verified"}
    assert verified == {"limit_hits_14d", "support_tickets_90d", "overage_usd_28d", "overage_toggled_off",
                        "incident_exposed_28d", "first_renewal_after_pricing_change"}
    assert len(spec.NODE_SCHEMA) == 10 and len(spec.EDGE_SCHEMA) == 11


def test_d2_quantise_rounds_half_up_not_half_even():
    # floor(x + 0.5): 0.5 -> 1, 1.5 -> 2, 2.5 -> 3 (np.rint would give 0, 2, 2)
    got = spec.d2_quantise(np.array([0.5e-9, 1.5e-9, 2.5e-9, 2.4999e-9, 5.0991]))
    assert got.tolist()[:4] == [1, 2, 3, 2] and got.dtype == np.int64
    assert int(got[4]) == int(np.floor(5.0991 * 1e9 + 0.5))


def test_ladybug_ddl_is_generated_from_spec():
    ddl = store.ddl_statements()
    assert len(ddl) == 21
    assert ddl[0].startswith("CREATE NODE TABLE Subscription(subscription_id STRING") and "PRIMARY KEY" in ddl[0]
    assert "CREATE REL TABLE SIMILAR_TO(FROM Renewal TO Renewal, rank INT64, d2 DOUBLE, d2_q INT64" in ddl[-1]
    assert not any(" Column" in s or " on " in s for s in ddl)  # Ladybug reserved words


# --------------------------------------------------------------------------- SIMILAR_TO rule
def _toy(rows):
    df = pd.DataFrame(rows, columns=["renewal_id", "plan_tier", "route", "x"])
    for f in spec.FEATURES:
        df[f] = 0.0
    df[spec.FEATURES[0]] = df["x"].astype(float)
    return df


def test_knn_blocks_by_plan_excludes_self_and_breaks_ties_by_id():
    ren = _toy([("a", "pro", "model", 0.0), ("b", "pro", "model", 1.0), ("c", "pro", "model", -1.0),
                ("d", "pro", "dunning", 0.0), ("u1", "ultra", "model", 0.0), ("u2", "ultra", "model", 5.0)])
    ref = ren[ren["route"] == "model"]
    e = build.knn(ren, ref, build.fit_scaler(ref), k=2, keep_extra=1)
    by = {s: g for s, g in e.groupby("src")}
    # a: b and c are equidistant -> tie broken by dst id; no self edge; k_eff = min(2, 3 - 1)
    assert by["a"][by["a"]["in_contract"]]["dst"].tolist() == ["b", "c"]
    assert by["a"]["d2_q"].iloc[0] == by["a"]["d2_q"].iloc[1]
    # d is not a candidate (route dunning) but is a source; it gets k = 2 of the 3 pro candidates + 1 cut row
    assert by["d"]["dst"].tolist() == ["a", "b", "c"] and by["d"]["in_contract"].tolist() == [True, True, False]
    assert not set(e["dst"]) & {"d"}
    # ultra block: 2 candidates, each source is a candidate -> k_eff = 1, never crosses plans
    assert by["u1"]["dst"].tolist() == ["u2"] and by["u2"]["dst"].tolist() == ["u1"]
    assert (e["src"] != e["dst"]).all()


def test_similar_to_std_zero_maps_to_zero_and_flags_mutual():
    ren = _toy([("a", "pro", "model", 0.0), ("b", "pro", "model", 1.0), ("c", "pro", "model", 10.0)])
    sim, scaler, cut, diag = build.similar_to_full(ren)
    edges, scaler2 = build.similar_to(ren)                 # the binding interface: (edges, scaler)
    assert edges.equals(sim) and scaler2.equals(scaler)
    assert (scaler["std"].iloc[1:] == 0).all() and scaler["n_ref"].iloc[0] == 3
    assert np.isfinite(sim["d2"]).all() and (sim["dist"] == np.sqrt(sim["d2"])).all()
    ab = sim[(sim["src"] == "a") & (sim["dst"] == "b")].iloc[0]
    assert ab["rank"] == 1 and bool(ab["mutual"]) and ab["spec_version"] == spec.SIMILAR_TO_SPEC_VERSION
    assert len(cut) == 0 and diag["exact_halves"] == 0


# --------------------------------------------------------------------------- tiny build
def test_tiny_counts(tiny_build):
    _, man = tiny_build
    assert man["counts"]["nodes"] == TINY_NODES and man["counts"]["edges"] == TINY_EDGES
    assert (man["counts"]["total_nodes"], man["counts"]["total_edges"]) == (616, 1949)


def test_parquet_files_match_spec_schema_and_order(tiny_build):
    bdir, man = tiny_build
    for _name, rel, schema in build.table_files():
        got = pq.read_schema(bdir / rel)
        assert got.names == schema.names, rel
        assert [str(t) for t in got.types] == [str(t) for t in schema.types], rel
        assert b"pandas" not in (got.metadata or {}), f"{rel} carries pandas metadata"
        assert pq.read_metadata(bdir / rel).num_rows == man["files"][rel]["rows"]
    assert (bdir / "parquet" / "nodes_Renewal.parquet").is_file()
    assert (bdir / "parquet" / "edges_SIMILAR_TO.parquet").is_file()
    assert (bdir / spec.SCALER_FILE).is_file() and (bdir / store.DB_FILE).is_file()
    ren = pd.read_parquet(bdir / "parquet/nodes_Renewal.parquet")
    assert ren["renewal_id"].is_monotonic_increasing and ren["renewal_id"].is_unique
    sim = pd.read_parquet(bdir / "parquet/edges_SIMILAR_TO.parquet")
    assert sim[["src", "rank"]].apply(tuple, axis=1).is_monotonic_increasing


def test_ladybug_counts_equal_parquet(tiny_build):
    bdir, man = tiny_build
    assert man["ladybug"]["counts_equal_parquet"] is True and man["ladybug"]["buffer_pool_mb"] == 256
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        assert store.count_all(conn) == {**TINY_NODES, **TINY_EDGES}
        hero = queries.fetch(conn, "renewal_header", {"renewal_id": "sub_santosh:2026-10-07"})[0]
        assert (hero["route"], hero["plan_tier"], str(hero["as_of"]), hero["city"]) == \
            ("score_today", "pro", "2026-09-30", "Pune")
        with pytest.raises(RuntimeError, match="read-only"):  # graph mutations are refused
            conn.execute("CREATE (:Plan {plan_tier: 'x', price_usd: 1.0, base_allowance_28d: 1})")
    finally:
        conn.close()
        db.close()


def test_manifest_provenance(tiny_build):
    bdir, man = tiny_build
    assert man["business_build_id"] == bdir.name and len(bdir.name) == 12
    assert man["spec"] == {"graph": "renewal-graph/v1", "similar_to": "similar_to/renewal-v1",
                           "contract": "renewal-graph/v1"}
    assert set(man["inputs"]["sha256"]) == set(spec.BRONZE_FILES)
    assert set(man["code_sha256"]) == set(mf.CONTENT_CODE) and "missing" not in man["code_sha256"].values()
    assert set(man["versions"]) == {"ladybug", "pyarrow", "pandas", "numpy", "python"}
    assert man["versions"]["ladybug"] == "0.21.2" and man["platform"] == mf.platform_tag()
    assert (man["seed"], man["n_users"]) == (42, 120) and man["seed_n_status"] in ("declared", "verified")
    assert "commit" in man and "dirty" in man
    assert man["data_end"] == "2026-09-30" and man["synthetic"] is True
    assert set(man["exports"]["sha256"]) == set(spec.EXPORT_FILES)
    assert man["builder"]["max_rss_bytes"] > 0 and man["builder"]["rss_soft_limit_mib"] == 512
    assert man["ladybug"]["max_rss_bytes"] > 0 and man["params"]["k"] == 10 and man["params"]["quant"] == 10**9
    assert man["similar_to"]["exact_halves"] == 0
    assert spec.latest_link("tiny", bdir.parents[2]).resolve() == bdir.resolve()


def test_rebuild_same_id_and_byte_identical_parquet(tiny_build, tmp_path):
    bdir, man = tiny_build
    # same root: the builder finds the existing build, rebuilds, compares bytes and keeps it
    again, man2 = build.build_profile("tiny", graph_root=bdir.parents[2], log=lambda *_: None)
    assert again == bdir and man2["business_build_id"] == man["business_build_id"]
    # a fresh root: independent build, same id, same sha256 for every Parquet file
    other, man3 = build.build_profile("tiny", graph_root=tmp_path / "other_root", log=lambda *_: None)
    assert other != bdir and man3["business_build_id"] == man["business_build_id"]
    assert {k: v["sha256"] for k, v in man3["files"].items()} == {k: v["sha256"] for k, v in man["files"].items()}
    for rel in man["files"]:
        assert (other / rel).read_bytes() == (bdir / rel).read_bytes(), rel


def test_build_id_changes_when_code_or_inputs_change(tmp_path):
    fixture = REPO / spec.TINY_FIXTURE
    base = mf.build_identity(fixture)
    assert base == mf.build_identity(fixture)  # stable
    # code: a one-byte change in any content-shaping file re-keys the build
    fake = tmp_path / "repo"
    for rel in mf.CONTENT_CODE:
        (fake / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / rel, fake / rel)
    assert mf.build_identity(fixture, repo=fake)["business_build_id"] == base["business_build_id"]
    for rel in mf.CONTENT_CODE:
        original = (fake / rel).read_bytes()
        (fake / rel).write_bytes(original + b"\n# changed\n")
        assert mf.build_identity(fixture, repo=fake)["business_build_id"] != base["business_build_id"], rel
        (fake / rel).write_bytes(original)
    # inputs: a changed bronze file re-keys the build
    sample = tmp_path / "sample"
    shutil.copytree(fixture, sample)
    assert mf.build_identity(sample)["business_build_id"] == base["business_build_id"]
    with open(sample / "support_tickets.csv", "a") as f:
        f.write("t-extra,sub_00001,2026-07-01\n")
    assert mf.build_identity(sample)["business_build_id"] != base["business_build_id"]


def test_missing_bronze_names_the_commands(tmp_path):
    with pytest.raises(build.GraphBuildError) as e:
        build.build_profile("default", graph_root=tmp_path / "root", sample_dir=tmp_path / "empty")
    assert "make churn-gold-local" in str(e.value) and "make graph-sample PROFILE=s42" in str(e.value)
    with pytest.raises(ValueError):
        spec.check_profile("../evil")


def test_reserved_names_are_not_profiles(tmp_path):
    """A profile named like a build-tree entry could write through $GRAPH_ROOT/current."""
    root = tmp_path / "g"
    for name in ("current", "logs", "latest", "builds", "sample", "export"):
        assert not spec.is_profile(name)
        with pytest.raises(ValueError, match="reserved"):
            spec.check_profile(name)
        with pytest.raises(ValueError, match="reserved"):
            spec.profile_dir(name, root)
        with pytest.raises(ValueError, match="reserved"):
            build.build_profile(name, graph_root=root)
    assert spec.is_profile("default") and spec.is_profile("tiny") and spec.is_profile("s42")
    p = run_script("build_graph_local.py", "sample", "--check", "--profile", "current", "--seed", "42",
                   "--n-users", "100", "--graph-root", str(root))
    assert p.returncode == 2 and "reserved" in p.stderr and "must be integers" not in p.stderr
    p = run_script("build_graph_local.py", "build", "--profile", "logs", "--graph-root", str(root))
    assert p.returncode == 1 and "reserved" in p.stderr
    p = run_script("check_graph_contract.py", "--profile", "current", "--graph-root", str(root))
    assert p.returncode == 1 and "reserved" in p.stderr and "Traceback" not in p.stderr
    assert not root.exists()                                # nothing was created for any of them


def test_profile_names_must_say_where_the_seed_comes_from(tmp_path):
    """Only default, tiny, inject and s<digits> are profiles: a name like 'staging' gets an error
    about seed derivation, never make's leftover 'taging is not an integer'."""
    root = tmp_path / "g"
    for name in ("default", "tiny", "inject", "s42", "s7", "s0"):
        assert spec.is_profile(name) and spec.check_profile(name) == name
    for name in ("staging", "s42x", "sx", "s", "eval", "tiny2", "s-1"):
        assert not spec.is_profile(name)
        with pytest.raises(ValueError, match="no seed can be derived") as e:
            spec.check_profile(name)
        for kind in ("s<digits>", "tiny", "inject", "default"):
            assert kind in str(e.value)
        with pytest.raises(ValueError, match="no seed can be derived"):
            build.build_profile(name, graph_root=root)
    for args in (("sample", "--profile", "staging", "--seed", "taging", "--n-users", "8000"),   # what make derived
                 ("sample", "--check", "--profile", "staging", "--seed", "", "--n-users", "")):
        p = run_script("build_graph_local.py", *args, "--graph-root", str(root))
        assert p.returncode == 2 and "no seed can be derived" in p.stderr and "s<digits>" in p.stderr
        assert "must be integers" not in p.stderr and "Traceback" not in p.stderr
    p = run_script("build_graph_local.py", "build", "--profile", "staging", "--graph-root", str(root))
    assert p.returncode == 1 and "Graph build FAILED" in p.stderr and "no seed can be derived" in p.stderr
    p = run_script("check_graph_contract.py", "--profile", "staging", "--graph-root", str(root))
    assert p.returncode == 1 and "no seed can be derived" in p.stderr and "Traceback" not in p.stderr
    assert mf.declared_seed("inject")[:2] == (42, 120) and mf.declared_seed("s7")[0] == 7
    assert not root.exists()


def test_graph_sample_requests_are_checked_explicitly(tmp_path):
    """tiny / inject are bound to the committed fixture (42 / 120): a different SEED or N_USERS is
    an explicit error, not silently ignored; s<digits> takes its seed from the name."""
    root = tmp_path / "g"
    assert build.sample_params("tiny") == build.sample_params("tiny", 42, 120) == (42, 120)
    assert build.sample_params("inject", None, 120) == (42, 120)
    assert build.sample_params("s7") == (7, 8000) and build.sample_params("s7", 7, 50) == (7, 50)

    def sample(*args):
        return run_script("build_graph_local.py", "sample", "--check", *args, "--graph-root", str(root))

    p = sample("--profile", "tiny", "--seed", "7")
    assert p.returncode == 2 and "SEED=7 cannot apply" in p.stderr and "EXPORTS ONLY" in p.stderr
    assert "committed tiny fixture (seed 42, N_USERS 120)" in p.stderr and "PROFILE=s<seed>" in p.stderr
    p = sample("--profile", "tiny", "--n-users", "8000")
    assert p.returncode == 2 and "N_USERS=8000 cannot apply" in p.stderr
    p = sample("--profile", "inject", "--seed", "42", "--n-users", "500")
    assert p.returncode == 2 and "N_USERS=500 cannot apply" in p.stderr and "poisoned user_name" in p.stderr
    p = sample("--profile", "s7", "--seed", "x")
    assert p.returncode == 2 and "SEED and N_USERS must be integers (got SEED='x'" in p.stderr
    p = sample("--profile", "s7", "--n-users", "0")
    assert p.returncode == 2 and "N_USERS must be a positive integer" in p.stderr
    for args, want in ((("--profile", "tiny"), "seed 42, N_USERS 120"), (("--profile", "tiny", "--seed", "42"), "120"),
                       (("--profile", "s7", "--seed", "", "--n-users", ""), "seed 7, N_USERS 8000"),
                       (("--profile", "inject"), "seed 42, N_USERS 120")):
        p = sample(*args)
        assert p.returncode == 0 and want in p.stdout and "nothing written" in p.stdout, p.stderr
    assert not root.exists()
    with pytest.raises(ValueError, match="PROFILE=default is refused"):
        build.prepare_sample("default", graph_root=root)
    with pytest.raises(ValueError, match="cannot apply"):
        build.prepare_sample("tiny", 7, graph_root=root)
    # a missing tiny bronze is not something graph-sample can fix: the message says so
    with pytest.raises(build.GraphBuildError, match="Restore the fixture from git") as e:
        build.check_bronze(tmp_path / "nowhere", "tiny")
    assert "exports only" in str(e.value) and "never regenerates bronze" in str(e.value)
    with pytest.raises(build.GraphBuildError, match="make graph-sample PROFILE=inject"):
        build.check_bronze(tmp_path / "nowhere", "inject")


def test_bronze_the_gold_twin_cannot_read_is_a_build_error(tmp_path):
    sample = tmp_path / "sample"
    shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
    lines = (sample / "support_tickets.csv").read_text().splitlines()
    (sample / "support_tickets.csv").write_text("\n".join(lines[1:]) + "\n")    # header row lost
    with pytest.raises(build.GraphBuildError, match="no header row or lacks a column"):
        build.build_profile("tiny", sample, tmp_path / "export", tmp_path / "root", log=lambda *_: None)
    assert not list((tmp_path / "root").rglob(".tmp-*"))    # the temp build dir is cleaned up


def test_build_tables_edge_cases_are_explicit():
    """No declared incident is a valid graph; an event time of day is refused (v1 stores dates)."""
    mod = build.load_gold_twin(REPO / spec.TINY_FIXTURE)
    silver, gold, today = build.run_gold(mod)
    quiet = {**silver, "incidents": silver["incidents"].iloc[:0]}
    t = build.build_tables(quiet, mod.gold(quiet, today), today)     # consts default to the gold twin's
    assert len(t["Incident"]) == 0 and len(t["EXPOSED_TO"]) == 0 and len(t["Renewal"]) == 121
    assert list(t["EXPOSED_TO"].columns) == ["src", "dst", "event_date"]
    assert int(t["Renewal"]["incident_exposed_28d"].sum()) == 0
    build.to_arrow(t["EXPOSED_TO"], spec.EDGE_SCHEMA["EXPOSED_TO"].schema)  # still writable
    assert build.build_tables(silver, gold, today)["EXPOSED_TO"].equals(
        build.build_tables(silver, gold, today, build.gold_constants(mod))["EXPOSED_TO"])

    for table, col in (("overage_charges", "charged_at"), ("overage_settings", "changed_at")):
        timed = silver[table].copy()
        timed[col] = timed[col] + pd.Timedelta(hours=10)
        with pytest.raises(build.GraphBuildError, match="carries a time of day"):
            build.build_tables({**silver, table: timed}, gold, today)


DATE_SOURCES = [(t, c, w) for t, c, w in build.DATE_GRAINED_SOURCES]


@pytest.mark.parametrize("table, col, what", DATE_SOURCES, ids=[w for _, _, w in DATE_SOURCES])
def test_a_time_of_day_in_any_date_grained_source_fails_the_build(table, col, what):
    """Every event source the graph reads is a DATE in renewal-graph/v1: one timed row is a build
    error that names the column, in build_tables and before gold() runs, never a silent truncation."""
    mod = build.load_gold_twin(REPO / spec.TINY_FIXTURE)
    silver = mod.silver()
    build.check_date_grain(silver)                           # the fixture is clean
    timed = silver[table].copy()
    assert len(timed), table
    timed[col] = timed[col].astype("datetime64[ns]")
    timed.loc[timed.index[0], col] = timed[col].iloc[0] + pd.Timedelta(hours=10, minutes=30)
    bad = {**silver, table: timed}
    with pytest.raises(build.GraphBuildError) as e:
        build.check_date_grain(bad)
    msg = str(e.value)
    assert msg.startswith(f"{what} carries a time of day (1 of {len(timed):,} rows, e.g. ") and "10:30:00" in msg
    assert "truncated silently" in msg and "to_date(hit_at)" in msg
    today = silver["snapshots"]["snapshot_date"].max()
    with pytest.raises(build.GraphBuildError, match=f"{what} carries a time of day"):
        build.build_tables(bad, mod.gold(silver, today), today, build.gold_constants(mod))
    mod.silver = lambda: bad                                 # the build path: refused before gold() runs
    mod.gold = lambda *_: pytest.fail("gold() must not run on a timed source")
    with pytest.raises(build.GraphBuildError, match=f"{what} carries a time of day"):
        build.run_gold(mod)


def test_date_guard_covers_every_event_source_and_leaves_hit_at_alone(tmp_path):
    covered = {what for _, _, what in build.DATE_GRAINED_SOURCES}
    assert {"subscription_events.event_date", "invoices.invoice_date", "daily_usage.activity_date",
            "overage_settings.changed_at", "overage_charges.charged_at", "support_tickets.created_date"} <= covered
    assert not any("hit_at" in w or "hit_date" in w for w in covered)
    mod = build.load_gold_twin(REPO / spec.TINY_FIXTURE)
    silver, gold, today = build.run_gold(mod)
    # limit_events.hit_at legitimately carries a time; the graph stores the gold rule's to_date(hit_at)
    assert (silver["limits"]["hit_at"] != silver["limits"]["hit_at"].dt.normalize()).any()
    t = build.build_tables(silver, gold, today, build.gold_constants(mod))
    assert (t["LimitHit"]["event_date"] == t["LimitHit"]["event_date"].dt.normalize()).all()
    assert sorted(t["LimitHit"]["event_date"]) == sorted(silver["limits"]["hit_at"].dt.normalize())
    # null dates are not times of day; the writer has the same guard as a backstop for any DATE column
    build.check_date_grain({"tickets": pd.DataFrame({"created_date": pd.to_datetime(["2026-07-01", None])})})
    df = t["HIT_LIMIT"].copy()
    df["event_date"] = df["event_date"] + pd.Timedelta(hours=1)
    with pytest.raises(build.GraphBuildError, match="event_date carries a time of day but is stored as a DATE"):
        build.write_parquet(df, tmp_path / "x.parquet", spec.EDGE_SCHEMA["HIT_LIMIT"].schema)
    # end to end: a timed bronze column fails the CLI build with the column named, and leaves no temp dir
    sample = tmp_path / "sample"
    shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
    lines = (sample / "invoices.csv").read_text().splitlines()
    i = lines[0].split(",").index("invoice_date")

    def build_with(timed_rows):
        rows = [line.split(",") for line in lines[1:]]
        for row in rows[:timed_rows]:
            row[i] = row[i] + " 08:15:00"
        (sample / "invoices.csv").write_text("\n".join([lines[0], *(",".join(r) for r in rows)]) + "\n")
        return run_script("build_graph_local.py", "build", "--profile", "tiny", "--sample-dir", str(sample),
                          "--graph-root", str(tmp_path / "root"))

    p = build_with(len(lines))                               # every row: the bronze pre-pass refuses the column
    assert p.returncode == 1 and (f"Graph build FAILED: invoices.invoice_date is not a plain YYYY-MM-DD date column: "
                                  f"all {len(lines) - 1:,} rows carry a time of day") in p.stderr
    assert "08:15:00' in invoices.csv line 2" in p.stderr and "Traceback" not in p.stderr
    p = build_with(1)                                        # one row: pandas itself refuses the mixed column
    assert p.returncode == 1 and "Traceback" not in p.stderr and "You might want to try" not in p.stderr
    assert (f"Graph build FAILED: invoices.invoice_date mixes formats: 1 of {len(lines) - 1:,} rows are not a plain "
            f"YYYY-MM-DD date, e.g. ") in p.stderr
    assert "08:15:00' in invoices.csv line 2 (some rows carry a time of day)" in p.stderr
    assert not list((tmp_path / "root").rglob(".tmp-*")) and not list((tmp_path / "root").rglob("manifest.json"))


@pytest.mark.parametrize("table, col, what", DATE_SOURCES, ids=[w for _, _, w in DATE_SOURCES])
def test_a_time_of_day_in_some_rows_names_the_file_column_and_line(table, col, what, tmp_path):
    """The realistic case: only some rows carry a time. pandas then refuses the column inside the
    user's silver(); the build error still names the file, the column, the line and an example."""
    sample = tmp_path / "sample"
    shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
    assert build.not_plain_dates(sample) == []               # the fixture is clean
    path = sample / f"{what.split('.')[0]}.csv"
    rows = list(csv.reader(io.StringIO(path.read_text())))
    i = rows[0].index(col)
    k = next(n for n in range(max(1, len(rows) // 2), len(rows)) if rows[n][i])     # a row in the middle
    rows[k][i] += "T10:30:00" if len(what) % 2 else " 10:30:00"
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    path.write_text(buf.getvalue())
    assert build.not_plain_dates(sample) == [{"what": what, "file": path.name, "column": col, "rows": len(rows) - 1,
                                              "bad": 1, "row": k + 1, "example": rows[k][i]}]
    with pytest.raises(build.GraphBuildError) as e:
        build.build_profile("tiny", sample, tmp_path / "export", tmp_path / "root", log=lambda *_: None)
    msg = str(e.value)
    if len(rows) > 2:                                        # the bronze pre-pass names the column, line, example
        assert msg.startswith(f"{what} mixes formats: 1 of {len(rows) - 1:,} rows are not a plain YYYY-MM-DD date, "
                              f"e.g. {rows[k][i]!r} in {path.name} line {k + 1} (some rows carry a time of day). "), msg
        assert "You might want to try" not in msg and "to_date(hit_at)" in msg
    else:                                                    # a one-row column: all of its rows carry the time
        assert msg.startswith(f"{what} is not a plain YYYY-MM-DD date column: all 1 rows carry a time of day, "
                              f"e.g. {rows[k][i]!r} in {path.name} line {k + 1}"), msg
    assert not list((tmp_path / "root").rglob(".tmp-*")) and not list((tmp_path / "root").rglob("manifest.json"))


NOT_A_DATE = {   # verifier round 2: UTC offsets and 'Z' (even at midnight), another format; then impossible days
    "offset": (lambda v: v + "T00:00:00+05:30", "a UTC offset"), "zulu": (lambda v: v + "T00:00:00Z", "a UTC offset"),
    "offset_with_time": (lambda v: v + " 10:30:00-08:00", "a UTC offset"),
    "us_format": (lambda v: f"{v[5:7]}/{v[8:10]}/{v[:4]}", "another date format"),
    "impossible_day": (lambda v: v[:5] + "02-30", "an impossible calendar date"),
    "impossible_month": (lambda v: v[:5] + "13" + v[7:], "an impossible calendar date")}


def _rewrite_column(sample, what, change, which):
    """Apply ``change`` to every non-empty value of the bronze column ``what`` (or to the middle one)."""
    path = sample / f"{what.split('.')[0]}.csv"
    rows = list(csv.reader(io.StringIO(path.read_text())))
    i = rows[0].index(what.split(".", 1)[1])
    idx = [n for n in range(1, len(rows)) if rows[n][i]]
    picked = idx if which == "all" else [idx[len(idx) // 2]]
    for n in picked:
        rows[n][i] = change(rows[n][i])
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    path.write_text(buf.getvalue())
    return path, rows, i, picked, len(rows) - 1


@pytest.mark.parametrize("shape", sorted(NOT_A_DATE))
@pytest.mark.parametrize("table, col, what", DATE_SOURCES, ids=[w for _, _, w in DATE_SOURCES])
def test_a_utc_offset_or_another_format_is_refused_by_name(table, col, what, shape, tmp_path):
    """A tz-aware value (an offset or 'Z', even at midnight: which day would it be?), another date
    format or a day that does not exist (2026-02-30, month 13) in any date-grained column is refused
    before pandas or gold() read it, with the file, the column, the line and an example: never a
    traceback (verifier round 2 saw TypeError and pandas' 'Tz-aware datetime' there), never pandas'
    own 'day is out of range' text, never a conversion."""
    change, carries = NOT_A_DATE[shape]
    for which in ("all", "one"):
        sample = tmp_path / which / "sample"
        shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
        path, rows, i, picked, total = _rewrite_column(sample, what, change, which)
        with pytest.raises(build.GraphBuildError) as e:
            build.build_profile("tiny", sample, tmp_path / "export", tmp_path / which / "root", log=lambda *_: None)
        msg = str(e.value)
        where = f"e.g. {rows[picked[0]][i]!r} in {path.name} line {picked[0] + 1}"
        if len(picked) == total:
            assert msg.startswith(f"{what} is not a plain YYYY-MM-DD date column: all {total:,} rows carry {carries}, "
                                  f"{where}"), msg
        else:                                                # some rows (one, or all that are not empty)
            assert msg.startswith(f"{what} mixes formats: {len(picked):,} of {total:,} rows are not a plain YYYY-MM-DD "
                                  f"date, {where} (some rows carry {carries})"), msg
        assert "to_date(hit_at)" in msg and not (tmp_path / which / "root").exists()     # refused before the lock


NOT_A_TIMESTAMP = {   # limit_events.hit_at: a naive YYYY-MM-DD[ HH:MM[:SS]] on a real day and clock
    "zulu": (lambda v: v.replace(" ", "T") + "Z", "a UTC offset"),
    "offset": (lambda v: v + "+05:30", "a UTC offset"),
    "impossible_day": (lambda v: "2026-02-30" + v[10:], "an impossible calendar date"),
    "impossible_clock": (lambda v: v[:11] + "25:10:00", "an impossible time of day"),
    "us_format": (lambda v: f"{v[5:7]}/{v[8:10]}/{v[:4]}{v[10:]}", "another date format")}


@pytest.mark.parametrize("shape", sorted(NOT_A_TIMESTAMP))
def test_hit_at_is_a_naive_timestamp_on_a_real_day(shape, tmp_path):
    """limit_events.hit_at is the one timestamp: the bronze pre-pass refuses an offset or 'Z' (a
    tz-aware column once ended in a TypeError traceback), an impossible day or clock and another
    format by name, in every row or in one, before pandas or gold() read it."""
    what = "limit_events.hit_at"
    assert (("limits", "hit_at", what),) == build.TIMESTAMP_SOURCES
    change, carries = NOT_A_TIMESTAMP[shape]
    for which in ("all", "one"):
        sample = tmp_path / which / "sample"
        shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
        assert build.not_plain_dates(sample) == [] == build.not_plain_dates(sample, allow_midnight=True)
        path, rows, i, picked, total = _rewrite_column(sample, what, change, which)
        with pytest.raises(build.GraphBuildError) as e:
            build.build_profile("tiny", sample, tmp_path / "export", tmp_path / which / "root", log=lambda *_: None)
        msg = str(e.value)
        where = f"e.g. {rows[picked[0]][i]!r} in {path.name} line {picked[0] + 1}"
        if which == "all":
            assert msg.startswith(f"{what} is not a naive YYYY-MM-DD[ HH:MM:SS] timestamp column: all {total:,} rows "
                                  f"carry {carries}, {where}"), msg
        else:
            assert msg.startswith(f"{what} mixes formats: 1 of {total:,} rows are not a naive YYYY-MM-DD[ HH:MM:SS] "
                                  f"timestamp, {where} (some rows carry {carries})"), msg
        assert "to_date(hit_at)" in msg and not (tmp_path / which / "root").exists()


@pytest.mark.slow
def test_bronze_dates_the_verifier_probed_are_refused_by_name_through_the_cli(tmp_path):
    """End to end (verifier round 3's date probe): a 'Z' on every hit_at, an impossible day in one
    ticket and in one snapshot fail `build_graph_local.py build` with exit 1 and a message naming
    the file, the column and the line: no traceback and no pandas hint."""
    cases = {"hit_at_zulu_all": ("limit_events.hit_at", NOT_A_TIMESTAMP["zulu"][0], "all", "a UTC offset"),
             "invalid_calendar_one": ("support_tickets.created_date", lambda v: "2026-02-30", "one",
                                      "an impossible calendar date"),
             "snapshot_invalid_one": ("subscription_snapshots.snapshot_date", lambda v: "2026-09-31", "one",
                                      "an impossible calendar date")}
    for name, (what, change, which, carries) in cases.items():
        sample = tmp_path / name / "sample"
        shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
        path, rows, i, picked, _total = _rewrite_column(sample, what, change, which)
        p = run_script("build_graph_local.py", "build", "--profile", "tiny", "--sample-dir", str(sample),
                       "--graph-root", str(tmp_path / name / "root"))
        err = p.stdout + p.stderr
        assert p.returncode == 1 and "Traceback" not in err and "You might want to try" not in err, (name, err[-600:])
        assert f"Graph build FAILED: {what} " in err, (name, err[-600:])
        assert f"in {path.name} line {picked[0] + 1}" in err, (name, err[-600:])
        assert f"carry {carries}" in err, (name, err[-600:])


def test_a_naive_midnight_is_the_same_date_and_builds_the_same_tables(tiny_build, tmp_path):
    """'2026-07-01 00:00:00' / '2026-07-01T00:00:00' (no offset) is the same date: every date-grained
    column written that way builds tables byte-identical to the clean fixture's."""
    _, man = tiny_build
    want = {k: v["sha256"] for k, v in man["files"].items()}
    for n, (_, _, what) in enumerate(build.DATE_GRAINED_SOURCES):
        sample = tmp_path / f"s{n}"
        shutil.copytree(REPO / spec.TINY_FIXTURE, sample)
        _rewrite_column(sample, what, lambda v, t="T" if n % 2 else " ": f"{v}{t}00:00:00", "all")
        assert build.not_plain_dates(sample, allow_midnight=True) == [] and build.not_plain_dates(sample)
        _, got = build.build_profile("tiny", sample, spec.export_dir("tiny", tiny_build[0].parents[2]),
                                     tmp_path / f"r{n}", log=lambda *_: None)
        assert {k: v["sha256"] for k, v in got["files"].items()} == want, what


def test_mixed_date_message_without_a_culprit_drops_the_pandas_hint():
    """A format problem outside the date-grained columns (e.g. in limit_events.hit_at): the
    message keeps pandas' finding and drops its 'You might want to try' hint."""
    pandas_says = 'unconverted data remains when parsing with format "%Y-%m-%d %H:%M:%S": "Z". You might want to try:'
    msg = build.mixed_dates_message(REPO / spec.TINY_FIXTURE, pandas_says)
    assert msg.startswith("a date column of the bronze CSVs in ") and '"%Y-%m-%d %H:%M:%S": "Z"): some rows' in msg
    assert "You might want to try" not in msg and "to_date(hit_at)" in msg


def test_gold_twin_is_loaded_not_the_audit_csv(tiny_build, tmp_path):
    """Features come from gold() in process: a build without any exports has the same Parquet."""
    _, man = tiny_build
    _, man2 = build.build_profile("tiny", graph_root=tmp_path / "no_exports", log=lambda *_: None)
    assert man2["exports"]["sha256"] == {}
    assert {k: v["sha256"] for k, v in man2["files"].items()} == {k: v["sha256"] for k, v in man["files"].items()}


# --------------------------------------------------------------------------- profiles, lock, promote, gc
def test_profile_paths(tmp_path, monkeypatch):
    monkeypatch.delenv("CHURN_SAMPLE_DIR", raising=False)
    monkeypatch.delenv("CHURN_EXPORT_DIR", raising=False)
    root = tmp_path / "g"
    assert spec.sample_dir("default", root) == REPO / "data/sample/churn"
    assert spec.export_dir("default", root) == REPO / "data/export"
    assert spec.sample_dir("tiny", root) == REPO / "data/sample/churn/fixtures/tiny"
    assert spec.export_dir("tiny", root) == root / "tiny/export"
    assert spec.sample_dir("s7", root) == root / "s7/sample" and spec.export_dir("s7", root) == root / "s7/export"
    assert spec.sample_dir("inject", root) == root / "inject/sample"
    assert spec.export_dir("inject", root) == root / "inject/export"
    assert spec.current_link(root) == root / "current" and spec.lock_path(root) == root / ".lock"
    monkeypatch.setenv("GRAPH_ROOT", str(root))
    assert spec.graph_root() == root and spec.builds_dir("s42") == root / "s42/builds"


def test_sample_refuses_default_and_seed_mismatch(tmp_path):
    root = str(tmp_path / "g")
    p = run_script("build_graph_local.py", "sample", "--check", "--profile", "default", "--seed", "42",
                   "--n-users", "8000", "--graph-root", root)
    assert p.returncode == 2 and "PROFILE=default is refused" in p.stderr
    p = run_script("build_graph_local.py", "sample", "--profile", "default", "--graph-root", root)
    assert p.returncode == 2 and "PROFILE=default is refused" in p.stderr       # without --check too
    p = run_script("build_graph_local.py", "sample", "--check", "--profile", "s7", "--seed", "42",
                   "--n-users", "100", "--graph-root", root)
    assert p.returncode == 2 and "seed 7" in p.stderr
    assert not (REPO / "data/graph/s7").exists() and not (tmp_path / "g").exists()


def test_build_lock_is_exclusive(tmp_path):
    root = tmp_path / "g"
    with store.BuildLock(root, timeout=5):
        assert spec.lock_path(root).read_text().strip() == str(os.getpid())
        with pytest.raises(TimeoutError), store.BuildLock(root, timeout=0.3, poll=0.05):
            pass
        # another process cannot take it either
        code = ("import sys; sys.path.insert(0, sys.argv[1]); from lakehouse_graph import store\n"
                "try:\n    store.BuildLock(sys.argv[2], timeout=0.3, poll=0.05).__enter__(); print('acquired')\n"
                "except TimeoutError:\n    print('blocked')\n")
        out = subprocess.run([sys.executable, "-c", code, str(REPO / "src"), str(root)], capture_output=True,
                             text=True, check=False)
        assert out.stdout.strip() == "blocked", out.stderr
    with store.BuildLock(root, timeout=1):  # released
        pass


def _fake_build(root, profile, name, built_at):
    d = spec.builds_dir(profile, root) / name
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps({"business_build_id": name, "built_at": built_at, "files": {}}))
    return d


def test_gc_keeps_last_three_and_never_a_held_build(tmp_path):
    root = tmp_path / "g"
    b = [_fake_build(root, "s42", f"b{i}", f"2026-09-30T10:0{i}:00Z") for i in range(1, 7)]  # b1 oldest
    store.write_pidfile(b[0])                               # b1: held by this (live) process
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    store.write_pidfile(b[1], pid=dead.pid)                 # b2: stale pidfile (process is gone)
    store.update_link(spec.latest_link("s42", root), b[2])  # b3: <profile>/latest points at it
    stale_tmp = spec.builds_dir("s42", root) / f".tmp-abc-{dead.pid}"
    stale_tmp.mkdir()
    assert store.live_pids(b[0]) == [os.getpid()] and store.live_pids(b[1]) == []
    rep = store.gc(keep=3, root=root)["s42"]                # every profile under the root
    assert rep["kept"] == ["b6", "b5", "b4"] and rep["removed"] == ["b2"]
    assert {h["build"] for h in rep["held"]} == {"b1", "b3"}
    assert b[0].is_dir() and b[2].is_dir() and not b[1].exists() and not stale_tmp.exists()
    store.remove_pidfile(b[0])
    store.update_link(spec.latest_link("s42", root), b[5])
    assert sorted(store.gc("s42", keep=3, root=root)["s42"]["removed"]) == ["b1", "b3"]   # gc(profile, keep=3)
    assert sorted(p.name for p in spec.builds_dir("s42", root).iterdir()) == ["b4", "b5", "b6"]
    with pytest.raises(ValueError, match="reserved"):
        store.gc("current", root=root)
    # $GRAPH_ROOT/current (a symlink into a build) is never mistaken for a profile
    store.update_link(spec.current_link(root), b[5])
    assert list(store.gc(root=root)) == ["s42"]


def test_update_link_is_a_relative_symlink_swap(tmp_path):
    root = tmp_path / "g"
    a, b = _fake_build(root, "default", "aaa", "1"), _fake_build(root, "default", "bbb", "2")
    link = spec.current_link(root)
    store.update_link(link, a)
    assert link.is_symlink() and os.readlink(link) == "default/builds/aaa" and link.resolve() == a.resolve()
    store.update_link(link, b)                              # replace in place (os.replace)
    assert link.resolve() == b.resolve() and not list(root.glob(".current.tmp-*"))
    (root / "plain").mkdir()
    with pytest.raises(RuntimeError):
        store.update_link(root / "plain", a)                # never replaces a real directory


@pytest.mark.slow
def test_promote_needs_a_strict_contract_pass_on_default(tiny_build, tmp_path):
    root = tmp_path / "g"
    fixture = REPO / spec.TINY_FIXTURE
    make_exports(fixture, root / "exports")
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)
    with pytest.raises(RuntimeError, match="only the default profile can be promoted"):
        store.promote("tiny", root=root)
    bdir, _ = build.build_profile("default", graph_root=root, sample_dir=fixture, export_dir=root / "exports",
                                  log=lambda *_: None)
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)                            # built but not checked
    p = run_script("check_graph_contract.py", "--build", str(bdir))
    assert p.returncode == 0, p.stdout + p.stderr
    assert store.read_contract(bdir)["strict"] is False     # a first plain pass is recorded ...
    with pytest.raises(RuntimeError):
        store.promote(root=root)                            # ... but a non-strict pass is not enough
    p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
    assert p.returncode == 0, p.stdout + p.stderr
    assert store.promote(root=root) == bdir
    assert spec.current_link(root).is_symlink() and spec.current_link(root).resolve() == bdir.resolve()
    p = run_script("build_graph_local.py", "promote", "--graph-root", str(root))
    assert p.returncode == 0 and "os.replace" in p.stdout and "stale" not in p.stdout

    # a later plain check keeps the strict record, so the build stays promotable
    strict_record = store.read_contract(bdir)
    p = run_script("check_graph_contract.py", "--build", str(bdir))
    assert p.returncode == 0 and "keeps the earlier strict pass" in p.stdout
    assert store.read_contract(bdir) == strict_record and store.promote(root=root) == bdir
    assert store.promote("default", bdir, root=root) == bdir          # promote(profile, build_dir)
    assert store.promote("default", bdir.name, root=root) == bdir     # ... or a bare build id
    with pytest.raises(RuntimeError, match="no strict contract pass recorded for build"):
        store.promote("default", "0" * 12, root=root)


def _passing_build(root, name, built_at, sample_dir):
    """A default-profile build directory whose manifest + contract.json say: strict pass."""
    ident = mf.build_identity(sample_dir)
    d = spec.builds_dir("default", root) / name
    d.mkdir(parents=True)
    man = {"business_build_id": name, "built_at": built_at, "files": {}, "exports": {"dir": "x", "sha256": {}},
           "inputs": {"sample_dir": str(sample_dir), "sha256": ident["payload"]["inputs"]}}
    (d / "manifest.json").write_text(json.dumps(man))
    rec = {"status": "pass", "strict": True, "business_build_id": name, "files_sha256": {}, "exports_sha256": {},
           "checked_at": "2026-09-30T12:00:00Z"}
    (d / store.CONTRACT_FILE).write_text(json.dumps(rec))
    return d, ident["business_build_id"]


def test_promote_picks_the_newest_fresh_build_and_rolls_back_only_by_name(tmp_path):
    root = tmp_path / "g"
    fixture = REPO / spec.TINY_FIXTURE
    fresh_id = mf.build_identity(fixture)["business_build_id"]
    old, _ = _passing_build(root, "aaaaaaaaaaaa", "2026-09-30T10:00:00Z", fixture)   # other code: stale now
    new, _ = _passing_build(root, "bbbbbbbbbbbb", "2026-09-30T11:00:00Z", fixture)   # newest, also stale
    assert [d.name for _, d in store.passing_builds("default", root)] == ["aaaaaaaaaaaa", "bbbbbbbbbbbb"]
    assert not mf.is_fresh(mf.read_manifest(new))
    with pytest.raises(RuntimeError, match="is stale"):
        store.promote(root=root)                            # a pass that predates a code change
    good, _ = _passing_build(root, fresh_id, "2026-09-30T09:00:00Z", fixture)        # oldest, but fresh
    assert mf.is_fresh(mf.read_manifest(good))
    assert store.promote(root=root) == good                 # fresh wins over more recently built / checked
    assert store.promote("default", "bbbbbbbbbbbb", root=root) == new               # explicit roll back
    p = run_script("build_graph_local.py", "promote", "--build", "aaaaaaaaaaaa", "--graph-root", str(root))
    assert p.returncode == 0 and "NOTE: this build is stale" in p.stdout
    assert spec.current_link(root).resolve() == old.resolve()
    # the record stops counting when the pinned exports or the Parquet move on
    rec = json.loads((good / store.CONTRACT_FILE).read_text())
    for key in ("exports_sha256", "files_sha256"):
        (good / store.CONTRACT_FILE).write_text(json.dumps({**rec, key: {"x": "1"}}))
        assert good not in [d for _, d in store.passing_builds("default", root)]
    (good / store.CONTRACT_FILE).write_text(json.dumps({**rec, "strict": False}))
    assert not store.is_strict_pass(store.read_contract(good), mf.read_manifest(good))


def _restamp_exports(edir, stamp="2031-01-01 00:00:00"):
    """What running the gold script again does: the same features with a new built_at."""
    audit = edir / "churn_renewals_audit.csv"
    audit.write_text(audit.read_text().replace(pd.read_csv(audit)["built_at"].iloc[0], stamp))


def test_unchanged_build_is_repinned_not_rebuilt(tmp_path):
    """Regenerated exports (new built_at) must not need a forced rebuild: the build is kept and
    its manifest pins (exports, seed / N_USERS status) are refreshed in place."""
    root, fixture = tmp_path / "g", REPO / spec.TINY_FIXTURE
    edir = root / "tiny/export"
    quiet = {"log": lambda *_: None}
    bdir, man = build.build_profile("tiny", graph_root=root, **quiet)
    assert man["exports"]["sha256"] == {} and man["seed_n_status"] == "declared" and "repinned_at" not in man
    make_exports(fixture, edir)
    logs: list[str] = []
    again, man2 = build.build_profile("tiny", graph_root=root, log=logs.append)
    assert again == bdir and man2["built_at"] == man["built_at"] and "repinned_at" in man2
    assert set(man2["exports"]["sha256"]) == set(spec.EXPORT_FILES) and man2 == mf.read_manifest(bdir)
    assert any("unchanged" in m for m in logs)
    assert any("re-pinned in manifest.json: exports sha256" in m for m in logs)
    assert man2["files"] == man["files"] and man2["business_build_id"] == man["business_build_id"]
    _restamp_exports(edir)
    _, man3 = build.build_profile("tiny", graph_root=root, **quiet)
    assert man3["exports"]["sha256"] == mf.export_hashes(edir) != man2["exports"]["sha256"]
    # nothing moved: nothing is rewritten
    before = (bdir / mf.MANIFEST_FILE).read_bytes()
    _, man4 = build.build_profile("tiny", graph_root=root, **quiet)
    assert man4 == man3 and (bdir / mf.MANIFEST_FILE).read_bytes() == before

    # seed / N_USERS: a verification is picked up, and is never downgraded to a declaration
    _, man5 = build.build_profile("tiny", graph_root=root, verify_seed=True, **quiet)
    assert man5["seed_n_status"] == "verified" and "sha256 match" in man5["seed_n_verification"]
    _, man6 = build.build_profile("tiny", graph_root=root, **quiet)
    assert man6["seed_n_status"] == "verified" and man6 == man5
    assert not list(spec.builds_dir("tiny", root).glob(".*"))   # no temp or verification dirs left behind


@pytest.mark.slow
def test_repinned_build_needs_and_passes_a_new_strict_check(tmp_path):
    """The contract side of re-pinning: regenerated exports fail --strict until graph-build re-pins
    them, and a strict pass recorded against the old exports stops counting for promote."""
    root = tmp_path / "g"
    edir = root / "tiny/export"
    make_exports(REPO / spec.TINY_FIXTURE, edir)
    bdir, man = build.build_profile("tiny", graph_root=root, log=lambda *_: None)
    p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr
    passed = store.read_contract(bdir)
    assert store.is_strict_pass(passed, man) and passed["exports_sha256"] == man["exports"]["sha256"]

    _restamp_exports(edir)
    p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
    assert p.returncode == 1 and "make graph-build PROFILE=tiny to re-pin" in p.stderr
    assert store.read_contract(bdir)["status"] == "fail"    # a failure always replaces the record
    _, man2 = build.build_profile("tiny", graph_root=root, log=lambda *_: None)
    assert man2["exports"]["sha256"] == mf.export_hashes(edir) and man2["files"] == man["files"]
    assert not store.is_strict_pass(passed, man2)           # that pass was checked against the old exports
    p = run_script("check_graph_contract.py", "--build", str(bdir), "--strict")
    assert p.returncode == 0, p.stdout[-2000:] + p.stderr
    assert "export sha256 pinned in the manifest" in p.stdout
    assert store.is_strict_pass(store.read_contract(bdir), man2)


def test_a_build_without_its_ladybug_file_is_replaced(tmp_path):
    root = tmp_path / "g"
    bdir, man = build.build_profile("tiny", graph_root=root, log=lambda *_: None)
    (bdir / store.DB_FILE).unlink()
    logs: list[str] = []
    again, man2 = build.build_profile("tiny", graph_root=root, log=logs.append)
    assert again == bdir and (bdir / store.DB_FILE).is_file() and any("is missing" in m for m in logs)
    assert man2["files"] == man["files"] and man2["ladybug"]["counts_equal_parquet"] is True
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        assert store.count_all(conn)["Renewal"] == 121
    finally:
        conn.close()
        db.close()


# --------------------------------------------------------------------------- atomic replace, locked promote / gc
def test_exchange_and_replace_dir(tmp_path, monkeypatch):
    a, b = tmp_path / "a", tmp_path / "b"
    for d, text in ((a, "new"), (b, "old")):
        d.mkdir()
        (d / "f").write_text(text)
    try:
        store.exchange_paths(a, b)                          # one system call: both names exist throughout
    except OSError as e:
        if REQUIRE_EXCHANGE:                                # CI sets it: the fallback must not hide a broken call
            pytest.fail(f"GRAPH_REQUIRE_EXCHANGE=1 but the atomic exchange failed on {sys.platform}: {e}")
        pytest.skip(f"no atomic exchange on this filesystem ({e}); replace_dir falls back to two renames")
    assert ((a / "f").read_text(), (b / "f").read_text()) == ("old", "new")
    with pytest.raises(OSError):
        store.exchange_paths(a, tmp_path / "missing")       # both paths must exist
    (a / "f").write_text("newer")
    assert store.replace_dir(a, b) == "exchange" and (b / "f").read_text() == "newer" and not a.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["b"]
    # the fallback where the filesystem has no exchange call: old -> .trash-*, new -> final
    a.mkdir()
    (a / "f").write_text("newest")

    def no_exchange(*_):
        raise OSError(errno.ENOTSUP, "no exchange here")

    monkeypatch.setattr(store, "exchange_paths", no_exchange)
    assert store.replace_dir(a, b) == "rename" and (b / "f").read_text() == "newest"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["b"]          # no trash, no temp left behind


def test_replace_dir_fallback_puts_the_old_build_back_when_the_second_rename_fails(tmp_path, monkeypatch):
    """old -> .trash, then new -> final fails: the old directory is back before the error is raised."""
    new, final = tmp_path / ".tmp-new", tmp_path / "build"
    for d, text in ((new, "new"), (final, "old")):
        d.mkdir()
        (d / "f").write_text(text)

    def no_exchange(*_):
        raise OSError(errno.ENOTSUP, "no exchange here")

    real, calls = os.replace, []

    def second_rename_fails(src, dst):
        calls.append((os.path.basename(src), os.path.basename(dst)))
        if len(calls) == 2:
            raise OSError(errno.EIO, "disk says no")
        return real(src, dst)

    monkeypatch.setattr(store, "exchange_paths", no_exchange)
    monkeypatch.setattr(store.os, "replace", second_rename_fails)
    with pytest.raises(OSError, match="disk says no"):
        store.replace_dir(new, final)
    monkeypatch.undo()
    trash = f"{store.TRASH_PREFIX}build-{os.getpid()}"
    assert calls == [("build", trash), (".tmp-new", "build"), (trash, "build")]
    assert (final / "f").read_text() == "old" and (new / "f").read_text() == "new"
    assert sorted(p.name for p in tmp_path.iterdir()) == [".tmp-new", "build"]      # no trash left


def test_failed_rename_replace_keeps_the_build_its_contract_and_current(tmp_path, monkeypatch):
    root = tmp_path / "g"
    bdir, man, rec, kw = _promoted_default_build(root)

    def no_exchange(*_):
        raise OSError(errno.ENOTSUP, "no exchange here")

    real = os.replace

    def refuse_the_new_build(src, dst):
        if os.path.basename(src).startswith(".tmp-") and os.fspath(dst) == os.fspath(bdir):
            raise OSError(errno.EIO, "disk says no")
        return real(src, dst)

    monkeypatch.setattr(store, "exchange_paths", no_exchange)
    monkeypatch.setattr(store.os, "replace", refuse_the_new_build)
    with pytest.raises(build.GraphBuildError, match=r"could not swap the new build in \(.*disk says no\); the "
                                                    r"existing build \w+ is unchanged"):
        build.build_profile("default", rebuild=True, log=lambda *_: None, **kw)
    monkeypatch.undo()
    assert store.read_contract(bdir) == rec and mf.read_manifest(bdir) == man       # the old build, untouched
    assert spec.current_link(root).resolve() == bdir.resolve() and (spec.current_link(root) / store.DB_FILE).is_file()
    assert not [p.name for p in spec.builds_dir("default", root).iterdir() if p.name.startswith(".")]
    assert store.promote(root=root) == bdir


def test_a_build_left_in_trash_by_a_crashed_replace_is_restored(tmp_path):
    """A rename-replace that dies between its two renames leaves the only copy of the old build in
    .trash-<id>-<pid>: build, promote and gc put it back instead of treating it as a stale temp dir."""
    root = tmp_path / "g"
    bdir, man, rec, kw = _promoted_default_build(root)
    builds, current = spec.builds_dir("default", root), spec.current_link(root)
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    trash = builds / f"{store.TRASH_PREFIX}{bdir.name}-{dead.pid}"

    def crash():
        os.replace(bdir, trash)                             # what the crash leaves behind
        assert not current.exists() and current.is_symlink()            # current dangles

    crash()
    mine = builds / f"{store.TRASH_PREFIX}ffffffffffff-{os.getpid()}"   # a live writer's trash is not touched
    mine.mkdir()
    assert store.restore_orphans(builds) == [bdir.name] and mine.is_dir() and not trash.exists()
    assert store.read_contract(bdir) == rec and current.resolve() == bdir.resolve()
    mine.rmdir()
    assert store.restore_orphans(builds) == [] and store.restore_orphans(tmp_path / "missing") == []

    crash()                                                 # gc restores before it judges what is old
    rep = store.gc("default", keep=3, root=root)["default"]
    assert (rep["restored"], rep["kept"]) == ([bdir.name], [bdir.name])
    assert (rep["removed"], rep["stale_tmp_removed"]) == ([], [])
    shutil.copytree(bdir, trash)                            # a trash whose build exists is only a leftover
    rep = store.gc("default", keep=3, root=root)["default"]
    assert (rep["restored"], rep["stale_tmp_removed"]) == ([], [trash.name]) and bdir.is_dir()

    crash()                                                 # promote heals it
    assert store.promote(root=root) == bdir and current.resolve() == bdir.resolve()
    crash()                                                 # and so does the next build, which then keeps it
    logs: list[str] = []
    again, man2 = build.build_profile("default", log=logs.append, **kw)
    assert again == bdir and any(f"restored build {bdir.name}" in m for m in logs)
    assert any("unchanged" in m for m in logs)
    assert store.read_contract(bdir) == rec and man2["files"] == man["files"]
    p = run_script("build_graph_local.py", "gc", "--graph-root", str(root))
    assert p.returncode == 0 and "restored" not in p.stdout
    crash()
    p = run_script("build_graph_local.py", "gc", "--graph-root", str(root))
    assert p.returncode == 0 and f"restored after an interrupted replace: {bdir.name}" in p.stdout


def _strict_record(man):
    return {"status": "pass", "strict": True, "business_build_id": man["business_build_id"],
            "files_sha256": {k: v["sha256"] for k, v in man["files"].items()},
            "exports_sha256": man["exports"]["sha256"], "checked_at": "2026-09-30T12:00:00Z"}


def _promoted_default_build(root):
    """A default-profile build of the tiny bronze with a strict pass on record, promoted to current."""
    fixture = REPO / spec.TINY_FIXTURE
    kw = {"graph_root": root, "sample_dir": fixture, "export_dir": root / "exports"}
    bdir, man = build.build_profile("default", log=lambda *_: None, **kw)
    rec = _strict_record(man)
    mf.write_json_atomic(bdir / store.CONTRACT_FILE, rec)
    assert store.promote(root=root) == bdir
    return bdir, man, rec, kw


def _fake_lineage(bdir, fresh: bool) -> None:
    """lineage/ + lineage.lbdb whose manifest is fresh for the running lineage code, or stale."""
    from lakehouse_graph.lineage import build as lineage_build

    (bdir / "lineage").mkdir()
    code = lineage_build.code_hashes() if fresh else {"src/lakehouse_graph/lineage/extract.py": "0" * 64}
    mf.write_json_atomic(bdir / "lineage" / "manifest.json", {"inputs_sha256": {}, "code_sha256": code})
    (bdir / "lineage.lbdb").write_text("lineage database")


def _fake_cohorts(bdir, text: str = "another phase's artefact", stale: str | None = None) -> dict:
    """cohorts.parquet and the manifest record that describes it (what build._cohorts_still_valid
    checks: the file's sha256, the cohorts code, the NetworkX version, an integer seed). ``stale``:
    'code' / 'library' / 'file' makes the record describe other code, another NetworkX or other bytes."""
    (bdir / "cohorts.parquet").write_text(text)
    rec = {"rows": 121, "sha256": mf.sha256_file(bdir / "cohorts.parquet"), "library": "networkx",
           "library_version": importlib.metadata.version("networkx"), "seed": 42,
           "code_sha256": mf.sha256_file(REPO / build.COHORTS_CODE)}
    changed = {"code": ("code_sha256", "0" * 64), "library": ("library_version", "0.0"), "file": ("sha256", "1" * 64)}
    if stale:
        rec[changed[stale][0]] = changed[stale][1]
    return rec


def test_rebuild_swaps_the_build_atomically_and_keeps_a_valid_contract(tmp_path):
    """Replacing a build never leaves `current` dangling and never drops a strict contract.json
    that still describes byte-identical Parquet."""
    root = tmp_path / "g"
    bdir, man, rec, kw = _promoted_default_build(root)
    (bdir / "old-build-marker").write_text("x")            # only the replaced directory has this
    cohorts = _fake_cohorts(bdir)
    _fake_lineage(bdir, fresh=True)
    mf.write_manifest(bdir, {**man, "lineage": {"summary": "of the lineage build"}, "cohorts": cohorts})
    current = spec.current_link(root)
    stop, dangling, polls = threading.Event(), [], [0]

    def watch():                                            # what a serving process would see
        while not stop.is_set():
            polls[0] += 1
            if not (current / mf.MANIFEST_FILE).is_file() or not (current / store.DB_FILE).is_file():
                dangling.append(polls[0])

    t = threading.Thread(target=watch)
    t.start()
    logs: list[str] = []
    try:
        again, man2 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    finally:
        stop.set()
        t.join()
    line = next(m for m in logs if "replaced the existing build" in m)
    assert again == bdir and not (bdir / "old-build-marker").exists()   # really a new directory
    assert man2["files"] == man["files"] and man2["business_build_id"] == man["business_build_id"]
    assert store.read_contract(bdir) == rec and store.is_strict_pass(store.read_contract(bdir), man2)
    assert "Parquet byte-identical, kept contract.json, lineage/, lineage.lbdb, cohorts.parquet" in line
    assert "not carried over: old-build-marker" in line     # nothing is lost silently
    # the other phases' artefacts describe the same content: kept, with their manifest summary
    assert (bdir / "cohorts.parquet").read_text() == "another phase's artefact"
    assert (bdir / "lineage" / "manifest.json").is_file() and (bdir / "lineage.lbdb").read_text() == "lineage database"
    assert man2["lineage"] == {"summary": "of the lineage build"} == mf.read_manifest(bdir)["lineage"]
    assert man2["cohorts"] == cohorts == mf.read_manifest(bdir)["cohorts"]
    assert current.resolve() == bdir.resolve() and spec.latest_link("default", root).resolve() == bdir.resolve()
    assert store.promote(root=root) == bdir                 # still promotable without a new check
    assert not [p.name for p in spec.builds_dir("default", root).iterdir() if p.name.startswith(".")]
    assert polls[0] > 0
    assert "(exchange)" in line or not REQUIRE_EXCHANGE, line    # CI: the swap really is the one-call exchange
    if "(exchange)" in line:                                # one rename call: never a missing build
        assert not dangling, f"current dangled in {len(dangling)} of {polls[0]} polls"
    db, conn = store.open_readonly(current / store.DB_FILE)
    try:
        assert store.count_all(conn)["Renewal"] == 121
    finally:
        conn.close()
        db.close()


def test_rebuild_fallback_and_changed_content(tmp_path, monkeypatch):
    root = tmp_path / "g"
    bdir, man, rec, kw = _promoted_default_build(root)

    def no_exchange(*_):
        raise OSError(errno.ENOTSUP, "no exchange here")

    monkeypatch.setattr(store, "exchange_paths", no_exchange)
    logs: list[str] = []
    _, man2 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    assert any("replaced the existing build" in m and "(rename)" in m for m in logs)
    assert store.read_contract(bdir) == rec and spec.current_link(root).resolve() == bdir.resolve()
    assert not [p.name for p in spec.builds_dir("default", root).iterdir() if p.name.startswith(".")]
    monkeypatch.undo()

    # the old build's Parquet is not what the rebuild produces: neither its contract record nor the
    # other phases' artefacts (made from that content) are carried over, and the log names them
    old = mf.read_manifest(bdir)
    first = next(iter(old["files"]))
    old["files"][first]["sha256"] = "0" * 64
    mf.write_manifest(bdir, {**old, "lineage": {"summary": "of other content"}})
    (bdir / "lineage").mkdir()
    (bdir / "cohorts.parquet").write_text("of other content")
    with pytest.raises(build.GraphBuildError, match="determinism violation"):
        build.build_profile("default", log=lambda *_: None, **kw)
    assert store.read_contract(bdir) == rec                 # a refused build changes nothing
    logs.clear()
    _, man3 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    assert man3["files"] == man["files"] and store.read_contract(bdir) is None and "lineage" not in man3
    assert not (bdir / "lineage").exists() and not (bdir / "cohorts.parquet").exists()
    line = next(m for m in logs if "replaced the existing build" in m)
    assert "no contract record carried over (run make graph-check); not carried over: cohorts.parquet, lineage" in line
    assert spec.current_link(root).resolve() == bdir.resolve()         # the link itself stays valid
    with pytest.raises(RuntimeError, match="no strict contract pass"):
        store.promote(root=root)

    # a build a live server holds is never replaced
    store.write_pidfile(bdir)
    with pytest.raises(build.GraphBuildError, match="held by live pid"):
        build.build_profile("default", rebuild=True, log=lambda *_: None, **kw)
    store.remove_pidfile(bdir)
    assert (bdir / mf.MANIFEST_FILE).is_file()
    assert not [p.name for p in spec.builds_dir("default", root).iterdir() if p.name.startswith(".")]


def test_carry_over_is_a_registry_that_checks_each_entry(tmp_path, monkeypatch):
    """Verifier round 2 (CARRY_OVER): a byte-identical replacement hands over what the registry
    names; a later phase registers its own artefact (and a validity check), a stale lineage build
    or cohorts table is dropped with the reason, and the log says what was kept and what was not.
    The built-ins come first; phases that register at import time append after them."""
    assert list(build.CARRY_OVER)[:3] == ["contract", "lineage", "cohorts"]
    assert build.CARRY_OVER["lineage"] == build.CarryOver(("lineage", "lineage.lbdb"), "lineage",
                                                          build._lineage_still_valid)
    assert build.CARRY_OVER["cohorts"] == build.CarryOver(("cohorts.parquet",), "cohorts", build._cohorts_still_valid)
    assert {k: build.PHASE_ARTEFACTS[k] for k in ("lineage", "cohorts")} == \
        {"lineage": ("lineage", "lineage.lbdb"), "cohorts": ("cohorts.parquet",)}
    for bad in ((), ("",), ("parquet",), (store.DB_FILE,), (mf.MANIFEST_FILE,), ("a/b",), (".hidden",),
                (spec.SCALER_FILE,)):
        with pytest.raises(ValueError, match="plain names in the build directory"):
            build.register_carry_over("x", bad)
    root = tmp_path / "g"
    bdir, man, rec, kw = _promoted_default_build(root)
    monkeypatch.setattr(build, "CARRY_OVER", dict(build.CARRY_OVER))    # a scratch registry for this test
    build.register_carry_over("viz", ("viz",), manifest_key="viz")      # what a later phase does at import
    build.register_carry_over("badge", ("badge.json",), still_valid=lambda old: "made by another badge version")
    assert dict(build.PHASE_ARTEFACTS)["viz"] == ("viz",)               # the old name is a live view
    (bdir / "viz").mkdir()
    (bdir / "viz" / "sub_santosh.html").write_text("<html></html>")
    (bdir / "badge.json").write_text("{}")
    cohorts = _fake_cohorts(bdir, "cohorts")
    _fake_lineage(bdir, fresh=False)                                    # its code changed since: stale
    mf.write_manifest(bdir, {**man, "viz": {"views": 1}, "cohorts": cohorts, "lineage": {"id": "old"}})
    logs: list[str] = []
    again, man2 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    line = next(m for m in logs if "replaced the existing build" in m)
    assert "Parquet byte-identical, kept contract.json, cohorts.parquet, viz/" in line, line     # registry order
    assert ("not carried over: badge.json, lineage, lineage.lbdb (lineage: stale: a file it was extracted from, or "
            "the lineage code, changed since it was built; badge: made by another badge version; rebuild them with "
            "their own targets)") in line, line
    assert (again / "viz" / "sub_santosh.html").is_file() and (again / "cohorts.parquet").read_text() == "cohorts"
    assert not any((again / name).exists() for name in ("lineage", "lineage.lbdb", "badge.json"))
    assert (man2["viz"], man2["cohorts"]) == ({"views": 1}, cohorts) and "lineage" not in man2
    assert mf.read_manifest(again) == man2 and store.read_contract(again) == rec
    # a fresh lineage build is carried with its manifest key
    _fake_lineage(again, fresh=True)
    mf.write_manifest(again, {**man2, "lineage": {"id": "fresh"}})
    logs.clear()
    _, man3 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    line = next(m for m in logs if "replaced the existing build" in m)
    assert "kept contract.json, lineage/, lineage.lbdb, cohorts.parquet, viz/" in line, line
    assert man3["lineage"] == {"id": "fresh"} and (again / "lineage.lbdb").read_text() == "lineage database"
    # a cohorts table whose record no longer holds (other cohorts code, another NetworkX, other bytes)
    # is dropped with the reason, and so is its manifest key
    why = {"code": f"cohorts: {build.COHORTS_CODE} changed since it was built",
           "library": "cohorts: built with networkx 0.0, networkx "
                      f"{importlib.metadata.version('networkx')} is installed",
           "file": "cohorts: cohorts.parquet differs from its record"}
    for stale, reason in why.items():
        mf.write_manifest(again, {**mf.read_manifest(again), "cohorts": _fake_cohorts(again, "cohorts", stale)})
        logs.clear()
        _, man4 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
        line = next(m for m in logs if "replaced the existing build" in m)
        assert f"not carried over: cohorts.parquet ({reason}; rebuild them with their own targets)" in line, line
        assert "cohorts" not in man4 and not (again / "cohorts.parquet").exists()
    mf.write_manifest(again, {**mf.read_manifest(again), "cohorts": "not a record"})   # nor without a record
    (again / "cohorts.parquet").write_text("cohorts")
    assert build._cohorts_still_valid(again) == "manifest.json has no cohorts record describing it"


def test_carry_over_api_and_the_pre_registry_signature(tmp_path):
    """carry_over() returns (kept, dropped, keys, reasons); _carry_over() keeps the three-value
    signature the Iceberg builder (lakehouse_graph.iceberg_source) unpacks."""
    files = {"parquet/nodes_Plan.parquet": {"sha256": "a" * 64}}
    for n in (1, 2):
        old, new = tmp_path / f"old{n}", tmp_path / f"new{n}"
        new.mkdir(parents=True)
        old.mkdir()
        mf.write_manifest(new, {"files": files})             # the builder writes it before carrying over
        cohorts = _fake_cohorts(old, "c")
        mf.write_manifest(old, {"files": files, "cohorts": cohorts})
        (old / store.CONTRACT_FILE).write_text("{}")
        _fake_lineage(old, fresh=False)
        (old / "stray").write_text("x")
        got = build.carry_over(old, new, files) if n == 1 else build._carry_over(old, new, files)
        if n == 1:
            assert isinstance(got, build.CarriedOver) and got.kept == [store.CONTRACT_FILE, "cohorts.parquet"]
            assert got.dropped == ["lineage", "lineage.lbdb", "stray"] and got.keys == {"cohorts": cohorts}
            assert got.reasons == ["lineage: stale: a file it was extracted from, or the lineage code, changed since "
                                   "it was built"]
        else:
            kept, dropped, keys = got
            assert (kept, dropped, keys) == ([store.CONTRACT_FILE, "cohorts.parquet"],
                                             ["lineage", "lineage.lbdb", "stray"], {"cohorts": cohorts})
        assert sorted(p.name for p in new.iterdir()) == ["cohorts.parquet", store.CONTRACT_FILE, mf.MANIFEST_FILE]
    # other Parquet bytes: nothing is carried, everything the old build held beyond the contract is named
    old, new = tmp_path / "old1", tmp_path / "new3"
    new.mkdir()
    mf.write_manifest(new, {"files": {}})
    got = build.carry_over(old, new, {"parquet/nodes_Plan.parquet": {"sha256": "b" * 64}})
    assert got.kept == [] and got.reasons == [] and "cohorts.parquet" in got.dropped
    assert [p.name for p in new.iterdir()] == [mf.MANIFEST_FILE]


@pytest.mark.slow
def test_a_rebuild_keeps_a_fresh_lineage_build_and_its_contract_verdict(tmp_path):
    """End to end with the real lineage builder: build the lineage tables into a promoted build,
    --rebuild the business graph (byte-identical Parquet): the lineage tables and their manifest
    key are carried over, and the strict lineage contract says exactly what it said before (same
    status, errors, warnings, ids and file hashes; nothing stale, nothing unrecorded). Whether it
    passes depends on the repo's pipelines, which the lineage track owns, not on the rebuild."""
    root = tmp_path / "g"
    bdir, _man, rec, kw = _promoted_default_build(root)
    p = run_script("build_lineage_local.py", "--graph-profile", "default", "--graph-root", str(root))
    assert p.returncode == 0, p.stdout + p.stderr
    summary = mf.read_manifest(bdir)["lineage"]
    lineage_files = {q.relative_to(bdir): q.read_bytes() for q in sorted((bdir / "lineage").rglob("*.parquet"))}

    def verdict(n):
        out = tmp_path / f"lineage-contract-{n}.json"
        p = run_script("check_lineage_contract.py", "--graph-profile", "default", "--graph-root", str(root), "--strict",
                       "--json", str(out))
        doc = json.loads(out.read_text())
        return p.returncode, {k: doc[k] for k in ("status", "errors", "warnings", "lineage_build_id", "files_sha256",
                                                  "counts")}

    before = verdict(0)
    logs: list[str] = []
    again, man2 = build.build_profile("default", rebuild=True, log=logs.append, **kw)
    line = next(m for m in logs if "replaced the existing build" in m)
    assert again == bdir and "kept contract.json, lineage/, lineage.lbdb" in line and "not carried over" not in line
    assert man2["lineage"] == summary and store.read_contract(bdir) == rec
    assert {q.relative_to(bdir): q.read_bytes() for q in sorted((bdir / "lineage").rglob("*.parquet"))} == lineage_files
    after = verdict(1)
    assert after == before, (before, after)
    said = " ".join(after[1]["errors"] + after[1]["warnings"])
    for integrity in ("stale lineage build", "changed or missing", "records this lineage build"):
        assert integrity not in said, said                  # intact, fresh and recorded in the business manifest
    assert store.promote(root=root) == bdir                 # still the promotable, strict-passing business build


def test_promote_and_gc_share_the_build_lock(tmp_path, monkeypatch):
    """gc cannot delete the build promote has chosen between the selection and the link swap."""
    root = tmp_path / "g"
    fixture = REPO / spec.TINY_FIXTURE
    fresh_id = mf.build_identity(fixture)["business_build_id"]
    good, _ = _passing_build(root, fresh_id, "2026-09-30T09:00:00Z", fixture)         # oldest: gc would remove it
    newer = [_passing_build(root, name, f"2026-09-30T1{i}:00:00Z", fixture)[0]
             for i, name in enumerate(("aaaaaaaaaaaa", "bbbbbbbbbbbb"))]               # newer, but stale
    real_update, chosen, release = store.update_link, threading.Event(), threading.Event()

    def slow_update(link, target):                          # promote has selected; hold it before the swap
        chosen.set()
        assert release.wait(180)
        return real_update(link, target)

    monkeypatch.setattr(store, "update_link", slow_update)
    out: dict = {}
    promoter = threading.Thread(target=lambda: out.update(promoted=store.promote(root=root)))
    collector = threading.Thread(target=lambda: out.update(gc=store.gc("default", keep=1, root=root)))
    promoter.start()
    assert chosen.wait(180)
    # promote holds the lock: nobody else gets it, in this process or another
    with pytest.raises(TimeoutError), store.BuildLock(root, timeout=0.3, poll=0.05):
        pass
    collector.start()
    time.sleep(0.6)                                         # gc is waiting for the lock, not deleting
    assert collector.is_alive() and "gc" not in out and good.is_dir()
    release.set()
    promoter.join(180)
    collector.join(180)
    assert not promoter.is_alive() and not collector.is_alive()
    assert out["promoted"] == good
    rep = out["gc"]["default"]
    assert rep["kept"] == [newer[1].name] and rep["removed"] == [newer[0].name]
    assert rep["held"] == [{"build": good.name, "reason": "current/latest points at it"}]
    assert good.is_dir() and spec.current_link(root).resolve() == good.resolve()
    # the lock is per graph root and is released afterwards
    with store.BuildLock(root, timeout=1):
        pass
    with pytest.raises(TimeoutError, match="holds"), store.BuildLock(root, timeout=5):
        store.promote(root=root, lock_timeout=0.2)          # not re-entrant: a nested promote times out


# --------------------------------------------------------------------------- inject profile
def test_inject_profile_has_exactly_one_poisoned_user_name(tiny_build, inject_build, graph_root):
    tiny_dir, tiny_man = tiny_build
    bdir, man = inject_build
    fixture = REPO / spec.TINY_FIXTURE
    sdir, edir = spec.sample_dir("inject", graph_root), spec.export_dir("inject", graph_root)
    assert man["profile"] == "inject" and mf.resolve_path(man["inputs"]["sample_dir"]) == sdir
    assert (man["seed"], man["n_users"], man["seed_n_status"]) == (42, 120, "verified")
    assert "poisoned" not in man["seed_n_source"] and "replaced by construction (1 row)" in man["seed_n_source"]

    # bronze: nine files are byte-identical copies; the snapshots differ from the fixture in one line
    for name in spec.BRONZE_FILES:
        same = (sdir / name).read_bytes() == (fixture / name).read_bytes()
        assert same == (name != "subscription_snapshots.csv"), name
    ours = (sdir / "subscription_snapshots.csv").read_text().splitlines()
    theirs = (fixture / "subscription_snapshots.csv").read_text().splitlines()
    changed = [(a, b) for a, b in zip(ours, theirs, strict=True) if a != b]
    assert len(changed) == 1 and changed[0][0].startswith(spec.INJECT_SUBSCRIPTION + ",")
    assert changed[0][0] == changed[0][1].replace("Rahul Chen", spec.INJECT_USER_NAME)

    # graph: exactly one Subscription carries the instruction; everything else equals the tiny build
    subs = pd.read_parquet(bdir / "parquet/nodes_Subscription.parquet").set_index("subscription_id")
    tiny_subs = pd.read_parquet(tiny_dir / "parquet/nodes_Subscription.parquet").set_index("subscription_id")
    poisoned = subs.index[subs["user_name"] == spec.INJECT_USER_NAME].tolist()
    assert poisoned == [spec.INJECT_SUBSCRIPTION] and len(subs) == 121
    assert "Ignore previous instructions" in spec.INJECT_USER_NAME and "APPROVED" in spec.INJECT_USER_NAME
    assert int((subs["user_name"] != tiny_subs["user_name"]).sum()) == 1
    assert subs.drop(columns="user_name").equals(tiny_subs.drop(columns="user_name"))
    differing = sorted(rel for rel in man["files"] if man["files"][rel] != tiny_man["files"][rel])
    assert differing == ["parquet/nodes_Subscription.parquet"]
    # the poisoned subscription is the hero's nearest neighbour, so a hero walk reaches it
    top = oracle.top_k(oracle.load_tables(bdir), "sub_santosh:2026-10-07")
    assert top[0]["renewal_id"].split(":")[0] == spec.INJECT_SUBSCRIPTION

    # exports (the user's gold script on the poisoned bronze) agree with the graph
    audit = pd.read_csv(edir / "churn_renewals_audit.csv")
    assert audit.loc[audit["user_name"] == spec.INJECT_USER_NAME, "user_id"].tolist() == [spec.INJECT_SUBSCRIPTION]
    assert oracle.export_crosscheck(oracle.load_tables(bdir), edir)["status"] == "ok"
    meta = mf.read_sample_meta("inject", graph_root)
    assert meta["inject"] == {"subscription_id": spec.INJECT_SUBSCRIPTION, "user_name": spec.INJECT_USER_NAME,
                              "rows_changed": 1, "source": spec.TINY_FIXTURE}

    # idempotent: a second prepare changes nothing, so the build keeps its export pins
    before = mf.export_hashes(edir)
    info = build.prepare_inject_profile(graph_root, log=lambda *_: None)
    assert info["changed"] is False and mf.export_hashes(edir) == before
    again, man2 = build.build_profile("inject", graph_root=graph_root, log=lambda *_: None)
    assert again == bdir and man2 == mf.read_manifest(bdir) and "repinned_at" not in man2

    # the committed fixture is untouched (the session guard asserts the bytes; git agrees)
    assert man["guarded_sha256"] == mf.guarded_hashes()
    if shutil.which("git"):
        p = subprocess.run(["git", "status", "--porcelain", "--", "data/sample"], cwd=REPO, capture_output=True,
                           text=True, check=False)
        if p.returncode == 0:
            assert p.stdout.strip() == "", p.stdout


def test_poison_snapshots_rewrites_only_the_named_rows():
    text = ("subscription_id,snapshot_date,user_name,plan_tier\nsub_1,2026-07-01,Ann Lee,pro\n"
            "sub_2,2026-07-01,\"Lee, Bo\",pro\nsub_1,2026-07-08,Ann Lee,pro\n")
    out, n = build.poison_snapshots(text, "sub_2", "x, \"y\"")
    assert n == 1 and out.splitlines()[2] == 'sub_2,2026-07-01,"x, ""y""",pro'
    assert [a for a, b in zip(out.splitlines(), text.splitlines(), strict=True) if a != b] == [out.splitlines()[2]]
    out, n = build.poison_snapshots(text, "sub_1", spec.INJECT_USER_NAME)
    assert n == 2 and out.count(spec.INJECT_USER_NAME) == 2 and out.endswith("pro\n")
    assert build.poison_snapshots(text, "sub_9", "x") == (text, 0)
    assert build.poison_snapshots(text.replace("\n", "\r\n"), "sub_2", "z")[0].count("\r\n") == 4


@pytest.mark.slow
def test_inject_profile_through_the_cli(tmp_path, user_data_untouched):
    """scripts/build_graph_local.py build --profile inject: prepare + exports + build in one command."""
    root = tmp_path / "g"
    p = run_script("build_graph_local.py", "build", "--profile", "inject", "--graph-root", str(root))
    assert p.returncode == 0, p.stdout + p.stderr
    assert "graph-sample inject" in p.stdout and "(1 row)" in p.stdout and "Graph build OK" in p.stdout
    assert "seed 42 N_USERS 120 (verified)" in p.stdout
    bdir = spec.latest_link("inject", root).resolve()
    subs = pd.read_parquet(bdir / "parquet/nodes_Subscription.parquet")
    assert (subs["user_name"] == spec.INJECT_USER_NAME).sum() == 1
    p = run_script("check_graph_contract.py", "--profile", "inject", "--graph-root", str(root), "--strict")
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr
    assert "the inject profile's one poisoned user_name" in p.stdout and "every golden value matches" in p.stdout
    p = run_script("build_graph_local.py", "build", "--profile", "inject", "--graph-root", str(root))
    assert p.returncode == 0 and "unchanged" in p.stdout and "graph-sample inject" not in p.stdout
    assert "re-pinned" not in p.stdout                      # nothing regenerated, nothing to re-pin
    p = run_script("build_graph_local.py", "promote", "--profile", "inject", "--graph-root", str(root))
    assert p.returncode == 1 and "only the default profile can be promoted" in p.stderr
    assert mf.guarded_hashes() == user_data_untouched


# --------------------------------------------------------------------------- seed 42 (slow)
@pytest.mark.slow
def test_s42_counts_and_resources(s42_build):
    _, man = s42_build
    assert man["counts"]["nodes"] == S42_NODES and man["counts"]["edges"] == S42_EDGES
    assert (man["counts"]["total_nodes"], man["counts"]["total_edges"]) == (40204, 130366)
    assert (man["seed"], man["n_users"]) == (42, 8000)
    d = man["similar_to"]
    assert d["cut_quantised_ties_broken_by_dst"] == 37 and d["exact_halves"] == 0
    assert d["sources_with_cut_candidate"] == 8001
    # reported, soft: the plan measured 376 MiB for the builder and 232 MB for the loader
    assert man["builder"]["max_rss_mib"] > 0 and man["ladybug"]["max_rss_bytes"] > 0


@pytest.mark.slow
def test_s42_rebuild_is_byte_identical(s42_build, tmp_path):
    bdir, man = s42_build
    _, man2 = build.build_profile("s42", graph_root=tmp_path / "again", sample_dir=spec.sample_dir(
        "s42", bdir.parents[2]), export_dir=spec.export_dir("s42", bdir.parents[2]), log=lambda *_: None)
    assert man2["business_build_id"] == man["business_build_id"]
    assert {k: v["sha256"] for k, v in man2["files"].items()} == {k: v["sha256"] for k, v in man["files"].items()}


@pytest.mark.slow
def test_s42_verify_seed(s42_build, tmp_path):
    bdir, _ = s42_build
    sdir = spec.sample_dir("s42", bdir.parents[2])
    assert mf.verify_seed(sdir, 42, 8000, tmp_path) is True
    assert mf.verify_seed(sdir, 7, 8000, tmp_path) is False
    assert oracle.hero_renewal(oracle.load_tables(bdir)) == "sub_santosh:2026-10-07"
