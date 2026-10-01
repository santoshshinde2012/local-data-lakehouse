"""The physically pruned, label-free evidence graph (lakehouse_graph.pruned, scripts/build_evidence_graph.py).

Tiny: built once (test_tools_support.evidence_tiny) and checked from both sides, the record the builder wrote and an
independent read of the database. Seed 42 (slow): the PLAN numbers (27,238 nodes; 115,999 edges with SIMILAR_TO
kept, 35,989 without; 495 flagged FIRST_RENEWAL_AFTER) and 6-feature parity 0.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys

import pyarrow.parquet as pq
import pytest
from conftest import REPO
from test_tools_support import evidence_tiny

from lakehouse_graph import build, oracle, pruned, store


@pytest.fixture(scope="module")
def ev(graph_root, tiny_build):
    return evidence_tiny(str(graph_root))


@pytest.fixture(scope="module")
def rec(ev):
    return pruned.read_record(ev[1])


def test_the_tiny_evidence_graph_is_cut_flagged_and_label_free(ev, rec):
    # PLAN 6.6 tiny: 616 nodes / 1,949 edges, 235 post-as_of event edges of which 5 are the declared exception
    assert rec["full_graph"] == {"total_nodes": 616, "total_edges": 1949}
    removed = rec["removed"]
    assert removed["removed_edges"] == {"HIT_LIMIT": 47, "OPENED": 4, "BILLED": 161, "EXPOSED_TO": 18,
                                        "SIMILAR_TO": 1182}
    assert sum(v for k, v in removed["removed_edges"].items() if k != "SIMILAR_TO") + 5 == 235
    assert removed["removed_nodes"] == {"LimitHit": 47, "Ticket": 4, "BillingEvent": 161}
    assert rec["counts"]["total_nodes"] == 404 and rec["counts"]["total_edges"] == 537
    assert removed["declared_exception_edges"] == 5 and removed["first_renewal_after_edges"] == 38
    cy, pd_ = rec["checks"]["cypher"], rec["checks"]["pandas"]
    assert not any(cy["post_as_of_subscription_event_edges"].values())
    assert not any(pd_["post_as_of_subscription_event_edges"].values())
    for side in (cy, pd_):
        assert side["pit_parity"] == {mode: dict.fromkeys(pruned.PARITY_FEATURES, 0)
                                      for mode in ("naive", "pit_window")}
    assert cy["first_renewal_after_post_as_of"] == cy["first_renewal_after_flagged"] == 5
    assert cy["billed_event_types"] == {"cancel_scheduled": 4}         # tiny: 4 cancel_flow renewals
    assert cy["label_named"] == [] and cy["similar_to_present"] is False
    assert rec["similar_to_dropped"]["destinations_route"] == ["model"]
    assert rec["similar_to_dropped"]["lapse_share_without_incoming_edge"] > 3 * rec["similar_to_dropped"][
        "lapse_share_all"]
    assert pruned.check_record(rec, 5) == []


def test_no_label_property_exists_anywhere(ev):
    """Read back independently: every Parquet schema and every engine table carries no label or identity column."""
    banned = set(pruned.LABEL_PROPERTIES) | set(pruned.IDENTITY_PROPERTIES) | {"outcome_evidence"}
    for f in sorted((ev[1] / pruned.EVIDENCE_DIR).glob("*.parquet")):
        names = set(pq.read_schema(f).names)
        assert not names & banned, (f.name, names & banned)
        assert not any(w in n.lower() for n in names for w in pruned.LABEL_WORDS), f.name
    assert not (ev[1] / pruned.EVIDENCE_DIR / "edges_SIMILAR_TO.parquet").exists()
    db, conn = pruned.open_db(ev[1] / pruned.EVIDENCE_DB)
    try:
        tables = [r[1] for r in store.rows(conn.execute("CALL show_tables() RETURN *"))]
        assert "SIMILAR_TO" not in tables and len(tables) == 20
        for t in tables:
            cols = {r[1] for r in store.rows(conn.execute(f"CALL table_info('{t}') RETURN *"))}
            assert not cols & banned, (t, cols & banned)
        fra = store.rows(conn.execute("MATCH (r:Renewal)-[f:FIRST_RENEWAL_AFTER]->() WHERE f.event_date > r.as_of "
                                      "RETURN count(*), sum(CASE WHEN f.declared_exception THEN 1 ELSE 0 END)"))
        assert fra == [[5, 5]]
    finally:
        conn.close()
        db.close()


def test_six_feature_parity_without_any_as_of_filter(ev):
    """Recomputed here from the evidence Parquet with the naive (no upper bound) windows: equal to gold."""
    t = pruned._read_tables(ev[1])
    full = oracle.load_tables(ev[1])
    gold = full["Renewal"].set_index("renewal_id")
    naive = pruned.pit_values(t, upper_bound=False)
    for f in pruned.PARITY_FEATURES:
        assert (abs(naive[f] - gold.loc[naive.index, f]) <= 1e-9).all(), f
    # and the full graph's naive traversal is wrong, which is what the pruning removes (PLAN: tiny 16 / 12 / 4)
    p = oracle.pit_parity(full)["naive_mismatches"]
    assert (p["limit_hits_14d"], p["incident_exposed_28d"], p["support_tickets_90d"]) == (16, 12, 4)


def test_rebuild_keeps_an_unchanged_evidence_graph_and_refuses_a_changed_build(ev, rec, tmp_path):
    p = subprocess.run([sys.executable, str(REPO / "scripts/build_evidence_graph.py"), "--build", str(ev[1])],
                       capture_output=True, text=True, timeout=300, check=False, cwd=REPO)
    assert p.returncode == 0 and "kept the unchanged" in p.stdout, p.stdout + p.stderr
    again = pruned.read_record(ev[1])
    assert again["db"] == rec["db"] and again["parquet"] == rec["parquet"]
    assert pruned.verify_record(ev[1], again) is None
    man = json.loads((ev[1] / "manifest.json").read_text())
    assert man["evidence"]["db_sha256"] == rec["db"]["sha256"] and man["evidence"]["label_free"] is True
    # a build whose Parquet no longer matches its manifest is refused, nothing written
    bad = tmp_path / "root" / "tiny" / "builds" / ev[1].name
    shutil.copytree(ev[1], bad)
    with open(bad / "parquet" / "nodes_Renewal.parquet", "ab") as f:
        f.write(b"\0")
    with pytest.raises(pruned.EvidenceError, match="differ from the sha256"):
        pruned.build_evidence(bad, log=lambda *_: None)


def test_byte_identical_rebuilds_carry_the_evidence_graph_over(ev, rec, tmp_path):
    """build.CARRY_OVER["evidence"] (registered by lakehouse_graph.pruned): a --rebuild with byte-identical Parquet
    keeps evidence/, evidence.lbdb, evidence.json and manifest.json["evidence"]; a stale record is dropped."""
    assert build.CARRY_OVER["evidence"].entries == (pruned.EVIDENCE_DIR, pruned.EVIDENCE_DB, pruned.EVIDENCE_META)
    assert build.CARRY_OVER["evidence"].manifest_key == "evidence"
    root = tmp_path / "root"
    shutil.copytree(ev[0] / "tiny" / "builds", root / "tiny" / "builds", symlinks=True)
    shutil.copytree(ev[0] / "tiny" / "export", root / "tiny" / "export")
    bdir, man = build.build_profile("tiny", graph_root=root, rebuild=True, log=lambda *_: None)
    assert bdir.name == ev[1].name
    assert (bdir / pruned.EVIDENCE_DB).is_file() and pruned.verify_record(bdir, pruned.read_record(bdir)) is None
    assert man.get("evidence", {}).get("db_sha256") == rec["db"]["sha256"]
    assert pruned.still_valid(bdir) is None
    with open(bdir / pruned.EVIDENCE_DB, "ab") as f:                  # tamper: the next rebuild drops it
        f.write(b"\0")
    assert "differs from the sha256" in pruned.still_valid(bdir)
    logs: list[str] = []
    bdir, man = build.build_profile("tiny", graph_root=root, rebuild=True, log=logs.append)
    assert not (bdir / pruned.EVIDENCE_DB).exists() and "evidence" not in man
    assert any("evidence" in line and "not carried over" in line for line in logs), logs


@pytest.mark.slow
def test_the_s42_evidence_graph_matches_the_plan(s42_build, tmp_path):
    root = tmp_path / "root"
    dest = root / "s42" / "builds" / s42_build[0].name
    shutil.copytree(s42_build[0], dest, ignore=shutil.ignore_patterns(".pids", "evidence*"))
    rec = pruned.build_evidence(dest, log=lambda *_: None)
    assert rec["full_graph"] == {"total_nodes": 40204, "total_edges": 130366}
    assert rec["counts"]["total_nodes"] == 27238 and rec["counts"]["total_edges"] == 35989
    assert rec["with_similar_to_for_comparison"] == {"total_nodes": 27238, "total_edges": 115999}   # PLAN Later A
    assert rec["removed"]["removed_edges"] == {"HIT_LIMIT": 3000, "OPENED": 114, "BILLED": 9852, "EXPOSED_TO": 1401,
                                               "SIMILAR_TO": 80010}
    assert sum(v for k, v in rec["removed"]["removed_edges"].items() if k != "SIMILAR_TO") == 14367
    assert rec["removed"]["declared_exception_edges"] == 495
    cy = rec["checks"]["cypher"]
    assert cy["first_renewal_after_flagged"] == 495 and cy["billed_event_types"] == {"cancel_scheduled": 287}
    for side in (cy, rec["checks"]["pandas"]):
        assert all(v == 0 for mode in side["pit_parity"].values() for v in mode.values())
        assert not any(side["post_as_of_subscription_event_edges"].values())
    assert cy["label_named"] == [] and not cy["similar_to_present"]
