"""Feature cohorts (cohorts/renewal-v1): NetworkX Louvain + Leiden over SIMILAR_TO, outside the contract.

  build_cohorts(build_dir) -> cohorts.parquet + manifest.json["cohorts"]
  cohort_summary(ctx, cohort_id=None, renewal_id=None, algorithm=None) -> (data, caveats)
  cohort_list(ctx, algorithm="leiden") -> (data, caveats)

The build-backed tests use a private tiny build (its own GRAPH_ROOT) because they write into the
build directory; the seed-42 numbers are compared with the plan in a slow test that only reads.
"""
from __future__ import annotations

import inspect
import json
import os
import re
import shutil
import subprocess
import sys

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from conftest import REPO, make_exports

from lakehouse_graph import build, cohorts, spec, store
from lakehouse_graph import manifest as mf


def _quiet(*_args) -> None:
    return None


def run(script: str, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """A repo script with this interpreter, bounded (a hung child must not hang the suite)."""
    return subprocess.run([sys.executable, str(REPO / "scripts" / script), *args], capture_output=True, text=True,
                          cwd=REPO, check=False, timeout=300, env=dict(os.environ, **(env or {})))


# --------------------------------------------------------------------------- pure functions
def test_wilson_matches_the_plan_golden_and_stays_in_bounds():
    assert cohorts.wilson(2, 10) == [0.0567, 0.5098]            # PLAN: Santosh's neighbours [0.057, 0.510]
    assert [round(x, 3) for x in cohorts.wilson(29, 72)] == [0.297, 0.518]   # PLAN: 29/72 [29.7, 51.8]
    assert cohorts.wilson(0, 0) is None
    for n in (1, 5, 37, 1000):
        for k in (0, 1, n // 2, n):
            lo, hi = cohorts.wilson(k, n)
            assert 0.0 <= lo <= k / n <= hi <= 1.0


def _frames(rows: list[tuple], edges: list[tuple]) -> tuple[pd.DataFrame, pd.DataFrame]:
    ren = pd.DataFrame(rows, columns=["renewal_id", "plan_tier", "route"])
    sim = pd.DataFrame(edges, columns=["src", "dst", "rank", "dist"])
    return ren, sim


def _toy() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Two tight triangles of reference renewals joined by one long edge, plus a dunning source."""
    rows = [(f"sub_{i:02d}:2026-08-01", "pro", "model") for i in range(6)] + [("sub_99:2026-08-01", "pro", "dunning")]
    e = []
    for group in ((0, 1, 2), (3, 4, 5)):
        for a in group:
            for rank, b in enumerate([x for x in group if x != a], start=1):
                e.append((f"sub_{a:02d}:2026-08-01", f"sub_{b:02d}:2026-08-01", rank, 0.1))
    e.append(("sub_02:2026-08-01", "sub_03:2026-08-01", 3, 9.0))
    e.append(("sub_99:2026-08-01", "sub_04:2026-08-01", 1, 0.2))
    e.append(("sub_99:2026-08-01", "sub_00:2026-08-01", 2, 0.3))
    return _frames(rows, e)


def test_reference_graph_is_the_undirected_weighted_union_among_reference_renewals():
    ren, sim = _toy()
    g, ids = cohorts.reference_graph(ren, sim)
    assert ids == sorted(f"sub_{i:02d}:2026-08-01" for i in range(6))          # the dunning source is not a node
    assert list(g.nodes) == list(range(6)) and g.number_of_edges() == 7      # 6 in-triangle + 1 bridge (union)
    assert g[0][1]["weight"] == pytest.approx(1 / 1.1) and g[2][3]["weight"] == pytest.approx(1 / 10)


def test_assign_labels_by_size_and_gives_non_reference_renewals_their_nearest_cohort():
    ren, sim = _toy()
    table, stats = cohorts.assign(ren, sim)
    t = table.set_index("renewal_id")
    for algo in cohorts.ALGORITHMS:
        assert stats["algorithms"][algo]["communities"] == 2 and stats["algorithms"][algo]["plan_purity"] == 1.0
        # equal sizes: the cohort holding the smallest renewal_id comes first
        assert t.at["sub_00:2026-08-01", algo] == f"{algo}-01" and t.at["sub_05:2026-08-01", algo] == f"{algo}-02"
        assert t.at["sub_99:2026-08-01", algo] == t.at["sub_04:2026-08-01", algo]   # rank-1 neighbour's cohort
    dun = t.loc["sub_99:2026-08-01"]
    assert (dun["is_reference"], dun["assigned_via"], dun["via_renewal_id"]) == (False, "nearest_reference",
                                                                                "sub_04:2026-08-01")
    assert stats["assigned"] == {"community": 6, "nearest_reference": 1}
    shuffled, _ = cohorts.assign(ren.sample(frac=1, random_state=3), sim.sample(frac=1, random_state=4))
    assert shuffled.equals(table)                                            # input row order does not matter


def test_no_outcome_column_is_stored():
    names = set(cohorts.SCHEMA.names)
    assert names == {"renewal_id", "plan_tier", "is_reference", "leiden", "louvain", "assigned_via",
                     "via_renewal_id", "spec_version"}
    assert not names & {"churned", "outcome", "route", "outcome_observed_on", "city", "user_name"}


def test_interfaces():
    def params(fn):
        return [(p.name, p.default) for p in inspect.signature(fn).parameters.values()]

    empty = inspect.Parameter.empty
    assert params(cohorts.cohort_summary) == [("ctx", empty), ("cohort_id", None), ("renewal_id", None),
                                              ("algorithm", None)]
    assert params(cohorts.cohort_list) == [("ctx", empty), ("algorithm", "leiden")]
    assert set(cohorts.TOOLS) == {"cohort_summary", "cohort_list"}
    assert all(name.startswith("cohort_") for name in cohorts.TOOLS)
    entry = build.CARRY_OVER["cohorts"]                                     # the carry-over registry
    assert entry.entries == (cohorts.COHORTS_FILE,) and entry.manifest_key == cohorts.MANIFEST_KEY == "cohorts"
    assert build.PHASE_ARTEFACTS["cohorts"] == (cohorts.COHORTS_FILE,)      # its manifest-key view


def test_suppress_cells_hides_small_cells_and_their_complement():
    s = cohorts.suppress_cells
    assert s({"pro": 30, "pro_plus": 12}, True) == {"pro": 30, "pro_plus": 12}
    # one small cell + a published total: the smallest published cell goes too (else total - rest = it)
    assert s({"pro": 30, "pro_plus": 12, "ultra": 3}, True) == {"pro": 30, "pro_plus": None, "ultra": None}
    assert s({"pro": 30, "ultra": 3}, True) == {"pro": None, "ultra": None}
    assert s({"pro": 30, "ultra": 3}, False) == {"pro": 30, "ultra": None}   # no total to subtract from
    assert s({"pro": 30, "pro_plus": 2, "ultra": 3}, True) == {"pro": 30, "pro_plus": None, "ultra": None}
    assert s({"pro": 4}, False) == {"pro": None} and s({}, True) == {}
    # hidden cells that hold fewer than MIN_CELL together are a small cell of their own (total - shown = 3)
    assert s({"pro": 30, "pro_plus": 12, "ultra": 2, "x": 1}, True) == {"pro": 30, "pro_plus": None, "ultra": None,
                                                                        "x": None}
    assert s({"pro": 30, "pro_plus": 2, "ultra": 1}, True) == dict.fromkeys(("pro", "pro_plus", "ultra"))
    assert s({"pro": 30, "pro_plus": 0, "ultra": 0}, True) == {"pro": 30, "pro_plus": None, "ultra": None}  # 0: none


def _cells(cells: list[tuple[str, str, int, int]]) -> pd.DataFrame:
    """A cohorts table from (cohort_id, plan_tier, reference rows, non-reference rows)."""
    rows = []
    for cid, plan, n_ref, n_other in cells:
        rows += [{"leiden": cid, "plan_tier": plan, "is_reference": True}] * n_ref
        rows += [{"leiden": cid, "plan_tier": plan, "is_reference": False}] * n_other
    return pd.DataFrame(rows, columns=["leiden", "plan_tier", "is_reference"])


def test_withheld_cohorts_pairs_a_lone_small_cohort_with_the_smallest_of_its_plan():
    """Population templates publish each plan's model n and lapses: one withheld cohort in a plan
    would come back as plan total minus the published ones, so the plan's smallest published
    cohort is withheld with it (and that may cascade into another plan)."""
    w = cohorts.withheld_cohorts
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro_plus", 20, 0), ("leiden-03", "pro", 12, 0),
                ("leiden-04", "pro", 9, 0), ("leiden-05", "pro", 3, 0)])
    assert w(t, "leiden") == {"leiden-04", "leiden-05"}
    assert cohorts.published_sizes(t, "leiden") == [30, 20, 12, None, None]   # nulls last: no position leak
    # two small cohorts of one plan already cover each other; pro_plus has none
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro_plus", 20, 0), ("leiden-03", "pro", 4, 0),
                ("leiden-04", "pro", 3, 0)])
    assert w(t, "leiden") == {"leiden-03", "leiden-04"}
    # a plan whose only cohort is small: nothing to pair it with (it is the plan's own population cell)
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro", 8, 0), ("leiden-03", "ultra", 4, 0)])
    assert w(t, "leiden") == {"leiden-03"}
    # sizes count reference rows only
    assert w(_cells([("leiden-01", "pro", 30, 9), ("leiden-02", "pro", 5, 40)]), "leiden") == frozenset()
    assert w(_cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro", 2, 40)]), "leiden") == {"leiden-01",
                                                                                                "leiden-02"}
    # a cohort spanning two plans carries the rule across: pro_plus pairs its small leiden-04 with
    # leiden-03 (9, the smallest there), but in pro_plus they hold 3 + 1 renewals together (a small cell
    # by subtraction), so leiden-05 goes too; that leaves pro with one withheld cohort, so leiden-02 goes too
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro", 20, 0), ("leiden-03", "pro", 8, 0),
                ("leiden-03", "pro_plus", 1, 0), ("leiden-05", "pro_plus", 10, 0), ("leiden-04", "pro_plus", 3, 0)])
    assert w(t, "leiden") == {"leiden-02", "leiden-03", "leiden-04", "leiden-05"}
    assert w(_cells([]), "leiden") == frozenset() and cohorts.published_sizes(_cells([]), "leiden") == []
    # two small cohorts holding fewer than MIN_CELL together: plan total - published = 3 would print a small
    # cell (and 2 + 1 = 3 pins both, since labels rank them), so the plan's smallest published cohort goes too
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro", 20, 0), ("leiden-03", "pro", 2, 0),
                ("leiden-04", "pro", 1, 0)])
    assert w(t, "leiden") == {"leiden-02", "leiden-03", "leiden-04"}
    t = _cells([("leiden-01", "pro", 30, 0), ("leiden-02", "pro", 20, 0), ("leiden-03", "pro", 4, 0),
                ("leiden-04", "pro", 1, 0)])
    assert w(t, "leiden") == {"leiden-03", "leiden-04"}                 # 5 together: a published-size cell


def test_assigned_cells_pair_within_each_plan_as_well_as_the_build(monkeypatch):
    """Verifier p3-hardening round 1 (major): metric_route_counts(plan_tier=...) prints each plan's non-model
    route counts exactly enough (score_today / pending are public), so a plan's non-reference renewals minus
    its published assigned cells give its hidden cells back together. The old rule paired cells against the
    build's total only: here (the s42 MIN_CELL 9 case in miniature) it hid pro's 40 beside pro_plus's 8, so
    pro_plus printed 78 and its total 86 gave the 8 back. Now each plan is a group of its own as well."""
    monkeypatch.setattr(cohorts, "MIN_CELL", 9)
    t = _cells([("leiden-01", "pro", 30, 40), ("leiden-02", "pro_plus", 20, 78), ("leiden-03", "pro_plus", 12, 8)])
    old = cohorts.suppress_cells({"leiden-01": 40, "leiden-02": 78, "leiden-03": 8}, True)
    assert old == {"leiden-01": None, "leiden-02": 78, "leiden-03": None}          # 86 - 78 = 8: the leak
    assert cohorts.assigned_cells(t, "leiden") == {"leiden-01": None, "leiden-02": None, "leiden-03": None}
    # two hidden cells of a plan holding 1 to MIN_CELL - 1 together: the plan's smallest published one goes too
    t = _cells([("leiden-01", "pro", 30, 40), ("leiden-02", "pro", 20, 25), ("leiden-03", "pro", 12, 2),
                ("leiden-04", "pro", 9, 3), ("leiden-05", "ultra", 9, 30)])
    assert cohorts.assigned_cells(t, "leiden") == {"leiden-01": 40, "leiden-02": None, "leiden-03": None,
                                                   "leiden-04": None, "leiden-05": 30}
    # hidden cells holding 0 together name no renewal
    t = _cells([("leiden-01", "pro", 30, 40), ("leiden-02", "pro", 20, 0), ("leiden-03", "pro", 12, 0),
                ("leiden-04", "ultra", 9, 12)])
    assert cohorts.assigned_cells(t, "leiden") == {"leiden-01": 40, "leiden-02": None, "leiden-03": None,
                                                   "leiden-04": 12}
    # a plan whose one cohort is small is its own population cell (metric_route_counts' job), but the build's
    # total still pairs it: the smallest published cell goes, and then pro holds one hidden cell, so all go
    t = _cells([("leiden-01", "pro", 30, 40), ("leiden-02", "pro", 20, 25), ("leiden-03", "ultra", 9, 3)])
    assert cohorts.assigned_cells(t, "leiden") == dict.fromkeys(("leiden-01", "leiden-02", "leiden-03"))
    assert cohorts.assigned_cells(_cells([]), "leiden") == {}


def _metrics_layout(w_flag: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(cohorts table, Renewal rows) of one pro block: A (3 model renewals), W (6), C (10), D (12). With
    ``w_flag`` W's members, and only they, toggled overage off: W is then exactly a metric_lapse_rate cell
    (pro, overage_toggled_off = 1), whose n and lapses metrics prints."""
    rows, ren = [], []
    for cid, size in (("leiden-04", 3), ("leiden-03", 6), ("leiden-02", 10), ("leiden-01", 12)):
        for i in range(size):
            rid = f"sub_{cid[-2:]}{i:03d}:2026-08-01"
            rows.append({"renewal_id": rid, "leiden": cid, "plan_tier": "pro", "is_reference": True})
            ren.append({"renewal_id": rid, "route": "model", "plan_tier": "pro", "limit_hits_14d": i % 4,
                        "first_renewal_after_pricing_change": 0, "incident_exposed_28d": i % 2,
                        "overage_toggled_off": int(w_flag and cid == "leiden-03")})
    return pd.DataFrame(rows), pd.DataFrame(ren)


def test_a_withheld_complement_that_a_metrics_cell_prints_counts_as_published():
    """Verifier p3-hardening round 1 (minor): metric_lapse_rate prints the n and lapses of any cell of plan x
    first-after x incident x overage x a limit_hits_14d range. A withheld complement that is exactly such a
    cell hides nothing (with the plan totals, the small cohort beside it would come back), so the plan's
    next published cohort is withheld as well. lapse_rate_cells finds exactly the cohorts that are a
    printable cell (the tightest cell around the members holds no one else, 5+ renewals)."""
    table, ren = _metrics_layout(w_flag=False)
    assert cohorts.lapse_rate_cells(table, "leiden", ren) == frozenset()
    assert cohorts.withheld_cohorts(table, "leiden", ren) == {"leiden-04", "leiden-03"}
    table, ren = _metrics_layout(w_flag=True)
    assert cohorts.lapse_rate_cells(table, "leiden", ren) == {"leiden-03"}
    assert cohorts.withheld_cohorts(table, "leiden") == {"leiden-04", "leiden-03"}          # without the check
    assert cohorts.withheld_cohorts(table, "leiden", ren) == {"leiden-04", "leiden-03", "leiden-02"}
    assert cohorts.published_sizes(table, "leiden", ren) == [12, None, None, None]
    # a small cohort that is a cell is no metrics answer (metrics suppresses n < 5 itself): still unknown
    small = ren.assign(overage_toggled_off=(table["leiden"] == "leiden-04").astype(int))
    assert cohorts.lapse_rate_cells(table, "leiden", small) == frozenset()


# --------------------------------------------------------------------------- a private tiny build
@pytest.fixture(scope="module")
def cohort_build(tmp_path_factory):
    """(graph root, build dir, record): a tiny build of its own with cohorts written into it."""
    root = tmp_path_factory.mktemp("cohorts_root")
    make_exports(spec.sample_dir("tiny", root), spec.export_dir("tiny", root))
    bdir, _ = build.build_profile("tiny", graph_root=root, log=_quiet)
    _path, rec = cohorts.build_cohorts(bdir, log=_quiet)
    return root, bdir, rec


def test_build_writes_cohorts_parquet_and_the_manifest_sidecar(cohort_build):
    _root, bdir, rec = cohort_build
    table = pq.read_table(bdir / cohorts.COHORTS_FILE)
    assert table.schema.equals(cohorts.SCHEMA) and not table.schema.metadata
    df = table.to_pandas()
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet", columns=["renewal_id", "route"]).to_pandas()
    assert list(df["renewal_id"]) == sorted(ren["renewal_id"]) and len(df) == 121
    assert (df["is_reference"] == df["renewal_id"].map(ren.set_index("renewal_id")["route"].eq("model"))).all()
    assert df["leiden"].notna().all() and df["louvain"].notna().all()
    sim = pq.read_table(bdir / "parquet/edges_SIMILAR_TO.parquet").to_pandas()
    rank1 = sim[sim["rank"] == 1].set_index("src")["dst"]
    others = df[~df["is_reference"]]
    assert (others["assigned_via"] == "nearest_reference").all() and len(others) == 13
    assert (others["via_renewal_id"] == others["renewal_id"].map(rank1)).all()
    man = mf.read_manifest(bdir)
    assert man[cohorts.MANIFEST_KEY] == rec
    assert rec["library"] == "networkx" and rec["library_version"] == nx.__version__ == "3.7"
    assert (rec["seed"], rec["resolution"], rec["weight"], rec["in_contract"]) == (42, 1.0, "1 / (1 + dist)", False)
    for algo in cohorts.ALGORITHMS:
        a = rec["algorithms"][algo]
        assert a["communities"] == len(a["sizes"]) == df[algo].nunique() and 0 < a["modularity"] < 1
        assert a["plan_purity"] == 1.0 and "seconds" not in a
        # sizes: the published ones largest first, then one null per withheld cohort (never its value)
        truth = df[df["is_reference"]].groupby(algo).size()
        withheld = cohorts.withheld_cohorts(df, algo)
        shown = sorted((int(v) for c, v in truth.items() if c not in withheld), reverse=True)
        assert int(truth.sum()) == 108 and a["sizes"] == [*shown, *[None] * len(withheld)]
        assert withheld == {c for c, v in truth.items() if v < cohorts.MIN_CELL}   # tiny: ultra's lone cohort only
    assert rec["sha256"] == mf.sha256_file(bdir / cohorts.COHORTS_FILE)
    assert rec["inputs_sha256"]["parquet/nodes_Renewal.parquet"] == man["files"]["parquet/nodes_Renewal.parquet"][
        "sha256"]


def test_rebuild_is_byte_identical_and_keeps_the_record(cohort_build):
    _root, bdir, rec = cohort_build
    before = (bdir / cohorts.COHORTS_FILE).read_bytes()
    logs: list[str] = []
    _path, again = cohorts.build_cohorts(bdir, log=logs.append)
    assert (bdir / cohorts.COHORTS_FILE).read_bytes() == before and again == rec
    assert logs and "unchanged" in logs[0]
    assert not [p.name for p in bdir.iterdir() if p.name.startswith(".cohorts")]      # no temp file left


@pytest.mark.parametrize("hashseed", ["1", "987"])
def test_same_seed_same_bytes_in_a_fresh_process(cohort_build, tmp_path, hashseed):
    _root, bdir, rec = cohort_build
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import pyarrow.parquet as pq; "
            "from lakehouse_graph import cohorts; "
            "r = pq.read_table(sys.argv[2] + '/parquet/nodes_Renewal.parquet', columns=['renewal_id', 'plan_tier', "
            "'route']).to_pandas(); s = pq.read_table(sys.argv[2] + '/parquet/edges_SIMILAR_TO.parquet').to_pandas(); "
            "t, _ = cohorts.assign(r, s); print(cohorts.write_table(t, __import__('pathlib').Path(sys.argv[3])))")
    p = subprocess.run([sys.executable, "-c", code, str(REPO / "src"), str(bdir), str(tmp_path / "c.parquet")],
                       capture_output=True, text=True, check=False, timeout=300,
                       env=dict(os.environ, PYTHONHASHSEED=hashseed))
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == rec["sha256"]


def test_another_seed_is_recorded(cohort_build, tmp_path):
    _root, bdir, _rec = cohort_build
    scratch = tmp_path / "copy"
    scratch.mkdir()
    for name in ("parquet", spec.SCALER_FILE, mf.MANIFEST_FILE):
        src = bdir / name
        (shutil.copytree if src.is_dir() else shutil.copy2)(src, scratch / name)
    _p, rec = cohorts.build_cohorts(scratch, seed=7, log=_quiet)
    assert rec["seed"] == 7 and mf.read_manifest(scratch)["cohorts"]["seed"] == 7
    with pytest.raises(ValueError, match="seed"):
        cohorts.build_cohorts(scratch, seed="7", log=_quiet)  # type: ignore[arg-type]
    with pytest.raises(cohorts.CohortsUnavailable, match="make graph-build"):
        cohorts.build_cohorts(tmp_path / "nothing", log=_quiet)


def test_a_byte_identical_graph_rebuild_carries_the_cohorts_over(cohort_build):
    """build.CARRY_OVER registers cohorts.parquet + manifest["cohorts"]: a --rebuild keeps both."""
    root, bdir, rec = cohort_build
    before = (bdir / cohorts.COHORTS_FILE).read_bytes()
    logs: list[str] = []
    again, man = build.build_profile("tiny", graph_root=root, rebuild=True, log=logs.append)
    assert again == bdir and (bdir / cohorts.COHORTS_FILE).read_bytes() == before
    assert man["cohorts"] == rec == mf.read_manifest(bdir)["cohorts"]
    assert any("kept" in m and "cohorts.parquet" in m for m in logs)


# --------------------------------------------------------------------------- questions
def test_cohort_summary_by_id(cohort_build):
    _root, bdir, rec = cohort_build
    data, caveats = cohorts.cohort_summary(bdir, cohort_id="leiden-01")
    assert caveats[0] == cohorts.CAVEAT == "Cohorts rediscover feature segments; labels, not structure."
    assert data["cohort_id"] == "leiden-01" and data["algorithm"] == "leiden"
    size = rec["algorithms"]["leiden"]["sizes"][0]
    assert data["size"]["model_renewals"] == size >= cohorts.MIN_CELL
    o = data["outcomes"]
    assert o["n"] == size and o["visibility"] == "today" and not o["suppressed"]
    assert o["wilson_95"] == cohorts.wilson(o["voluntary_lapses"], o["n"]) and o["rate"] == round(
        o["voluntary_lapses"] / o["n"], 4)
    assert sum(v for v in data["plan_mix"].values() if v) == size and len(data["plan_mix"]) == 1   # purity 1.0
    assert 0 < len(data["top_features"]) <= cohorts.TOP_FEATURES
    assert all(f["feature"] in spec.FEATURES for f in data["top_features"])
    zs = [abs(f["mean_z"]) for f in data["top_features"]]
    assert zs == sorted(zs, reverse=True)
    prov = data["provenance"]
    assert (prov["library"], prov["library_version"], prov["seed"], prov["in_contract"]) == ("networkx", "3.7", 42,
                                                                                            False)
    assert prov["modularity"] == rec["algorithms"]["leiden"]["modularity"]
    assert json.loads(json.dumps(data)) == data                         # plain JSON: no numpy / pandas scalars
    assert cohorts.cohort_summary(bdir, cohort_id="leiden-01") == (data, caveats)        # deterministic
    assert cohorts.cohort_summary(bdir, cohort_id="louvain-01", algorithm="null")[0]["algorithm"] == "louvain"
    assert "sub_" not in json.dumps(data)                               # no renewal id is ever listed


def test_small_cohorts_are_suppressed(cohort_build):
    _root, bdir, rec = cohort_build
    smallest = len(rec["algorithms"]["leiden"]["sizes"])
    assert rec["algorithms"]["leiden"]["sizes"][-1] is None     # tiny: ultra's 4 renewals (the plan's honesty
    data, caveats = cohorts.cohort_summary(bdir, cohort_id=f"leiden-{smallest:02d}")   # note 10), withheld
    o = data["outcomes"]
    assert o["suppressed"] and o["n"] is None and o["voluntary_lapses"] is None and o["rate"] is None
    assert o["wilson_95"] is None and data["size"]["model_renewals"] is None and data["top_features"] == []
    assert set(data["plan_mix"].values()) == {None} and data["name"] == f"ultra: {cohorts.WITHHELD_NAME}"
    assert o["not_yet_observed"] is None
    assert any("suppressed" in c for c in caveats)
    listed, _ = cohorts.cohort_list(bdir)
    row = next(r for r in listed["cohorts"] if r["cohort_id"] == f"leiden-{smallest:02d}")
    assert row["suppressed"] and row["model_renewals"] is None and row["rate"] is None


@pytest.mark.parametrize("min_cell", [5, 9])
def test_a_suppressed_n_cannot_be_recovered_by_subtraction(cohort_build, monkeypatch, min_cell):
    """model_renewals = n + not_yet_observed + excluded_named_renewal. For every tiny renewal and both
    algorithms: when n is suppressed, not_yet_observed is too (so n cannot be worked out from the
    published cells); when n is published, the identity holds exactly; a withheld size takes its plan
    mix and not_yet_observed with it. MIN_CELL 9 makes tiny's leiden pro block hold one small cohort
    (8) next to published ones, so its complement (9) is withheld as well."""
    monkeypatch.setattr(cohorts, "MIN_CELL", min_cell)
    _root, bdir, _rec = cohort_build
    ids = pq.read_table(bdir / cohorts.COHORTS_FILE, columns=["renewal_id"]).column(0).to_pylist()
    hidden_with_size = 0
    for rid in ids:
        for algo in cohorts.ALGORITHMS:
            data, caveats = cohorts.cohort_summary(bdir, renewal_id=rid, algorithm=algo)
            o, size = data["outcomes"], data["size"]["model_renewals"]
            if o["suppressed"]:
                assert o["n"] is None and o["voluntary_lapses"] is None and o["not_yet_observed"] is None, (rid, o)
                assert o["rate"] is None and o["wilson_95"] is None
                assert any("not-yet-observed count" in c for c in caveats)
                hidden_with_size += size is not None
            elif size is not None:
                assert size == o["n"] + o["not_yet_observed"] + int(o["excluded_named_renewal"]), (rid, algo, o)
            if size is None:
                assert o["not_yet_observed"] is None and set(data["plan_mix"].values()) == {None}, (rid, algo, o)
    assert hidden_with_size > 0              # the case the rule exists for: size published, n hidden


def _plan_totals(bdir) -> dict[str, tuple[int, int]]:
    """What a population template serves today: each plan's model n and lapses."""
    from lakehouse_graph import queries

    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        rows = queries.fetch(conn, "first_renewal_after_by_plan")
    finally:
        conn.close()
        db.close()
    out: dict[str, tuple[int, int]] = {}
    for r in rows:
        n, lapses = out.get(r["plan_tier"], (0, 0))
        out[r["plan_tier"]] = (n + int(r["n"]), lapses + int(r["lapses"] or 0))
    return out


@pytest.mark.parametrize("min_cell", [5, 9, 16, 22])
def test_plan_totals_minus_the_published_cohorts_never_give_a_withheld_one_back(cohort_build, monkeypatch,
                                                                                 min_cell):
    """Across tools: first_renewal_after_by_plan publishes each plan's model n and lapses as of today.
    Subtracting the published cohort_list rows of that plan must never pin one withheld cohort,
    unless that cohort is the plan's whole block (then it IS the plan's population cell: tiny ultra)."""
    monkeypatch.setattr(cohorts, "MIN_CELL", min_cell)
    _root, bdir, _rec = cohort_build
    totals = _plan_totals(bdir)
    table = pq.read_table(bdir / cohorts.COHORTS_FILE).to_pandas()
    complements = 0
    for algo in cohorts.ALGORITHMS:
        listed, caveats = cohorts.cohort_list(bdir, algo)
        assert any("smallest other cohort of the same plan" in c for c in caveats)
        rows = {r["cohort_id"]: r for r in listed["cohorts"]}
        plans = table[table["is_reference"]].groupby(algo)["plan_tier"].agg(lambda s: tuple(sorted(set(s))))
        withheld = cohorts.withheld_cohorts(table, algo)
        assert {c for c, r in rows.items() if r["suppressed"]} == {c for c, r in rows.items()
                                                                   if r["model_renewals"] is None} == withheld
        sizes = table[table["is_reference"]].groupby(algo).size()
        complements += sum(int(sizes[c]) >= min_cell for c in withheld)
        for plan in totals:
            members = [c for c in rows if plans[c] == (plan,)]
            hidden = [c for c in members if rows[c]["model_renewals"] is None]
            assert len(hidden) != 1 or members == hidden, (algo, plan, hidden, members)
        # assigned non-reference renewals per cohort add up to the population route totals of the build
        # (routes) and of each plan (metric_route_counts): never exactly one hidden cell beside published
        # ones in either, and published cells are exact
        others = table[~table["is_reference"]][algo].value_counts()
        assigned = {c: cohorts.cohort_summary(bdir, cohort_id=c)[0]["size"]["assigned_non_reference"] for c in rows}
        assert sum(v is None for v in assigned.values()) != 1 or len(assigned) == 1, (algo, assigned)
        assert all(v is None or v == int(others.get(c, 0)) >= min_cell for c, v in assigned.items())
        for plan in totals:
            members = [c for c in rows if plans[c] == (plan,)]
            assert sum(assigned[c] is None for c in members) != 1 or len(members) == 1, (algo, plan, assigned)
    # only MIN_CELL 9 leaves a plan with exactly one small cohort beside published ones (leiden pro: 8)
    assert (complements > 0) == (min_cell == 9)


def _size_candidates(features: list[dict], integer_features: set, lo: int, hi: int) -> list[int]:
    """The verifier's probe_means: the sizes n in [lo, hi] for which every published member mean of an
    integer-valued feature (cohort_mean, 4 decimals) times n is a whole number."""
    out = []
    for n in range(lo, hi + 1):
        means = [f["cohort_mean"] for f in features if f["feature"] in integer_features and "cohort_mean" in f]
        if all(abs(m * n - round(m * n)) <= 0.00005 * n + 1e-9 for m in means):
            out.append(n)
    return out


def test_a_complement_cohort_is_withheld_in_every_view(cohort_build, monkeypatch):
    """MIN_CELL 9 in tiny: leiden-06 (8 model renewals) is small and leiden-05 (9) is the smallest other
    cohort of the pro block, so it is withheld although it has 9, in every view. The verifier's major
    (p2d-cohorts-viz round 2): its published top_features cohort_mean values pinned its size to exactly 9
    within the label bounds [9, 15], and the pro plan total (89) minus the published cohorts (72) minus 9
    gave the small cohort's n = 8 back. Now a withheld cohort prints no member statistic, and the small
    cohort and its complement read alike (name, fields, caveats), so every size in the label interval
    stays possible."""
    monkeypatch.setattr(cohorts, "MIN_CELL", 9)
    _root, bdir, _rec = cohort_build
    table = pq.read_table(bdir / cohorts.COHORTS_FILE).to_pandas()
    assert cohorts.withheld_cohorts(table, "leiden") == {"leiden-05", "leiden-06", "leiden-07"}
    assert cohorts.published_sizes(table, "leiden") == [31, 21, 20, 15, None, None, None]
    data, caveats = cohorts.cohort_summary(bdir, cohort_id="leiden-05")
    o = data["outcomes"]
    assert o["suppressed"] and o["n"] is None and o["voluntary_lapses"] is None and o["rate"] is None
    assert data["size"]["model_renewals"] is None and data["plan_mix"] == {"pro": None}
    assert data["name"] == f"pro: {cohorts.WITHHELD_NAME}" and data["top_features"] == []
    assert any("complementary suppression" in c for c in caveats)
    small, small_caveats = cohorts.cohort_summary(bdir, cohort_id="leiden-06")     # 8 < 9: the primary small cell

    def alike(d):   # all but the id and the assigned non-reference cell (a cell of its own, own rule)
        return {**d, "cohort_id": None, "size": {**d["size"], "assigned_non_reference": None}}

    assert alike(small) == alike(data) and small_caveats == caveats                 # they read alike
    # the verifier's probe: no published mean, so every size the label order allows stays possible
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet").to_pandas()
    integer_features = {f for f in spec.FEATURES if (ren[f].dropna() % 1 == 0).all()}
    listed = cohorts.cohort_list(bdir, "leiden")[0]["cohorts"]
    above = [r["model_renewals"] for r in listed if r["model_renewals"] is not None and r["cohort_id"] < "leiden-05"]
    candidates = _size_candidates(data["top_features"], integer_features, 9, min(above))
    assert candidates == list(range(9, 16)) and len(candidates) > 1
    # a historical member: every view of a withheld cohort is withheld (its n + not_yet + self is the size)
    for member in table[(table["leiden"] == "leiden-05") & table["is_reference"]]["renewal_id"]:
        d, cav = cohorts.cohort_summary(bdir, renewal_id=member)
        assert d["outcomes"]["visibility"] == "source_as_of" and d["size"]["model_renewals"] is None
        assert d["outcomes"]["n"] is None and d["outcomes"]["voluntary_lapses"] is None
        assert d["outcomes"]["not_yet_observed"] is None and set(d["plan_mix"].values()) == {None}
        assert d["top_features"] == [] and any("complementary suppression" in c for c in cav)


def test_a_past_view_is_published_only_as_a_lower_bound_far_from_today(cohort_build):
    """A named historical renewal's view counts the members observed by its as_of, never itself: lower
    bounds of today's published cell. It differs from today's by D = not yet observed + itself; with
    0 < D < MIN_CELL today's lapses minus its lapses would print the outcome of fewer than MIN_CELL
    renewals (its own among them), so such a view is withheld (complementary suppression)."""
    _root, bdir, _rec = cohort_build
    ids = pq.read_table(bdir / cohorts.COHORTS_FILE, columns=["renewal_id"]).column(0).to_pylist()
    today = {r["cohort_id"]: r for r in cohorts.cohort_list(bdir)[0]["cohorts"]}
    seen = {"published": 0, "behind": 0}
    for rid in ids:
        data, caveats = cohorts.cohort_summary(bdir, renewal_id=rid)
        o, cid = data["outcomes"], data["cohort_id"]
        if o["visibility"] != "source_as_of" or today[cid]["suppressed"]:
            continue
        if o["suppressed"]:
            assert o["n"] is None and o["voluntary_lapses"] is None and o["not_yet_observed"] is None
            if any("differs from the cohort's published counts as of today" in c for c in caveats):
                seen["behind"] += 1
            continue
        behind = o["not_yet_observed"] + int(o["excluded_named_renewal"])
        assert behind == 0 or behind >= cohorts.MIN_CELL, (rid, o)
        assert o["n"] <= today[cid]["model_renewals"] and o["voluntary_lapses"] <= today[cid]["voluntary_lapses"]
        assert behind == 0 or any("lower bounds" in c for c in caveats)
        seen["published"] += 1
    assert seen["published"] > 0 and seen["behind"] > 0, seen


def test_build_cohorts_reads_the_manifest_under_the_lock_and_hashes_what_it_read(cohort_build, monkeypatch,
                                                                                tmp_path):
    """The record's inputs_sha256 are the sha256 of the Parquet bytes the cohorts were computed from,
    read with the manifest under the graph root's build lock (a concurrent --rebuild cannot slip
    between them); Parquet that no longer matches the sha256 its manifest pins is refused."""
    root, bdir, rec = cohort_build
    assert rec["inputs_sha256"] == {rel: mf.sha256_file(bdir / rel) for rel in cohorts.INPUT_FILES}
    held = []
    real = mf.read_manifest

    def read_manifest(path):          # probe: is the build lock held while the manifest is read?
        try:
            with store.BuildLock(root, timeout=0):
                held.append(False)
        except TimeoutError:
            held.append(True)
        return real(path)

    monkeypatch.setattr(cohorts.mf, "read_manifest", read_manifest)
    cohorts.build_cohorts(bdir, log=_quiet)
    assert held and held[0] is True, held
    monkeypatch.setattr(cohorts.mf, "read_manifest", real)
    # a copy whose SIMILAR_TO Parquet was changed after the build (manifest not updated): refused
    scratch = tmp_path / "edited"
    shutil.copytree(bdir, scratch, ignore=shutil.ignore_patterns("*.lbdb*", "lineage*"))
    rel = cohorts.INPUT_FILES[1]
    t = pq.read_table(scratch / rel).to_pandas()
    t.loc[t.index[0], "dist"] = float(t.loc[t.index[0], "dist"]) + 1.0
    build.write_parquet(t, scratch / rel, spec.EDGE_SCHEMA["SIMILAR_TO"].schema)
    with pytest.raises(cohorts.CohortsUnavailable, match=re.escape("differ from the sha256 its manifest.json pins")):
        cohorts.build_cohorts(scratch, log=_quiet)


# --------------------------------------------------------------------------- the suppression property
SCENARIOS = 80
PAST_START = pd.Timestamp("2026-06-01")


def _scenario(rng, min_cell: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(cohorts table, Renewal frame) of a random cohort layout: 1-3 plans, 1-4 cohorts each, sizes from 1
    to 40 (often around MIN_CELL), 0-6 non-reference renewals assigned to each, random labels and dates,
    0/1 flags and limit_hits_14d 0-9; in some plans one cohort's members alone toggled overage off (or
    have a first renewal after a cut), which makes that cohort exactly a metric_lapse_rate cell."""
    plans = rng.sample(["pro", "pro_plus", "ultra"], rng.randint(1, 3))
    groups = []
    for plan in plans:
        for _ in range(rng.randint(1, 4)):
            small = rng.random() < 0.5
            groups.append((plan, rng.randint(1, min_cell + 3) if small else rng.randint(min_cell, 40)))
    renewals, members, k = [], [], 0
    for g, (plan, size) in enumerate(groups):
        ids = []
        for _ in range(size):
            as_of = PAST_START + pd.Timedelta(days=rng.randint(0, 100))
            rid = f"sub_{k:05d}:{as_of.date()}"
            k += 1
            ids.append(rid)
            renewals.append({"renewal_id": rid, "plan_tier": plan, "route": "model", "as_of": as_of,
                             "churned": int(rng.random() < 0.25),
                             "outcome_observed_on": as_of + pd.Timedelta(days=7 + rng.randint(0, 25)),
                             "group": g, "is_reference": True})
        members.append(ids)
        for _ in range(rng.randint(0, 6)):
            route = rng.choice(["dunning", "cancel_flow", "score_today", "pending"])
            current = route in cohorts.CURRENT_ROUTES
            as_of = PAST_START + pd.Timedelta(days=130 if current else rng.randint(0, 100))
            rid = f"sub_{k:05d}:{as_of.date()}"
            k += 1
            renewals.append({"renewal_id": rid, "plan_tier": plan, "route": route, "as_of": as_of, "churned": 0,
                             "outcome_observed_on": pd.NaT if current else as_of + pd.Timedelta(days=9),
                             "group": g, "is_reference": False, "via": rng.choice(ids)})
    ren = pd.DataFrame(renewals)
    for f in spec.FEATURES:
        ren[f] = [float(rng.randint(0, 9)) if f in spec.INT_FEATURES else rng.random() * 3 for _ in range(len(ren))]
    for f in METRIC_FLAGS:
        ren[f] = [float(rng.random() < 0.3) for _ in range(len(ren))]
    for plan in plans:                            # make one cohort of the plan a metrics cell, now and then
        if rng.random() < 0.5:
            g = rng.choice([g for g, (p, _size) in enumerate(groups) if p == plan])
            flag = rng.choice(["overage_toggled_off", "first_renewal_after_pricing_change"])
            in_plan = ren["plan_tier"] == plan
            ren.loc[in_plan, flag] = (ren.loc[in_plan, "group"] == g).astype(float)
    # labels: by size (descending), then by smallest member id (cohorts.cohort_ids)
    order = sorted(range(len(groups)), key=lambda g: (-groups[g][1], min(members[g])))
    label = {g: f"leiden-{rank:02d}" for rank, g in enumerate(order, start=1)}
    table = pd.DataFrame({
        "renewal_id": ren["renewal_id"], "plan_tier": ren["plan_tier"], "is_reference": ren["is_reference"],
        "leiden": ren["group"].map(label), "louvain": ren["group"].map(label),
        "assigned_via": ren["is_reference"].map({True: cohorts.ASSIGNED_MEMBER, False: cohorts.ASSIGNED_NEAREST}),
        "via_renewal_id": ren.get("via"), "spec_version": cohorts.COHORT_SPEC_VERSION})
    table["via_renewal_id"] = table["via_renewal_id"].where(table["via_renewal_id"].notna(), None)
    return table.sort_values("renewal_id").reset_index(drop=True), ren


def _write_scenario(bdir, table: pd.DataFrame, ren: pd.DataFrame) -> None:
    (bdir / "parquet").mkdir(parents=True)
    cohorts.write_table(table, bdir / cohorts.COHORTS_FILE)
    cols = ["renewal_id", "plan_tier", "route", "as_of", "churned", "outcome_observed_on", *spec.FEATURES]
    ren[cols].to_parquet(bdir / "parquet/nodes_Renewal.parquet", index=False)
    ref = ren[ren["is_reference"]]
    pd.DataFrame({"feature": list(spec.FEATURES), "mean": [float(ref[f].mean()) for f in spec.FEATURES],
                  "std": [float(ref[f].std(ddof=0)) or 1.0 for f in spec.FEATURES], "n_ref": len(ref),
                  "spec_version": "similar_to/renewal-v1"}).to_parquet(bdir / spec.SCALER_FILE, index=False)
    mf.write_manifest(bdir, {"cohorts": {"library": "networkx", "library_version": nx.__version__, "seed": 42,
                                         "algorithms": {}}})


METRIC_FLAGS = ("first_renewal_after_pricing_change", "incident_exposed_28d", "overage_toggled_off")


def _lapse_rate_cells(ren: pd.DataFrame) -> set[frozenset]:
    """Every model-renewal cell metric_lapse_rate prints with 5+ renewals, by brute force: plan (or all), first
    renewal after a cut (or either), any limit_hits_14d range, and up to two of the incident / overage keys
    (metrics.py), computed here independently of cohorts.lapse_rate_cells."""
    model = ren[ren["route"] == spec.REFERENCE_ROUTE].set_index("renewal_id")
    ids = model.index.to_numpy()
    hits = model["limit_hits_14d"].to_numpy()
    out = set()
    for plan in [None, *sorted(model["plan_tier"].unique())]:
        base = np.ones(len(model), bool) if plan is None else (model["plan_tier"] == plan).to_numpy()
        for first in (None, 0.0, 1.0):
            sel = base if first is None else base & (model["first_renewal_after_pricing_change"] == first).to_numpy()
            for lo in range(10):
                for hi in range(lo, 10):
                    ranged = sel & (hits >= lo) & (hits <= hi)
                    for inc in (None, 0.0, 1.0):
                        for off in (None, 0.0, 1.0):
                            cell = ranged
                            if inc is not None:
                                cell = cell & (model["incident_exposed_28d"] == inc).to_numpy()
                            if off is not None:
                                cell = cell & (model["overage_toggled_off"] == off).to_numpy()
                            if cell.sum() >= 5:
                                out.add(frozenset(ids[cell]))
    return out


def _printed(bdir, table, ren) -> dict:
    """Everything the cohort tools print about one layout (cohort_list; cohort_summary by every id and
    for every renewal), plus what the population tools print: each plan's model n and lapses, the
    non-reference renewals of the build (routes) and of each plan by route (metric_route_counts), and the
    metric_lapse_rate cells (n and lapses) that are exactly one cohort."""
    listed = {r["cohort_id"]: r for r in cohorts.cohort_list(bdir, "leiden")[0]["cohorts"]}
    by_id = {c: cohorts.cohort_summary(bdir, cohort_id=c) for c in listed}
    views = {rid: cohorts.cohort_summary(bdir, renewal_id=rid) for rid in table["renewal_id"]}
    model = ren[ren["route"] == spec.REFERENCE_ROUTE]
    cells = _lapse_rate_cells(ren)
    members = model.groupby("group")["renewal_id"].agg(frozenset)
    label = table[table["is_reference"]].groupby("leiden")["renewal_id"].first()
    group_of = model.set_index("renewal_id")["group"]
    metrics_cells = {c for c, rid in label.items() if members[group_of[rid]] in cells}
    others = ren[ren["route"] != spec.REFERENCE_ROUTE]
    return {"listed": listed, "by_id": by_id, "views": views,
            "plan_n": model.groupby("plan_tier").size().to_dict(),
            "plan_lapses": model.groupby("plan_tier")["churned"].sum().to_dict(),
            "non_reference": len(others),
            "plan_route_non_reference": others.groupby(["plan_tier", "route"]).size().to_dict(),
            "metrics_cells": metrics_cells}


def _recoverable(equations: list[dict], unknowns: list) -> set:
    """Unknowns whose exact value follows from published sums by linear algebra: the unit vector lies in
    the row space of the equations (each is {unknown: coefficient}; its published value minus the
    published cells it sums is known, so only the coefficients matter). Bounds are not used."""
    col = {u: i for i, u in enumerate(unknowns)}
    rows = np.zeros((len(equations), len(unknowns)))
    for row, eq in zip(rows, equations, strict=True):
        for u, coef in eq.items():
            row[col[u]] = coef
    rank = int(np.linalg.matrix_rank(rows)) if len(rows) else 0
    out = set()
    for u in unknowns:
        unit = np.zeros((1, len(unknowns)))
        unit[0, col[u]] = 1.0
        if int(np.linalg.matrix_rank(np.vstack([rows, unit]))) == rank:
            out.add(u)
    return out


def _check_layout(p: dict, table: pd.DataFrame, min_cell: int) -> int:
    """The suppression property on one layout (see the test); returns how many withheld cohorts it had."""
    listed, by_id = p["listed"], p["by_id"]
    plan_of = {c: next(iter(by_id[c][0]["plan_mix"])) for c in listed}
    plans = {pl: sorted(c for c in listed if plan_of[c] == pl) for pl in set(plan_of.values())}
    withheld = {c for c, r in listed.items() if r["model_renewals"] is None}
    sizes = table[table["is_reference"]].groupby("leiden").size()             # the truth, never printed
    # unknowns: per cohort its size s, today's lapses l and assigned non-reference count a (and a's split by
    # route, r); per published past view with members behind today's cell, those members' lapses d (today's
    # l minus the view's)
    eqs: list[dict] = []
    routes = sorted({rt for _pl, rt in p["plan_route_non_reference"]})
    unknowns = [(x, c) for c in listed for x in ("s", "l", "a")] + [("r", (c, rt)) for c in listed for rt in routes]
    for c, r in listed.items():
        data = by_id[c][0]
        if r["model_renewals"] is not None:
            assert r["model_renewals"] >= min_cell and not r["suppressed"], (c, r)
            eqs += [{("s", c): 1}, {("l", c): 1}]
            assert all(v is None or v >= min_cell for v in data["plan_mix"].values()), data["plan_mix"]
        else:   # withheld: no size, no outcome count, no member statistic, the one name, in every view
            assert r["suppressed"] and r["voluntary_lapses"] is None and data["top_features"] == [], (c, data)
            assert data["name"] == f"{plan_of[c]}: {cohorts.WITHHELD_NAME}" and set(data["plan_mix"].values()) == {None}
        a = data["size"]["assigned_non_reference"]
        if a is not None:
            assert a >= min_cell
            eqs.append({("a", c): 1})
    for pl, cs in plans.items():
        eqs += [{("s", c): 1 for c in cs}, {("l", c): 1 for c in cs}]          # the population templates
        # the plan's withheld cohorts that no metrics cell prints (a withheld cohort that is one has its n
        # and lapses printed by metric_lapse_rate, 5+ renewals), together: 0, or MIN_CELL or more, or the
        # plan's whole block; never one of them beside published ones
        printed = [c for c in cs if c in withheld and c in p["metrics_cells"]]
        together = p["plan_n"][pl] - sum(listed[c]["model_renewals"] or 0 for c in cs) - \
            sum(int(sizes[c]) for c in printed)
        assert together == 0 or together >= min_cell or set(cs) <= withheld, (pl, together)
        hidden = [c for c in cs if c in withheld]
        assert len(set(hidden) - set(printed)) != 1 or set(cs) <= withheld, (pl, hidden, printed)
        # withheld cohorts of a plan read alike: the same fields and caveats but their id and assigned cell
        alike = {str({**by_id[c][0], "cohort_id": None, "size": {**by_id[c][0]["size"],
                                                                  "assigned_non_reference": None}}) for c in hidden}
        assert len(alike) <= 1 and len({str(by_id[c][1]) for c in hidden}) <= 1, pl
    eqs.append({("a", c): 1 for c in listed})                                  # the route totals
    hidden_a = [c for c in listed if by_id[c][0]["size"]["assigned_non_reference"] is None]
    shown_a = sum(by_id[c][0]["size"]["assigned_non_reference"] or 0 for c in listed)
    assert len(hidden_a) != 1 or len(listed) == 1
    assert p["non_reference"] - shown_a == 0 or p["non_reference"] - shown_a >= min_cell or len(hidden_a) == len(listed)
    for c in listed if routes else ():                                         # a's split by route
        eqs.append({("a", c): 1, **{("r", (c, rt)): -1 for rt in routes}})
    for pl, cs in plans.items():                                                # metric_route_counts(plan_tier)
        for rt in routes:
            eqs.append({("r", (c, rt)): 1 for c in cs})
        eqs.append({("a", c): 1 for c in cs})
        hidden = [c for c in cs if c in hidden_a]
        shown = sum(by_id[c][0]["size"]["assigned_non_reference"] or 0 for c in cs)
        total = sum(n for (plan, _rt), n in p["plan_route_non_reference"].items() if plan == pl)
        assert len(hidden) != 1 or hidden == cs, (pl, hidden)
        assert total - shown == 0 or total - shown >= min_cell or hidden == cs, (pl, total, shown)
    for c in p["metrics_cells"]:                                                # metric_lapse_rate prints them
        eqs += [{("s", c): 1}, {("l", c): 1}]
    behind = {}
    for rid, (data, _caveats) in p["views"].items():
        o, c = data["outcomes"], data["cohort_id"]
        if o["n"] is None:
            assert o["voluntary_lapses"] is None and o["not_yet_observed"] is None and o["suppressed"], (rid, o)
            continue
        assert c not in withheld and o["n"] >= min_cell, (rid, o)
        size = data["size"]["model_renewals"]
        d = o["not_yet_observed"] + int(o["excluded_named_renewal"])
        assert size == o["n"] + d and (d == 0 or d >= min_cell), (rid, o)   # n + not_yet + self = size
        assert o["voluntary_lapses"] <= listed[c]["voluntary_lapses"]           # a lower bound of today's
        eqs.append({("s", c): 1})
        if d:
            behind[rid] = d
            unknowns.append(("d", rid))
            eqs.append({("l", c): 1, ("d", rid): -1})
    limit = 0
    for x, key in _recoverable(eqs, unknowns):
        if x == "d":
            assert behind[key] >= min_cell                                       # never a small group's lapses
        elif x == "r":
            continue                                                            # a split no tool prints
        elif key in withheld or x == "a":
            whole = plans[plan_of[key]] == [key]
            published_a = by_id[key][0]["size"]["assigned_non_reference"] is not None
            if x == "a":
                assert whole or published_a or len(listed) == 1, (x, key)       # only a plan's whole block
                continue
            # metrics prints it (5+ renewals); or the documented limit: no published cohort is left in the
            # plan, and every other withheld cohort of it is a metrics cell (cohorts.py "What it does not stop")
            documented = all(c in withheld for c in plans[plan_of[key]]) and \
                all(c in p["metrics_cells"] for c in plans[plan_of[key]] if c != key)
            assert whole or key in p["metrics_cells"] or documented, (x, key)
            limit += documented and key not in p["metrics_cells"] and not whole
    return len(withheld), len(p["metrics_cells"] & withheld), limit


def test_suppression_cannot_be_inverted_by_arithmetic(tmp_path, monkeypatch):
    """The suppression property, over random cohort layouts and MIN_CELL 3, 5 and 9 (seeded). From every
    number the cohort tools print (cohort_list, cohort_summary for every cohort and for every renewal,
    past views included) and the population tools' plan totals (model n and lapses) and route totals:
    no printed count is under MIN_CELL; no withheld cohort's size, lapses or assigned cell is determined
    by linear algebra over all of them (_recoverable), unless the cohort is its plan's whole block
    (the plan's population cell); a plan's withheld cohorts never number one beside published ones and
    hold 0 or MIN_CELL+ renewals together; a past view's lapses differ from today's by the outcomes of 0
    or MIN_CELL+ renewals; and withheld cohorts print no member statistic and read alike, small or not."""
    import random

    rng = random.Random(20261001)
    withheld = coincide = limit = 0
    for i in range(SCENARIOS):
        min_cell = rng.choice([3, 5, 9])
        table, ren = _scenario(rng, min_cell)
        bdir = tmp_path / f"layout{i:03d}"
        _write_scenario(bdir, table, ren)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(cohorts, "MIN_CELL", min_cell)
            w, c, x = _check_layout(_printed(bdir, table, ren), table, min_cell)
        withheld, coincide, limit = withheld + w, coincide + c, limit + x
    # the layouts exercise the rule: each old behaviour (no 'together' rule, a withheld complement's member
    # means, past views of withheld cohorts or 1-4 renewals behind today's cell, assigned cells paired against
    # the build's total only, a withheld complement that is a metrics cell left to cover a small cohort) fails
    # this test. Measured: see the assertion message.
    assert withheld >= 120 and coincide >= 3, (withheld, coincide, limit)


def test_cohort_summary_for_a_renewal_follows_the_outcome_visibility_rule(cohort_build, monkeypatch):
    _root, bdir, _rec = cohort_build
    monkeypatch.setattr(cohorts, "MIN_CELL", 1)                          # look at the counted rows themselves
    cols = ["renewal_id", "route", "as_of", "churned", "outcome_observed_on"]
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet", columns=cols).to_pandas().set_index("renewal_id")
    ren["as_of"] = pd.to_datetime(ren["as_of"])
    ren["outcome_observed_on"] = pd.to_datetime(ren["outcome_observed_on"])
    table = pq.read_table(bdir / cohorts.COHORTS_FILE).to_pandas().set_index("renewal_id")
    # a historical reference renewal: never its own outcome, only outcomes observed by its as_of
    rid = "sub_00000:2026-07-27"
    data, caveats = cohorts.cohort_summary(bdir, renewal_id=rid)
    cid = table.at[rid, "leiden"]
    members = ren.loc[table.index[table["leiden"] == cid]]
    members = members[members["route"] == "model"].drop(index=rid)
    seen = members[members["outcome_observed_on"] <= ren.at[rid, "as_of"]]
    o = data["outcomes"]
    assert data["named_renewal"] == {"renewal_id": rid, "is_reference": True, "assigned_via": "community",
                                     "via_renewal_id": None}
    assert o["visibility"] == "source_as_of" and o["n"] == len(seen) < len(members)
    assert o["voluntary_lapses"] == int(seen["churned"].sum()) and o["not_yet_observed"] == len(members) - len(seen)
    assert o["excluded_named_renewal"] is True
    assert any("never the named renewal's own outcome" in c for c in caveats)
    # the current hero: today's view, not a member itself (assigned through its rank-1 neighbour)
    data, caveats = cohorts.cohort_summary(bdir, renewal_id="sub_santosh:2026-10-07")
    assert data["outcomes"]["visibility"] == "today" and data["named_renewal"]["assigned_via"] == "nearest_reference"
    assert data["named_renewal"]["via_renewal_id"] is not None
    assert any("nearest reference renewal" in c for c in caveats)


@pytest.mark.parametrize("kwargs, message", [
    ({}, "exactly one"),
    ({"cohort_id": "leiden-01", "renewal_id": "sub_santosh:2026-10-07"}, "exactly one"),
    ({"cohort_id": "null", "renewal_id": "None"}, "exactly one"),
    ({"cohort_id": "leiden-1"}, "invalid cohort_id"),
    ({"cohort_id": "kmeans-01"}, "invalid cohort_id"),
    ({"cohort_id": "leiden-01; DROP"}, "invalid cohort_id"),
    ({"cohort_id": "leiden-99"}, "unknown cohort_id"),
    ({"cohort_id": "leiden-01", "algorithm": "louvain"}, "is a leiden cohort"),
    ({"renewal_id": "sub_santosh"}, "invalid renewal_id"),
    ({"renewal_id": "sub_nobody:2026-10-07"}, "unknown renewal_id"),
    ({"renewal_id": "sub_santosh:2026-10-07", "algorithm": "kmeans"}, "invalid algorithm"),
])
def test_invalid_input_is_a_clear_error(cohort_build, kwargs, message):
    with pytest.raises(ValueError, match=message):
        cohorts.cohort_summary(cohort_build[1], **kwargs)


def test_missing_cohorts_file_names_the_command_that_builds_it(tmp_path):
    with pytest.raises(cohorts.CohortsUnavailable,
                       match=re.escape(f"run python scripts/build_graph_cohorts.py --build {tmp_path}")):
        cohorts.cohort_summary(tmp_path, cohort_id="leiden-01")
    with pytest.raises(ValueError, match="build_dir"):
        cohorts.cohort_list(object())


def test_cohort_list_is_ordered_and_matches_the_summaries(cohort_build):
    _root, bdir, rec = cohort_build
    data, caveats = cohorts.cohort_list(bdir, "louvain")
    ids = [r["cohort_id"] for r in data["cohorts"]]
    assert ids == [f"louvain-{i:02d}" for i in range(1, rec["algorithms"]["louvain"]["communities"] + 1)]
    shown = [r["model_renewals"] for r in data["cohorts"] if r["model_renewals"] is not None]
    assert shown == sorted(shown, reverse=True) and caveats[0] == cohorts.CAVEAT
    first = cohorts.cohort_summary(bdir, cohort_id="louvain-01")[0]
    assert data["cohorts"][0]["rate"] == first["outcomes"]["rate"] and data["cohorts"][0]["name"] == first["name"]


def test_ctx_object_with_build_dir_works(cohort_build):
    ctx = type("Ctx", (), {"build_dir": cohort_build[1]})()
    assert cohorts.cohort_summary(ctx, cohort_id="leiden-01") == cohorts.cohort_summary(str(cohort_build[1]),
                                                                                         cohort_id="leiden-01")


def test_cli_build_list_summary(cohort_build):
    root, bdir, _rec = cohort_build
    p = run("build_graph_cohorts.py", "--profile", "tiny", "--graph-root", str(root))
    assert p.returncode == 0, p.stderr
    assert "Graph cohorts OK (cohorts/renewal-v1, networkx 3.7, seed 42" in p.stdout
    assert "outside the contract" in p.stdout
    p = run("build_graph_cohorts.py", "list", "--build", str(bdir))
    assert p.returncode == 0 and "leiden-01" in p.stdout and "suppressed (small cell)" in p.stdout
    p = run("build_graph_cohorts.py", "summary", "--renewal", "sub_santosh:2026-10-07", "--build", str(bdir), "--json")
    assert p.returncode == 0, p.stderr
    out = json.loads(p.stdout)
    assert out["data"]["named_renewal"]["renewal_id"] == "sub_santosh:2026-10-07" and out["caveats"][0] == cohorts.CAVEAT
    p = run("build_graph_cohorts.py", "summary", "--cohort", "leiden-99", "--build", str(bdir))
    assert p.returncode == 1 and "unknown cohort_id" in p.stderr
    p = run("build_graph_cohorts.py", "--profile", "s7", "--graph-root", str(root))
    assert p.returncode == 1 and "make graph-build PROFILE=s7" in p.stderr


@pytest.mark.slow
def test_cohorts_do_not_touch_the_graph_contract(cohort_build):
    """Outside the contract: with cohorts.parquet and manifest["cohorts"] in the build, the strict
    graph contract still passes (it neither reads nor checks them)."""
    root, bdir, _rec = cohort_build
    assert (bdir / cohorts.COHORTS_FILE).is_file() and "cohorts" in mf.read_manifest(bdir)
    p = run("check_graph_contract.py", "--profile", "tiny", "--graph-root", str(root), "--strict")
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-2000:]
    assert store.read_contract(bdir) is not None
    contract_src = (REPO / "scripts/check_graph_contract.py").read_text()
    assert cohorts.COHORTS_FILE not in contract_src                          # never reads the file
    assert not re.search(r"\bimport\b[^\n]*\bcohorts\b|\bcohorts\.(cohort_|build_|assign)", contract_src)


@pytest.mark.slow
def test_seed_42_numbers_against_the_plan(s42_build):
    """PLAN 2.2: Louvain 15 (modularity 0.805), Leiden 14, plan purity 1.0 (measured unweighted by
    REMAP). The spec weights edges 1 / (1 + dist): Louvain 15 / 0.8065 and Leiden 15 / 0.8056; the
    unweighted graph gives Louvain 15 and the plan's Leiden 14. The named cohorts of the plan
    (overage-off 21%, cap-pressure pro ~24%) come back."""
    bdir, _ = s42_build
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet").to_pandas()
    sim = pq.read_table(bdir / "parquet/edges_SIMILAR_TO.parquet").to_pandas()
    table, stats = cohorts.assign(ren, sim)
    lv, ld = stats["algorithms"]["louvain"], stats["algorithms"]["leiden"]
    assert (lv["communities"], ld["communities"]) == (15, 15)
    assert lv["modularity"] == pytest.approx(0.8065, abs=0.002) and ld["modularity"] == pytest.approx(0.8056, abs=0.002)
    assert lv["plan_purity"] == ld["plan_purity"] == 1.0
    assert stats["graph"]["nodes"] == 7387 and stats["assigned"] == {"community": 7387, "nearest_reference": 614}
    g, _ids = cohorts.reference_graph(ren, sim)
    for _u, _v, d in g.edges(data=True):
        d["weight"] = 1.0
    unweighted = [len(cohorts.detect(g, a)) for a in ("louvain", "leiden")]
    assert unweighted == [15, 14]
    ref = ren[ren["route"] == "model"].set_index("renewal_id")
    t = table.set_index("renewal_id")
    scaler = pq.read_table(bdir / spec.SCALER_FILE).to_pandas()
    for algo in cohorts.ALGORITHMS:
        ref[algo] = t[algo]
        named = {}
        for _cid, members in ref.groupby(algo):
            top = cohorts.top_features(members, scaler)[0]
            plan = members["plan_tier"].mode().iloc[0]
            named.setdefault((plan, top["feature"], top["direction"]), []).append(
                (len(members), int(members["churned"].sum())))
        # overage-off (PLAN: 21%): the pro cohort led by overage_toggled_off, 30 of 143 in both algorithms
        assert named[("pro", "overage_toggled_off", "higher")] == [(143, 30)], algo
        # cap pressure (PLAN: ~24.5%): the pro cohort led by limit_hits_14d; louvain 36 of 152 (23.7%),
        # leiden a tighter 30 of 94
        cap = named[("pro", "limit_hits_14d", "higher")]
        assert cap == ([(152, 36)] if algo == "louvain" else [(94, 30)]), (algo, cap)
        assert all(lapses / size > 0.2 for size, lapses in cap)


@pytest.mark.slow
def test_seed_42_assigned_cells_never_come_back_from_a_plan_total(s42_build, monkeypatch):
    """Verifier p3-hardening round 1 (major), planted: on s42 at MIN_CELL 9 the build-total rule published
    pro_plus's leiden-01 (78 assigned non-reference renewals) beside a hidden leiden-12 (8), and
    metric_route_counts(plan_tier='pro_plus') prints that plan's non-model routes (cancel_flow 30, dunning
    56, score_today 0, pending 0: 86), so 86 - 78 gave the 8 back; louvain-13 (8, pro_plus) and louvain-15
    (6, pro) came back the same way. Now no plan of either algorithm, at MIN_CELL 5, 9, 20 or 40, holds
    exactly one hidden cell beside published ones, or hidden cells holding 1 to MIN_CELL - 1 together."""
    bdir, _ = s42_build
    ren = pq.read_table(bdir / "parquet/nodes_Renewal.parquet").to_pandas()
    sim = pq.read_table(bdir / "parquet/edges_SIMILAR_TO.parquet").to_pandas()
    table, stats = cohorts.assign(ren, sim)
    assert stats["assigned"] == {"community": 7387, "nearest_reference": 614}   # every renewal has a cohort
    others = table[~table["is_reference"]]
    nonmodel = ren[ren["route"] != spec.REFERENCE_ROUTE].groupby("plan_tier").size()   # metric_route_counts

    def plan_cohorts(algo: str, plan: str) -> list[str]:
        return sorted(set(table.loc[table["plan_tier"] == plan, algo].dropna()))

    monkeypatch.setattr(cohorts, "MIN_CELL", 9)
    for algo, small, plan in (("leiden", "leiden-12", "pro_plus"), ("louvain", "louvain-13", "pro_plus"),
                              ("louvain", "louvain-15", "pro")):
        counts = others[algo].value_counts()
        old = cohorts.suppress_cells({c: int(counts.get(c, 0)) for c in sorted(table[algo].dropna().unique())}, True)
        cs = plan_cohorts(algo, plan)
        assert [c for c in cs if old[c] is None] == [small]                    # the old leak, pinned
        assert int(nonmodel[plan]) - sum(old[c] for c in cs if old[c] is not None) == int(counts[small]) < 9
        new = cohorts.assigned_cells(table, algo)
        assert new[small] is None and sum(new[c] is None for c in cs) >= 2, (algo, plan, new)
    for min_cell in (5, 9, 20, 40):
        monkeypatch.setattr(cohorts, "MIN_CELL", min_cell)
        for algo in cohorts.ALGORITHMS:
            new = cohorts.assigned_cells(table, algo)
            for plan in sorted(table["plan_tier"].unique()):
                cs = plan_cohorts(algo, plan)
                hidden = [c for c in cs if new[c] is None]
                left = int(nonmodel.get(plan, 0)) - sum(new[c] for c in cs if new[c] is not None)
                assert len(hidden) != 1 or hidden == cs, (min_cell, algo, plan, hidden)
                assert left == 0 or left >= min_cell or hidden == cs, (min_cell, algo, plan, left)
