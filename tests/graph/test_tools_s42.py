"""Seed 42 (N_USERS 8000): the PLAN's golden tool answers and the leak sweep over all 8,001 renewals.

Slow (the s42 profile is generated and built once per session by conftest). The same checks run as
``scripts/check_graph_tools.py --profile s42`` in CI.
"""
from __future__ import annotations

import os

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "0")

import pytest
from conftest import REPO

from lakehouse_graph import oracle, queries, tools
from lakehouse_graph.context import CURRENT_ROUTES, ToolContext

pytestmark = pytest.mark.slow
SANTOSH = "sub_santosh:2026-10-07"


@pytest.fixture(scope="module")
def ctx(s42_build, graph_root):
    c = ToolContext(s42_build[0], allow_unchecked=True, graph_root=graph_root, audit=False)
    yield c
    c.close()


def data(ctx, name, **args):
    return tools.call(ctx, name, args)["data"]


def test_plan_goldens(ctx):
    rows = data(ctx, "graph_renewal_evidence", renewal_id=SANTOSH)["rows"]
    assert [(r["event_date"], r["relation"], r["target_id"]) for r in rows] == [
        ("2026-08-15", "CUT_CAP", "cap-cut-2026-08"), ("2026-08-25", "EXPOSED_TO", "inc-002"),
        ("2026-09-09", "EXPOSED_TO", "inc-003"), ("2026-09-20", "CUT_CAP", "cap-cut-2026-09"),
        ("2026-09-20", "FIRST_RENEWAL_AFTER", "cap-cut-2026-09"), ("2026-09-24", "HIT_LIMIT", "lh:sub_santosh:001"),
        ("2026-09-25", "HIT_LIMIT", "lh:sub_santosh:002"), ("2026-09-27", "HIT_LIMIT", "lh:sub_santosh:003")]
    assert [r["in_feature_window"] for r in rows if r["relation"] == "EXPOSED_TO"] == [False, True]
    sim = data(ctx, "graph_similar_renewals", renewal_id=SANTOSH)
    assert [(r["renewal_id"], r["d2_q"]) for r in sim["rows"]][:2] == [("sub_07200:2026-08-17", 5099099315),
                                                                       ("sub_06614:2026-08-21", 5593304415)]
    assert sim["summary"]["lapsed"] == 2 and sim["summary"]["wilson_95"] == [0.057, 0.51]
    assert [r["renewal_id"] for r in sim["rows"] if r["outcome"] == "voluntary_lapse"] == ["sub_07200:2026-08-17",
                                                                                            "sub_01355:2026-09-05"]
    assert [x["feature"] for x in sim["rows"][0]["top3_feature_shares"]] == [
        "cheap_model_share_28d", "engagement_trend", "weekend_usage_ratio"]
    assert [(x["renewal_id"], x["path_dist"]) for x in sim["nearest_known_lapses"]] == [
        ("sub_07200:2026-08-17", 2.2581), ("sub_01355:2026-09-05", 2.5699), ("sub_05762:2026-09-11", 4.6438)]
    inc = data(ctx, "graph_exposure", entity_id="inc-002", response_format="detailed")
    # PLAN 3: pro 606/545/57/33/28, pro_plus 185/162/10/11/12, ultra 46/44/5/0/2. ultra dunning 2 is a cell under 5:
    # it is suppressed with its complements (ultra model in its row, pro_plus dunning and model across plans)
    assert {c["plan_tier"]: (c["exposed"], c["model"], c["voluntary_lapses"], c["cancel_flow"], c["dunning"],
                             c["current"]) for c in inc["cells"]} == {
        "pro": (606, 545, 57, 33, 28, 0), "pro_plus": (185, None, None, 11, None, 0),
        "ultra": (46, None, None, 0, None, 0)}
    assert inc["by_route"] == {"suppressed": False, "model": 751, "voluntary_lapses": 72, "cancel_flow": 44,
                               "dunning": 42, "current": 0}                # PLAN Q20: 751 model renewals, 72 lapses
    assert inc["naive_additional"] == 329 and inc["breakdown_withheld"] is False
    sep = data(ctx, "graph_exposure", entity_id="cap-cut-2026-09", renewal_id=SANTOSH)
    # PLAN Q6: total 1, the plan x route breakdown suppressed (all six rows null: no key names Santosh's plan or route)
    assert sep["total"] == 1 and sep["breakdown_withheld"] and all(c["suppressed"] for c in sep["cells"])
    assert sep["named_renewal_member"] is True and sep["known_by_as_of"] == {"true": None, "false": None}
    assert all(c[k] is None for c in sep["cells"] for k in tools.ROUTE_COUNTS)
    aug = data(ctx, "graph_exposure", entity_id="cap-cut-2026-08")
    assert aug["total"] == 2502 and aug["known_by_as_of"] == {"true": 2007, "false": 495}
    rows = {(c["plan_tier"], c["known_by_as_of"]): c for c in aug["cells"]}
    assert (rows[("pro", True)]["model"], rows[("pro", False)]["model"]) == (1443, 356)
    assert {c["plan_tier"]: (c["lapses"], c["n"]) for c in data(ctx, "metric_lapse_rate", group_by=["plan_tier"])
            ["cells"]} == {"pro": (464, 5815), "pro_plus": (73, 1258), "ultra": (11, 314)}
    tot = data(ctx, "metric_lapse_rate", plan_tier="pro", first_renewal_after_pricing_change=True,
               limit_hits_14d_min=3, limit_hits_14d_max=5)["total"]
    assert (tot["lapses"], tot["n"], tot["wilson_95"]) == (29, 72, [0.297, 0.518])
    rc = data(ctx, "metric_route_counts")
    routes = {c["route"]: c["renewals"] for c in rc["counts"]}
    # score_today (Santosh) is public by design: graph_find serves him with his route, graph_describe the 8,001
    assert routes["dunning"] == 326 and routes["cancel_flow"] == 287 and routes["score_today"] == 1
    assert rc["total_renewals"] == 8001 == data(ctx, "graph_describe")["nodes"]["Renewal"]


def test_no_null_is_computable_on_s42(ctx, s42_build):
    """The exact integer attack of scripts/check_graph_tools.py (rational simplex + branch and bound over the printed
    answers) pins no null of any s42 exposure, route-count or lapse-rate answer."""
    import importlib.util

    mspec = importlib.util.spec_from_file_location("check_graph_tools_s42", REPO / "scripts/check_graph_tools.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    t = oracle.load_tables(s42_build[0])
    rep, calls = mod.Report(), mod.Calls(ctx)
    mod.section_small_cells(rep, calls, t, ctx)
    assert rep.errors == [], rep.errors


def test_leak_sweep_over_every_renewal(ctx, s42_build):
    t = oracle.load_tables(s42_build[0])
    ren = t["Renewal"].set_index("renewal_id")
    observed = ren["outcome_observed_on"]
    declared, rejected, accepted, visible = 0, 0, 0, 0
    for rid in ren.index:
        as_of = ren.at[rid, "as_of"].date().isoformat()
        current = ren.at[rid, "route"] in CURRENT_ROUTES
        for r in data(ctx, "graph_renewal_evidence", renewal_id=rid)["rows"]:
            if r["relation"] == "FIRST_RENEWAL_AFTER" and r["event_date"] > as_of:
                declared += 1
                assert r["known_by_as_of"] is False and r["declared_exception"] is True
            else:
                assert r["event_date"] <= as_of, (rid, r)
        if not current:
            d = data(ctx, "graph_similar_renewals", renewal_id=rid, explain=False)
            for r in d["rows"]:
                if r["outcome"] != queries.NOT_YET_OBSERVED:
                    visible += 1
                    assert observed[r["renewal_id"]].date().isoformat() <= as_of
        try:
            data(ctx, "graph_similar_renewals", renewal_id=rid, outcome_visibility="today", explain=False)
            accepted += 1
        except tools.ToolInputError:
            rejected += 1
    assert declared == 495 and (rejected, accepted) == (8000, 1) and visible > 0


def test_check_graph_tools_script_on_s42(s42_build, graph_root):
    import subprocess
    import sys

    p = subprocess.run([sys.executable, str(REPO / "scripts/check_graph_tools.py"), "--build", str(s42_build[0]),
                        "--graph-root", str(graph_root), "--allow-unchecked", "--skip-sweep"],
                       capture_output=True, text=True, timeout=900, check=False, cwd=REPO)
    assert p.returncode == 0, p.stdout[-4000:]
    assert "PLAN: inc-002 606 / 185 / 46 and 329 detailed" in p.stdout
