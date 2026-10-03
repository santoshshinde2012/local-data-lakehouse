"""Metric tools (lakehouse_graph.metrics): Wilson intervals, small-cell suppression and the three tools on tiny."""
from __future__ import annotations

import itertools
import math
import os

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "0")

import pytest

from lakehouse_graph import metrics, oracle, spec, tools
from lakehouse_graph.context import ToolContext


@pytest.fixture(scope="module")
def ctx(tiny_build, graph_root):
    c = ToolContext(tiny_build[0], allow_unchecked=True, graph_root=graph_root, audit=False)
    yield c
    c.close()


def call(ctx, name, **args):
    return tools.call(ctx, name, args)["data"]


# ------------------------------------------------------------------------------------------------ wilson
@pytest.mark.parametrize(("k", "n", "want"), [
    (2, 10, [0.057, 0.51]),        # PLAN 3: Santosh's 2 of 10 neighbours
    (29, 72, [0.297, 0.518]),      # PLAN 3: pro, 3-5 cap hits, first after a cut
    (227, 2292, [0.087, 0.112]),   # PLAN 2.2: first renewal after a cap cut 9.9% [8.8, 11.2]
    (321, 5095, [0.057, 0.07]),    # ... vs 6.3% [5.7, 7.0]
    (0, 10, [0.0, 0.278]),
])
def test_wilson_matches_the_plan(k, n, want):
    assert metrics.wilson(k, n) == want


def test_wilson_bounds_hold_everywhere():
    assert metrics.wilson(0, 0) is None
    for n in (1, 2, 5, 17, 100, 8001):
        for k in sorted({0, 1, n // 3, n // 2, n - 1, n}):
            lo, hi = metrics.wilson(k, n, digits=12)
            p = k / n
            assert 0.0 <= lo <= p + 1e-12 and p - 1e-12 <= hi <= 1.0
    # symmetry: the interval of k failures mirrors the one of k successes
    lo, hi = metrics.wilson(3, 20, digits=12)
    lo2, hi2 = metrics.wilson(17, 20, digits=12)
    assert math.isclose(lo, 1 - hi2) and math.isclose(hi, 1 - lo2)


# ------------------------------------------------------------------------------------------------ suppression
def test_suppress_primary_and_complementary():
    total = [lambda k: "total"]
    assert metrics.suppress({"a": 10, "b": 7, "c": 4}, total) == {"c", "b"}        # c < 5, then the smallest other
    assert metrics.suppress({"a": 10, "b": 3, "c": 4}, total) == {"b", "c"}        # two hidden: nothing to add
    assert metrics.suppress({"a": 10, "b": 7}, total) == set()
    assert metrics.suppress({"a": 4}, total) == {"a"}                              # nothing left to hide with it
    two = {(p, f): n for (p, f), n in zip(itertools.product("xy", (True, False)), (50, 3, 40, 30), strict=True)}
    hidden = metrics.suppress(two, [lambda k: "total", lambda k: k[0], lambda k: k[1]])
    assert ("x", False) in hidden
    for margin in (lambda k: k[0], lambda k: k[1], lambda k: "total"):
        groups = {}
        for key in two:
            groups.setdefault(margin(key), []).append(key in hidden)
        assert all(sum(g) != 1 for g in groups.values() if len(g) > 1)
    assert metrics.suppress(two, [lambda k: "total", lambda k: k[0], lambda k: k[1]]) == hidden   # deterministic


def test_suppress_starts_from_given_hidden_keys():
    total = [lambda k: "total"]
    assert metrics.suppress({"a": 10, "b": 7, "c": 40}, total, hidden={"c"}) == {"c", "b"}   # c forced, then b
    assert metrics.suppress({"a": 10, "b": 7}, total, hidden={"zzz"}) == set()            # unknown keys ignored


def test_recoverable_models_integers_bounds_and_the_listing_rule():
    """metrics.recoverable is exact over integers with the rule's lower bounds (p2a verify-3): x + y = 5 and
    x + z = 7 leave everything free; printing y = 2 fixes x and z; and x + y = 2 with both nulls >= 1 (a 0 would have
    been printed) fixes both, which a rank test misses."""
    v = {"x": 3, "y": 2, "z": 4, "s1": 5, "s2": 7}
    t = metrics.Table(values=v, equations=[{"x": 1, "y": 1, "s1": -1}, {"x": 1, "z": 1, "s2": -1}],
                      always_shown=frozenset({"s1", "s2"}), fixed_shape=False)
    assert metrics.recoverable(t, {"x", "y", "z"}) == set()
    assert metrics.recoverable(t, {"x", "z"}) == {"x", "z"}
    pair = metrics.Table(values={"a": 1, "b": 1, "s": 2}, equations=[{"a": 1, "b": 1, "s": -1}],
                         always_shown=frozenset({"s"}))
    assert metrics.recoverable(pair, {"a", "b"}) == {"a", "b"}            # bounds, not rank: 1 + 1 = 2
    assert metrics.recoverable(pair, {"a", "b"}, withheld=True) == set()  # withheld: each null may be 0
    assert metrics.recoverable(pair, set()) == set()
    got = metrics.protect(pair)
    assert got.withheld and metrics.recoverable(pair, got.hidden, got.withheld) == set()


@pytest.mark.parametrize(("hits", "band"), [(0, "0"), (1, "1-2"), (2, "1-2"), (3, "3-5"), (5, "3-5"), (6, "6-9"),
                                            (9, "6-9"), (10, "10+"), (60, "10+")])
def test_limit_hit_bands(hits, band):
    assert metrics.limit_hits_band(hits) == band


# ------------------------------------------------------------------------------------------------ tools on tiny
def test_lapse_rate_by_plan_is_the_oracle_with_small_plans_suppressed(ctx, tiny_build):
    t = oracle.load_tables(tiny_build[0])
    d = call(ctx, "metric_lapse_rate", group_by=["plan_tier"])
    want = oracle.routes(t)["model_lapses_by_plan"]
    assert (want["pro"]["n"], want["pro"]["lapses"], want["pro_plus"]["lapses"], want["ultra"]["n"]) == (89, 9, 0, 4)
    cells = {c["plan_tier"]: c for c in d["cells"]}
    assert list(cells) == ["pro", "pro_plus", "ultra"]
    assert cells["ultra"]["suppressed"] and cells["ultra"]["n"] is None               # n = 4
    assert cells["pro_plus"]["suppressed"]                                            # the total would give it back
    # pro 89 / 9 is null too: printed, the total's 9 lapses would leave pro_plus + ultra with 0, pinning both (all
    # 4 ultra renewals renewed: an attribute disclosure the exact check refuses)
    assert cells["pro"]["suppressed"]
    assert d["total"]["n"] == 108 and d["total"]["lapses"] == 9 and d["total"]["wilson_95"] == metrics.wilson(9, 108)
    assert d["population"].startswith("model-routed renewals")


def test_lapse_rate_filters_and_groups(ctx):
    d = call(ctx, "metric_lapse_rate", group_by=["first_renewal_after_pricing_change"])
    # 75 / 6 and 33 / 3 are null together: the first-after boxes share one publication with graph_exposure's pricing
    # tables (33 = the model cells of the pricing changes), and the exact check cannot prove them safe to print
    # within its budget, so they stay null (undecided is the safe side). The total is printed.
    assert {c["first_renewal_after_pricing_change"]: (c["n"], c["lapses"]) for c in d["cells"]} == \
        {False: (None, None), True: (None, None)}
    assert all(c["suppressed"] for c in d["cells"]) and (d["total"]["n"], d["total"]["lapses"]) == (108, 9)
    # a cap-hit filter selects one band at a time (min 1 alone would span several bands: a ToolArgumentError)
    d = call(ctx, "metric_lapse_rate", plan_tier="pro", limit_hits_14d_min=1, limit_hits_14d_max=2)
    assert d["filters"] == {"plan_tier": "pro", "limit_hits_14d_min": 1, "limit_hits_14d_max": 2}
    assert len(d["cells"]) == 1                                        # one filtered cell, no group_by
    c = d["cells"][0]
    assert c["n"] is None or c["n"] == 0 or c["n"] >= 5               # a 1-4 count is never printed
    d = call(ctx, "metric_lapse_rate", group_by=["plan_tier", "limit_hits_14d_band"])
    assert d["bands"] == ["0", "1-2", "3-5", "6-9", "10+"]
    pro = [c["limit_hits_14d_band"] for c in d["cells"] if c["plan_tier"] == "pro"]
    assert pro == d["bands"]                                           # every band, in band order, 0 included
    assert len(d["cells"]) == 3 * 5                                    # the fixed shape: plans x bands
    assert all(c["n"] == 0 or c["n"] >= 5 for c in d["cells"] if not c["suppressed"])
    # no ultra renewal hit the cap 10+ times on tiny: that 0 sits in a null line, so it stays null, and the "no
    # renewal matches" caveat (said only of a printed 0) must not give it back
    empty = tools.call(ctx, "metric_lapse_rate", {"plan_tier": "ultra", "limit_hits_14d_min": 10})
    assert empty["data"]["total"]["n"] is None and empty["data"]["total"]["suppressed"]
    assert not any("No model-routed renewal" in c for c in empty["caveats"])


def test_route_counts_suppress_small_cells(ctx):
    d = call(ctx, "metric_route_counts")
    by = {(c["route"], c["outcome"]): c for c in d["counts"]}
    assert [k for k in by] == list(metrics.ROUTE_OUTCOMES)            # the fixed shape, 0 included
    assert by[("model", "renewed")]["renewals"] == 99 and by[("model", "voluntary_lapse")]["renewals"] == 9
    # cancel_flow 4 is null, and dunning 8 with it: the Renewal count (121, printed by graph_describe) would give
    # the 4 back by subtraction (p2a verify-3: the old answer left score_today = 1 recoverable that way)
    assert by[("cancel_flow", "voluntary_lapse")]["suppressed"] and by[("dunning", "involuntary_lapse")]["suppressed"]
    # the current renewal is public by design: graph_find serves it with its route
    assert by[("score_today", "pending")]["renewals"] == 1 and by[("pending", "pending")]["renewals"] == 0
    assert d["total_renewals"] == 121 == call(ctx, "graph_describe")["nodes"]["Renewal"]
    for plan in ("pro", "pro_plus", "ultra"):
        rows = call(ctx, "metric_route_counts", plan_tier=plan)["counts"]
        assert all(r["renewals"] == 0 or r["renewals"] >= 5 for r in rows
                   if not r["suppressed"] and r["route"] not in metrics.PUBLIC_ROUTES), plan
    assert set(d["route_meaning"]) == {"cancel_flow", "dunning", "model", "score_today"}


def test_metric_answers_resist_the_exact_integer_attack(ctx, tiny_build):
    """scripts/check_graph_tools.py section 6 on tiny: route counts (all plans and every plan, against the Renewal
    count) and six lapse-rate groupings (with their one-key answers): no null is computable."""
    import importlib.util

    from conftest import REPO

    mspec = importlib.util.spec_from_file_location("check_graph_tools_metrics", REPO / "scripts/check_graph_tools.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    t = oracle.load_tables(tiny_build[0])
    rep, calls = mod.Report(), mod.Calls(ctx)
    mod.section_small_cells(rep, calls, t, ctx)
    assert rep.errors == [], rep.errors


def test_feature_cards_follow_the_spec(ctx):
    for f in spec.GOLD_FEATURES:
        d = call(ctx, "metric_feature_card", feature=f)
        card = spec.FEATURE_CARDS[f]
        assert {k: d[k] for k in ("definition", "window", "source", "pit_status", "verification")} == \
            {k: card[k] for k in ("definition", "window", "source", "pit_status", "verification")}
    d = call(ctx, "metric_feature_card", feature="limit_hits_14d")
    assert d["window"] == "(as_of-14, as_of]" and d["pit_status"] == "compliant" and d["backing_edge"] == "HIT_LIMIT"
    assert d["verification"] == "graph-verified"
    env = tools.call(ctx, "metric_feature_card", {"feature": "renewals_completed"})
    assert env["data"]["pit_status"] == "declared_exception" and any("lineage_pit" in c for c in env["caveats"])
