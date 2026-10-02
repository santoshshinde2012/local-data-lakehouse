"""Small-cell protection of the exposure and metric tables, attacked the way an adversary would (p2a verify-3).

Two independent implementations meet here:
  * lakehouse_graph.metrics.protect(): primary + complementary suppression, then an exact integer check
    (bounds propagation + complete branching) that every null keeps at least two possible values;
  * the adversary of scripts/check_graph_tools.py (attack_*): built only from the PRINTED answer and the documented
    publication rule, solved with an exact rational simplex + branch and bound. A null it can compute is a leak.

The verifier's three counterexamples (tiny inc-002, cap-cut-2026-09, the synthetic 52/50/1/1 row) are regression
cases: their old answers are pinned by the adversary (the attack is live), the new ones are not. Seeded random
tables (many zeros and small counts, the s42 / tiny / inject shapes) are attacked the same way.
"""
from __future__ import annotations

import importlib.util
import random

import pytest
from conftest import REPO

from lakehouse_graph import metrics, tools

PLANS = ("pro", "pro_plus", "ultra")


@pytest.fixture(scope="module")
def adv():
    mspec = importlib.util.spec_from_file_location("check_graph_tools_adversary", REPO / "scripts/check_graph_tools.py")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    return mod


def _raw(plan: str, model: int, lapses: int, cancel_flow: int, dunning: int, current: int = 0) -> dict:
    return {"plan_tier": plan, "exposed": model + cancel_flow + dunning + current, "model": model,
            "voluntary_lapses": lapses, "cancel_flow": cancel_flow, "dunning": dunning}


def _truth(raw: list[dict]) -> dict:
    return {r["plan_tier"]: {"exposed": r["exposed"], "model": r["model"], "voluntary_lapses": r["voluntary_lapses"],
                             "cancel_flow": r["cancel_flow"], "dunning": r["dunning"],
                             "current": r["exposed"] - r["model"] - r["cancel_flow"] - r["dunning"]} for r in raw}


def _incident_answer(raw: list[dict], name: str = "inc-x") -> dict:
    rows, by_route, withheld = tools.incident_table(raw)
    by = dict(by_route)
    total = by.pop("exposed")
    return {"entity": {"id": name, "kind": "incident"}, "total": total, "breakdown_withheld": withheld,
            "by_route": by, "cells": rows}


def _pc(plan: str, route: str, known: bool, n: int, lapses: int = 0) -> dict:
    return {"plan_tier": plan, "route": route, "known_by_as_of": known, "renewals": n, "voluntary_lapses": lapses}


def _pricing_answer(raw: list[dict], name: str = "cap-cut-x") -> tuple[dict, dict]:
    rows, split, total, withheld = tools.pricing_table(raw)
    d = {"entity": {"id": name, "kind": "pricing_change"}, "total": total, "breakdown_withheld": withheld,
         "known_by_as_of": {"true": split[True], "false": split[False]}, "cells": rows}
    truth: dict = {}
    for r in raw:
        route = "current" if r["route"] in ("score_today", "pending") else r["route"]
        n, lap = truth.get((r["plan_tier"], r["known_by_as_of"], route), (0, 0))
        truth[(r["plan_tier"], r["known_by_as_of"], route)] = (n + r["renewals"],
                                                              lap + (r["voluntary_lapses"] if route == "model" else 0))
    return d, truth


# ------------------------------------------------------------------------------------------------ the engine
def test_integer_problem_is_exact_on_small_cases():
    # x + y = 2 with a null cell >= 1 (a 0 would have been printed): both pinned at 1 (verify-3 case 1)
    t = metrics.Table(values={"x": 1, "y": 1, "s": 2}, equations=[{"x": 1, "y": 1, "s": -1}],
                      always_shown=frozenset({"s"}))
    assert t.problem({"x", "y"}).pinned() == ({"x", "y"}, set())
    # the same with a lower bound of 0 (inside a null line): three values each
    p = metrics.IntegerProblem(t.values, t.equations, [], ["x", "y"], {"x": 0, "y": 0}, 2)
    assert p.pinned() == (set(), set()) and p.feasible_range("x") == (0, 2)
    # a 2 x 2 block of nulls inside printed row and column sums keeps a degree of freedom
    v = {"a": 3, "b": 7, "c": 6, "d": 2, "r1": 10, "r2": 8, "c1": 9, "c2": 9}
    eqs = [{"a": 1, "b": 1, "r1": -1}, {"c": 1, "d": 1, "r2": -1}, {"a": 1, "c": 1, "c1": -1},
           {"b": 1, "d": 1, "c2": -1}]
    p = metrics.IntegerProblem(v, eqs, [], ["a", "b", "c", "d"], dict.fromkeys("abcd", 1), 20)
    assert p.pinned() == (set(), set()) and p.feasible_range("a") == (2, 8) and p.feasible_range("d") == (1, 7)
    # ... but a lapses count of 0 inside a null pair is pinned by non-negativity (sum 0)
    v2 = {"l1": 0, "l2": 0, "m1": 4, "m2": 3, "L": 0}
    p = metrics.IntegerProblem(v2, [{"l1": 1, "l2": 1, "L": -1}], [{"l1": 1, "m1": -1}, {"l2": 1, "m2": -1}],
                               ["l1", "l2", "m1", "m2"], {"l1": 0, "l2": 0, "m1": 1, "m2": 1}, 10)
    pinned, undecided = p.pinned()
    assert pinned == {"l1", "l2"} and not undecided
    # the truth must satisfy its own lower bounds: a wrong rule is an error, never a silent pass
    with pytest.raises(ValueError):
        metrics.IntegerProblem({"x": 0, "s": 0}, [{"x": 1, "s": -1}], [], ["x"], {"x": 1}, 5).pinned()


def test_search_budget_is_conservative():
    v = {f"x{i}": 50 for i in range(8)}
    v["s"] = 400
    t = metrics.Table(values=v, equations=[{**{f"x{i}": 1 for i in range(8)}, "s": -1}], always_shown=frozenset({"s"}))
    pinned, undecided = t.problem({f"x{i}" for i in range(8)}).pinned(budget=1)
    assert undecided and not pinned          # out of budget: undecided, which protect() treats as pinned
    assert metrics.recoverable(t, {f"x{i}" for i in range(8)}, budget=1) == undecided


def test_protect_prints_what_is_already_fixed_and_withholds_what_cannot_be_protected():
    # a zero line: its nulls are fixed at 0 by the printed 0 margin, so they are printed
    rows, by_route, withheld = tools.incident_table([_raw("pro", 40, 3, 2, 9)])
    assert not withheld and [r["plan_tier"] for r in rows] == list(PLANS)
    ultra = rows[2]
    assert all(ultra[k] == 0 for k in tools.INCIDENT_COUNTS)
    # one renewal in all: nothing printed can protect it, so only the total is shown
    rows, by_route, withheld = tools.incident_table([_raw("pro", 0, 0, 0, 0, current=1)])
    assert withheld and by_route["exposed"] == 1
    assert all(r[k] is None for r in rows for k in tools.INCIDENT_COUNTS)


# ------------------------------------------------------------------------------------------------ regressions
def test_verifier_case_1_tiny_inc_002(adv):
    """Old answer: pro 12 printed, pro_plus / ultra null, total 14: each null row pinned at 1. New answer: safe."""
    raw = [_raw("pro", 12, 1, 0, 0), _raw("pro_plus", 1, 0, 0, 0), _raw("ultra", 1, 0, 0, 0)]
    old = {"entity": {"id": "inc-002", "kind": "incident"}, "total": 14, "breakdown_withheld": False,
           "by_route": {"suppressed": False, "model": 14, "voluntary_lapses": 1, "cancel_flow": 0, "dunning": 0,
                        "current": 0},
           "cells": [{"plan_tier": "pro", "suppressed": False, "exposed": 12, "model": 12, "voluntary_lapses": 1,
                      "cancel_flow": 0, "dunning": 0, "current": 0},
                     *({"plan_tier": p, "suppressed": True, **dict.fromkeys(tools.INCIDENT_COUNTS)}
                       for p in ("pro_plus", "ultra"))]}
    pins = adv.attack_pins(adv.attack_incident(old, _truth(raw)))
    assert ("plan", "pro_plus") in [k for k, _ in pins] and ("plan", "ultra") in [k for k, _ in pins]
    new = _incident_answer(raw, "inc-002")
    a = adv.attack_incident(new, _truth(raw))
    assert adv.attack_pins(a) == [] and a.printed_small == []
    assert new["cells"][0]["exposed"] is None          # pro's 12 is the complement now


def test_verifier_case_2_cap_cut_2026_09(adv):
    """Old answer: one listed cell (pro / score_today / known) with null renewals and the split 1 / 0: the key names
    Santosh's plan and route, and the cell is pinned at 1. New answer: the breakdown is withheld, all six rows null."""
    raw = [_pc("pro", "score_today", True, 1)]
    old = {"entity": {"id": "cap-cut-2026-09", "kind": "pricing_change"}, "total": 1, "breakdown_withheld": False,
           "known_by_as_of": {"true": 1, "false": 0},
           "cells": [{"plan_tier": p, "known_by_as_of": k, "suppressed": (p, k) == ("pro", True),
                      **{c: (None if (p, k, c) == ("pro", True, "current") else 0) for c in adv.INCIDENT_ROUTES},
                      "voluntary_lapses": 0} for p in PLANS for k in (True, False)]}
    _, truth = _pricing_answer(raw, "cap-cut-2026-09")
    assert [k for k, _ in adv.attack_pins(adv.attack_pricing(old, truth))] == [("cell", "pro", True, "current")]
    new, truth = _pricing_answer(raw, "cap-cut-2026-09")
    assert new["breakdown_withheld"] and new["known_by_as_of"] == {"true": None, "false": None}
    assert all(r[c] is None for r in new["cells"] for c in tools.ROUTE_COUNTS)
    assert adv.attack_pins(adv.attack_pricing(new, truth)) == []


def test_verifier_case_3_synthetic_row(adv):
    """Old answer: pro 52 = model 50 + cancel_flow 1 + dunning 1, current 0 printed: the two nulls sum to 2 and are
    each pinned at 1. New answer: safe (and every plan is listed)."""
    raw = [_raw("pro", 50, 4, 1, 1)]
    old = {"entity": {"id": "inc-x", "kind": "incident"}, "total": 52, "breakdown_withheld": False,
           "by_route": {"suppressed": True, "model": 50, "voluntary_lapses": 4, "cancel_flow": None, "dunning": None,
                        "current": 0},
           "cells": [{"plan_tier": "pro", "suppressed": True, "exposed": 52, "model": 50, "voluntary_lapses": 4,
                      "cancel_flow": None, "dunning": None, "current": 0},
                     *({"plan_tier": p, "suppressed": False, **dict.fromkeys(tools.INCIDENT_COUNTS, 0)}
                       for p in ("pro_plus", "ultra"))]}
    pins = {k for k, _ in adv.attack_pins(adv.attack_incident(old, _truth(raw)))}
    assert {("cell", "pro", "cancel_flow"), ("cell", "pro", "dunning")} <= pins
    new = _incident_answer(raw)
    assert adv.attack_pins(adv.attack_incident(new, _truth(raw))) == []


def test_primary_suppression_alone_is_caught_by_the_adversary(adv, monkeypatch):
    """The attack is live: with complements and the exact check switched off, the s42-shaped inc-002 leaks."""
    raw = [_raw("pro", 545, 57, 33, 28), _raw("pro_plus", 162, 10, 11, 12), _raw("ultra", 44, 5, 0, 2)]

    def primary_only(table, budget=metrics.SEARCH_BUDGET, max_rounds=200):
        hidden = {k for k in table.primary()}
        hidden |= {num for model, num in table.numerator_of.items() if model in hidden}
        return metrics.Protected(hidden, False, 0, set(), set())

    monkeypatch.setattr(tools.metrics, "protect", primary_only)
    d = _incident_answer(raw)
    assert d["cells"][2]["dunning"] is None and d["cells"][2]["model"] == 44
    assert ("cell", "ultra", "dunning") in [k for k, _ in adv.attack_pins(adv.attack_incident(d, _truth(raw)))]


# ------------------------------------------------------------------------------------------------ PLAN numbers
def test_incident_table_on_the_s42_numbers():
    """PLAN 3 inc-002 (pro 606/545/57/33/28, pro_plus 185/162/10/11/12, ultra 46/44/5/0/2): ultra dunning 2 is null
    with its complements (ultra model, pro_plus dunning and model); Q20's 751 / 72 stay printed."""
    rows, by_route, withheld = tools.incident_table([_raw("pro", 545, 57, 33, 28), _raw("pro_plus", 162, 10, 11, 12),
                                                     _raw("ultra", 44, 5, 0, 2)])
    shown = {r["plan_tier"]: tuple(r[k] for k in tools.INCIDENT_COUNTS) for r in rows}
    assert not withheld and shown == {"pro": (606, 545, 57, 33, 28, 0), "pro_plus": (185, None, None, 11, None, 0),
                                      "ultra": (46, None, None, 0, None, 0)}
    assert by_route == {"suppressed": False, "exposed": 837, "model": 751, "voluntary_lapses": 72, "cancel_flow": 44,
                        "dunning": 42, "current": 0}


def test_pricing_table_on_the_s42_cap_cut_2026_08(adv):
    raw = [_pc("pro", "cancel_flow", False, 21, 21), _pc("pro", "dunning", False, 9),
           _pc("pro", "model", False, 356, 41), _pc("pro", "cancel_flow", True, 76, 76),
           _pc("pro", "dunning", True, 65), _pc("pro", "model", True, 1443, 148),
           _pc("pro_plus", "cancel_flow", False, 2, 2), _pc("pro_plus", "dunning", False, 4),
           _pc("pro_plus", "model", False, 83, 8), _pc("pro_plus", "cancel_flow", True, 11, 11),
           _pc("pro_plus", "dunning", True, 15), _pc("pro_plus", "model", True, 304, 22),
           _pc("ultra", "cancel_flow", False, 1, 1), _pc("ultra", "dunning", False, 3),
           _pc("ultra", "model", False, 16, 1),
           _pc("ultra", "dunning", True, 3), _pc("ultra", "model", True, 90, 7)]
    d, truth = _pricing_answer(raw, "cap-cut-2026-08")
    assert d["total"] == 2502 and d["known_by_as_of"] == {"true": 2007, "false": 495} and not d["breakdown_withheld"]
    pro = {r["known_by_as_of"]: r for r in d["cells"] if r["plan_tier"] == "pro"}
    assert (pro[True]["model"], pro[True]["voluntary_lapses"], pro[False]["model"]) == (1443, 148, 356)
    a = adv.attack_pricing(d, truth)
    assert adv.attack_pins(a) == [] and a.printed_small == []


# ------------------------------------------------------------------------------------------------ property tests
def _random_incident(rnd: random.Random) -> list[dict]:
    raw = []
    for plan in rnd.sample(PLANS, rnd.randint(1, 3)):
        hi = rnd.choice([1, 2, 4, 6, 9, 15, 60, 900])
        m, cf, d, cur = (rnd.randint(0, hi) if rnd.random() < 0.6 else 0 for _ in range(4))
        m = m or (0 if cf + d + cur else 1)
        raw.append(_raw(plan, m, rnd.randint(0, m), cf, d, cur))
    return raw


@pytest.mark.parametrize("seed", range(6))
def test_random_incident_tables_resist_the_adversary(adv, seed):
    """Seeded random tables (1-3 plans exposed, many zeros and counts of 1-4): no printed count of 1-4 and the
    adversary pins no null; metrics' own exact check agrees."""
    rnd = random.Random(20261001 + seed)
    for _ in range(40):
        raw = _random_incident(rnd)
        d = _incident_answer(raw)
        a = adv.attack_incident(d, _truth(raw))
        assert a.printed_small == [] and adv.attack_pins(a) == [], raw
        table = tools.incident_problem(raw)
        got = metrics.protect(table)
        assert not metrics.recoverable(table, got.hidden, got.withheld), raw


def test_many_random_incident_tables_pass_the_engine_check():
    """800 more tables through metrics.protect + its exact check (fast path): no null is ever computable."""
    rnd = random.Random(1001)
    withheld = 0
    for _ in range(800):
        raw = _random_incident(rnd)
        table = tools.incident_problem(raw)
        got = metrics.protect(table)
        assert not metrics.recoverable(table, got.hidden, got.withheld), raw
        # the only sensitive entries the public numbers fix on their own: a plan row made of current (public by
        # design) renewals only. Never a cell, a route or a lapses count.
        rows = {r["plan_tier"]: r for r in raw}
        assert all(k[0] == "plan" and all(rows[k[1]][c] == 0 for c in ("model", "cancel_flow", "dunning"))
                   for k in got.external), raw
        withheld += bool(got.withheld)   # protect() returns the set of withheld groups ({""}: this table)
    assert withheld < 400           # most random tables keep a breakdown


@pytest.mark.parametrize("seed", range(4))
def test_random_pricing_tables_resist_the_adversary(adv, seed):
    rnd = random.Random(20261002 + seed)
    routes = ("model", "cancel_flow", "dunning", "score_today", "pending")
    for _ in range(40):
        raw = [_pc(p, r, k, n, rnd.randint(0, n) if r == "model" else 0)
               for p in PLANS for r in routes for k in (True, False)
               for n in [rnd.choice([1, 2, 3, 4, 5, 6, 9, 15, 80, 700])] if rnd.random() < 0.3] \
            or [_pc("pro", "model", True, 7, 1)]
        d, truth = _pricing_answer(raw)
        assert d["total"] == sum(r["renewals"] for r in raw)
        a = adv.attack_pricing(d, truth)
        assert a.printed_small == [] and adv.attack_pins(a) == [], raw
