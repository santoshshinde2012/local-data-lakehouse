"""The three metric tools (toolset ``lakehouse-metrics``) and the build's protected population publication.

  metric_lapse_rate(ctx, group_by=[], plan_tier=None, first_renewal_after_pricing_change=None,
                    limit_hits_14d_min=None, limit_hits_14d_max=None)
      voluntary-lapse rate over the MODEL-routed renewals (the labelled population: renewed or voluntary_lapse, as
      of the build's data_end) with n, lapses, rate and a Wilson 95% interval for the total and every cell
  metric_route_counts(ctx, plan_tier=None)
      renewals per route and outcome (model / cancel_flow / dunning / score_today / pending), one plan or all
  metric_feature_card(ctx, feature)
      what one of the 22 gold features means: definition, window, source column, backing graph edge, point-in-time
      status, contract range, whether SIMILAR_TO uses it, how it is verified

Each returns the envelope (lakehouse_graph.envelope). No Ladybug, no network: pandas over the build's Parquet (the
metrics server runs under the same sandbox profile).

What metric_lapse_rate can select (a decomposable family; anything else is refused with a repair hint):
  * plan_tier, first_renewal_after_pricing_change and a limit_hits_14d range combine freely, as filters or (with
    limit_hits_14d_band) grouping keys. A range follows the bands 0, 1-2, 3-5, 6-9, 10+: limit_hits_14d_min is a band
    start (0, 1, 3, 6, 10) and limit_hits_14d_max a band end (0, 2, 5, 9), so two answers never differ by a few
    renewals of one hit count;
  * incident_exposed_28d and overage_toggled_off are grouped alone or together, over all model renewals (no filter,
    no other key). The two groups of dimensions share only the population total, so every answer is a box of one of
    two small cubes (plan x first-after x band: 30 cells; incident x overage: 4) and their joint check below is exact.
  A filter is a slice: metric_lapse_rate(plan_tier='ultra') prints what group_by=['plan_tier'] prints for ultra.

What is public by design (row level, PLAN 8.2): graph_find lists any renewal with its route and graph_renewal_evidence
shows its plan, its own evidence and (detailed) its features at as_of, so counts can always be made one renewal at a
time; neighbour outcomes follow the visibility rule. The current renewals (routes score_today and pending) are cheap
to list (graph_find with data_end + 7 days, a renewal date) and named_renewal_member gives their exposure, so the
publication treats their counts, everywhere, as known numbers: printed, never a protection.

Small cells (MIN_CELL = 5, PLAN 8.2, queries.py rule 7). Everything the aggregate tools print about the population
is ONE publication per build (``publication(ctx)``, computed once per process): every metric_lapse_rate answer of the
family, metric_route_counts for all plans and each plan, graph_describe's Renewal count, and the graph_exposure tables
of every incident and pricing change (tools.py reads their numbers from here). A count of 1-4 renewals is null, and
``protect()`` adds complements (or withholds a table's breakdown) until an exact integer check proves that every
null keeps at least two possible values given EVERY number those answers print together, the current renewals and
what the attacker knows of the structure: counts are non-negative integers, a lapses count is at most its n, the
publication rule's lower bounds (a null outside a null line is at least 1: a 0 there would have been printed), and
how the tables relate (exposed renewals of a plan and route are at most the population's; a first renewal after a
pricing change has one FIRST_RENEWAL_AFTER edge, so the pricing tables' model cells add up to the first-after cells of
the lapse rates; the incident flag is the union of the incidents, between the largest and the sum, per plan). The
check is complete: a null is free only with a witness (a second integer population that agrees with every printed
number and differs in that null), pinned only when an exhaustive search finds none, and undecided (out of budget)
counts as pinned. A breakdown of a global event under 5 renewals is withheld (PLAN Q6: cap-cut-2026-09).

What this does not cover: counting renewals one by one (row-level facts above); the cohort tools (cohorts.py has its
own rule and its own documented limits); the evidence graph of graph_cypher (label-free, outside this publication).
"""
from __future__ import annotations

import datetime as dt
import itertools
import math
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass, field, replace
from fractions import Fraction

import numpy as np
import pandas as pd

from . import spec
from .context import ToolArgumentError

MIN_CELL = 5
WILSON_Z = 1.959963984540054
POPULATION_ROUTE = spec.REFERENCE_ROUTE    # "model": the only route with a voluntary-lapse label
GROUP_KEYS = ("plan_tier", "first_renewal_after_pricing_change", "incident_exposed_28d", "overage_toggled_off",
              "limit_hits_14d_band")
FLAG_KEYS = ("first_renewal_after_pricing_change", "incident_exposed_28d", "overage_toggled_off")
LIMIT_HIT_BANDS = (("0", 0, 0), ("1-2", 1, 2), ("3-5", 3, 5), ("6-9", 6, 9), ("10+", 10, None))
BANDS = tuple(label for label, _, _ in LIMIT_HIT_BANDS)
BAND_STARTS = tuple(lo for _, lo, _ in LIMIT_HIT_BANDS)                       # limit_hits_14d_min values
BAND_ENDS = tuple(hi for _, _, hi in LIMIT_HIT_BANDS if hi is not None)       # limit_hits_14d_max values
CUBE_KEYS = ("plan_tier", "first_renewal_after_pricing_change", "limit_hits_14d_band")   # filters + keys
FLAG_CUBE_KEYS = ("incident_exposed_28d", "overage_toggled_off")                         # keys, unfiltered
ROUTE_MEANING = {
    "model": "scored by the radar model at T-7; outcome renewed or voluntary_lapse (the labelled population)",
    "cancel_flow": "cancellation scheduled on or before T-7 (already decided; outcome voluntary_lapse)",
    "dunning": "renewal payment failed (outcome involuntary_lapse)",
    "score_today": "current renewal: its T-7 is the build's data_end, outcome pending",
    "pending": "renewal after the data end, outcome pending",
}
POPULATION = ("model-routed renewals (route = model): voluntary_lapse vs renewed, outcomes as of the build's "
              "data_end")
CURRENT_ROUTES = ("score_today", "pending")
# Every (route, outcome) pair the renewal model produces, in the order route counts list them (a fixed shape:
# all six rows always, 0 included). score_today / pending are the current renewals: public by design.
ROUTE_OUTCOMES = (("model", "renewed"), ("model", "voluntary_lapse"), ("cancel_flow", "voluntary_lapse"),
                  ("dunning", "involuntary_lapse"), ("score_today", "pending"), ("pending", "pending"))
PUBLIC_ROUTES = CURRENT_ROUTES
EXPOSURE_ROUTES = ("model", "cancel_flow", "dunning", "current")   # a partition of an exposed set; current = public
ALL = "*"


def wilson(k: int, n: int, digits: int = 3) -> list[float] | None:
    """Wilson score 95% interval for k successes in n trials; None when n is 0."""
    if n <= 0:
        return None
    p = k / n
    den = 1 + WILSON_Z * WILSON_Z / n
    centre = (p + WILSON_Z * WILSON_Z / (2 * n)) / den
    half = WILSON_Z * math.sqrt(p * (1 - p) / n + WILSON_Z * WILSON_Z / (4 * n * n)) / den
    return [round(max(0.0, centre - half), digits), round(min(1.0, centre + half), digits)]


def rate_cell(n: int, lapses: int, hidden: bool) -> dict:
    """n / lapses / rate / wilson_95 / suppressed for one published cell."""
    if hidden:
        return {"n": None, "lapses": None, "rate": None, "wilson_95": None, "suppressed": True}
    return {"n": int(n), "lapses": int(lapses), "rate": round(lapses / n, 4) if n else None,
            "wilson_95": wilson(int(lapses), int(n)), "suppressed": False}


def suppress(counts: dict[Hashable, int], margins: list[Callable[[Hashable], Hashable]],
             min_cell: int = MIN_CELL, hidden: Iterable[Hashable] = ()) -> set:
    """Keys to suppress: primary (count < min_cell) plus classic complementary over the published margins (a
    heuristic kept for callers that need no proof; the tools use protect())."""
    hidden = {k for k, n in counts.items() if n < min_cell} | (set(hidden) & set(counts))
    changed = True
    while changed:
        changed = False
        for margin in margins:
            groups: dict[Hashable, list] = defaultdict(list)
            for k in counts:
                groups[margin(k)].append(k)
            for g in sorted(groups, key=repr):
                ks = groups[g]
                inside = [k for k in ks if k in hidden]
                shown = sorted((counts[k], repr(k), k) for k in ks if k not in hidden)
                if len(inside) == 1 and shown:
                    hidden.add(shown[0][2])
                    changed = True
    return hidden


# --------------------------------------------------------------------------- exact disclosure control: the engine
# The attacker's integer program. Unknowns are the null BASE entries (cells; and auxiliary unknowns the attacker cannot
# see, such as how many model renewals of a plan the incident flag covers); a DERIVED entry (a sum: a margin, a box of
# the lapse-rate cubes, a table total) is a linear form over base entries, an equation when printed and a target with
# a lower bound when null. Rows are ranged, lo <= sum(c * x) <= hi, over the unknowns. A target is FREE only with a
# witness (an integer assignment that satisfies every row and differs from the truth in it) and PINNED only when an
# exhaustive search (bounds propagation to a fixpoint + complete branching) finds none.
SEARCH_BUDGET = 20_000   # search nodes per question; over budget the answer is "undecided", treated as pinned
PROTECT_BUDGET = 400     # the same inside protect(): an undecided null gets a complement (sensitive) or stays null
# Witnesses are searched within this distance of the truth in every unknown: a second table found there is a proof of
# freedom like any other; finding none there is NOT a proof, and the caller treats that null as pinned (more nulls or
# a printed non-sensitive count, never a computable null). It keeps every search small on any build size.
WITNESS_WINDOW = 3


class Undecided(RuntimeError):
    """The exact search ran out of its node budget (the caller treats the null as pinned: safe side)."""


def _ceil_div(a: int, b: int) -> int:
    return -((-a) // b)


def _propagate(rows: list, var_rows: list, lo: list[int], hi: list[int], pending: set) -> bool:
    """Bounds propagation to the fixpoint over ranged rows (lo <= sum(c * x) <= hi, None: unbounded), visiting only the
    rows whose variables changed. False: no integer point is left."""
    while pending:
        r = pending.pop()
        idx, coef, rlo, rhi = rows[r]
        mn = mx = 0
        for i, c in zip(idx, coef):
            if c > 0:
                mn += c * lo[i]
                mx += c * hi[i]
            else:
                mn += c * hi[i]
                mx += c * lo[i]
        gap_hi = None if rhi is None else rhi - mn          # room above the row's minimum
        gap_lo = None if rlo is None else mx - rlo          # room below the row's maximum
        if (gap_hi is not None and gap_hi < 0) or (gap_lo is not None and gap_lo < 0):
            return False
        room = gap_hi if gap_lo is None else gap_lo if gap_hi is None else min(gap_hi, gap_lo)
        if room is None:
            continue
        for i, c in zip(idx, coef):
            span = (hi[i] - lo[i]) * (c if c > 0 else -c)
            if span <= room:                                # this variable cannot be tightened by this row
                continue
            changed = False
            if c > 0:
                if gap_hi is not None and span > gap_hi:    # c * x_i <= c * lo_i + gap_hi
                    new_hi = lo[i] + gap_hi // c
                    if new_hi < hi[i]:
                        hi[i], changed = new_hi, True
                if gap_lo is not None and span > gap_lo:    # c * x_i >= c * hi_i - gap_lo
                    new_lo = hi[i] - gap_lo // c
                    if new_lo > lo[i]:
                        lo[i], changed = new_lo, True
            else:
                d = -c
                if gap_hi is not None and span > gap_hi:    # -d * x_i <= -d * hi_i + gap_hi
                    new_lo = hi[i] - gap_hi // d
                    if new_lo > lo[i]:
                        lo[i], changed = new_lo, True
                if gap_lo is not None and span > gap_lo:
                    new_hi = lo[i] + gap_lo // d
                    if new_hi < hi[i]:
                        hi[i], changed = new_hi, True
            if changed:
                if lo[i] > hi[i]:
                    return False
                pending.update(var_rows[i])
    return True


def _search(rows: list, var_rows: list, lo: list[int], hi: list[int], pending: set, prefer: list[int],
            budget: list[int]) -> list[int] | None:
    """One integer point inside [lo, hi] that satisfies every row (depth first, values nearest ``prefer`` first), or
    None if there is none. Complete: every branch is propagated empty, solved or split, so None proves infeasibility.
    Raises Undecided when ``budget[0]`` search nodes are used up."""
    stack = [(list(lo), list(hi), set(pending))]
    while stack:
        budget[0] -= 1
        if budget[0] < 0:
            raise Undecided("search budget exhausted")
        lo_, hi_, pend = stack.pop()
        if not _propagate(rows, var_rows, lo_, hi_, pend):
            continue
        best, width = -1, None
        for j in range(len(lo_)):
            w = hi_[j] - lo_[j]
            if w > 0 and (width is None or w < width):
                best, width = j, w
        if best < 0:
            return lo_
        i = best
        p = min(max(prefer[i], lo_[i]), hi_[i])
        branches = []
        if p + 1 <= hi_[i]:
            branches.append((p + 1, hi_[i]))
        if lo_[i] <= p - 1:
            branches.append((lo_[i], p - 1))
        branches.append((p, p))            # popped first: the preferred value, then below, then above
        for a, b in branches:
            nlo, nhi = list(lo_), list(hi_)
            nlo[i], nhi[i] = a, b
            stack.append((nlo, nhi, set(var_rows[i])))
    return None


class IntegerProblem:
    """The attacker's integer program for one published table.

    values        every entry's true integer value (the truth is always feasible)
    equations     ``{key: coefficient}`` rows with ``sum(coefficient * value) == 0`` (keys may be derived)
    inequalities  the same with ``<= 0``
    hidden        the null entries: the targets
    lower         per null entry, the lower bound the publication rule lets the attacker assume
    cap           an upper bound for every unknown (above every true value)
    forms         derived key -> {base key: coefficient}; a printed derived key is an equation, a null one a target
    aux           base unknowns that are never printed and never targets (e.g. a union's size)
    """

    def __init__(self, values: dict, equations: list[dict], inequalities: list[dict], hidden: list,
                 lower: dict, cap: int, forms: dict | None = None, aux: Iterable = (), window: int | None = None):
        self.values = values
        self.window = window
        self.forms = forms or {}
        self.keys = list(hidden)
        hidden_set = set(self.keys)
        aux = [k for k in aux if k not in hidden_set]
        self.base = [k for k in self.keys if k not in self.forms] + list(aux)
        self.pos = {k: i for i, k in enumerate(self.base)}
        self.truth = [int(values[k]) for k in self.base]
        self.lo = [int(lower.get(k, 0)) for k in self.base]
        self.hi = [int(cap)] * len(self.base)
        rows: list = []
        for group, ranged in ((equations, False), (inequalities, True)):
            for row in group:
                coef, const = self._expand(row)
                if coef:
                    rows.append((tuple(coef), tuple(coef.values()), None if ranged else -const, -const))
                elif (const > 0) if ranged else (const != 0):
                    raise ValueError(f"published table is inconsistent: {row}")
        for d, form in self.forms.items():
            coef, const = self._expand(form)
            if d in hidden_set:
                lb = int(lower.get(d, 0))
                if coef and (lb > 0 or any(c < 0 for c in coef.values())):
                    rows.append((tuple(coef), tuple(coef.values()), lb - const, None))
                elif not coef and const < lb:
                    raise ValueError(f"published table is inconsistent with its own lower bounds: {d}")
            else:
                v = int(values[d]) - const
                if coef:
                    rows.append((tuple(coef), tuple(coef.values()), v, v))
                elif v != 0:
                    raise ValueError(f"published table is inconsistent: {d}")
        self.rows = rows
        self.var_rows = [[] for _ in self.base]
        for r, (idx, _, _, _) in enumerate(rows):
            for i in idx:
                self.var_rows[i].append(r)
        if not _propagate(self.rows, self.var_rows, self.lo, self.hi, set(range(len(rows)))):
            raise ValueError("published table is inconsistent with its own lower bounds")
        self._basis: list | None = None

    def _span_basis(self) -> list:
        """An echelon basis of the published EQUATIONS (rows with lo == hi) over the rationals, built once: a list of
        (pivot, {unknown: coefficient}) in insertion order, each row free of every earlier pivot."""
        if self._basis is None:
            basis: list = []
            for idx, coef, rlo, rhi in self.rows:
                if rlo is None or rlo != rhi:
                    continue
                row = {i: Fraction(c) for i, c in zip(idx, coef, strict=True)}
                row = self._reduce(row, basis)
                if row:
                    pivot = min(row)
                    lead = row[pivot]
                    basis.append((pivot, {i: c / lead for i, c in row.items()}))
            self._basis = basis
        return self._basis

    @staticmethod
    def _reduce(row: dict, basis: list) -> dict:
        row = dict(row)
        for pivot, b in basis:            # ascending insertion order: row b holds no earlier pivot
            c = row.get(pivot)
            if c:
                for i, cb in b.items():
                    v = row.get(i, 0) - c * cb
                    if v:
                        row[i] = v
                    else:
                        row.pop(i, None)
        return row

    def span_pinned(self, key) -> bool:
        """Is ``key`` an exact linear combination of the published equations (so every table that agrees with them
        gives it the same value)? Sound and complete for equalities alone; the bounds and inequalities only add pins
        the search finds. It catches the pins that bounds propagation misses (a null equal to a printed total minus
        printed zeros, through two forms) without spending the search budget."""
        coef, _const = self._expand({key: 1})
        if not coef:
            return True
        return not self._reduce({i: Fraction(c) for i, c in coef.items()}, self._span_basis())

    def _expand(self, row: dict) -> tuple[dict[int, int], int]:
        """A row over keys -> (coefficients over the unknowns, constant): printed base keys are constants, derived keys
        expand to their forms."""
        coef: dict[int, int] = {}
        const = 0
        for k, c in row.items():
            parts = self.forms.get(k, {k: 1})
            for b, cb in parts.items():
                if b in self.pos:
                    i = self.pos[b]
                    coef[i] = coef.get(i, 0) + int(c) * int(cb)
                else:
                    const += int(c) * int(cb) * int(self.values[b])
        return {i: c for i, c in coef.items() if c}, const

    def value_of(self, key, point: list[int]) -> int:
        """An entry's value at an assignment of the unknowns."""
        coef, const = self._expand({key: 1})
        return const + sum(c * point[i] for i, c in coef.items())

    def _solve(self, extra: list, lo: list[int], hi: list[int], pending: set, budget: int) -> list[int] | None:
        rows, var_rows = self.rows, self.var_rows
        if extra:
            rows = rows + extra
            var_rows = [list(v) for v in var_rows]
            for r, (idx, _, _, _) in enumerate(extra, start=len(self.rows)):
                for i in idx:
                    var_rows[i].append(r)
                pending.add(r)
        return _search(rows, var_rows, lo, hi, pending, self.truth, [budget])

    def _bounded(self, key, at_least: int | None, at_most: int | None, budget: int,
                 window: int | None = None) -> list[int] | None:
        """An integer point with at_least <= key <= at_most (None: unbounded), or None. ``window``: look only within
        that distance of the truth in every unknown (a point found there is still a point; None there is not a
        proof, which callers treat as pinned: the safe side)."""
        lo, hi = list(self.lo), list(self.hi)
        if window is not None:
            lo = [max(a, t - window) for a, t in zip(lo, self.truth, strict=True)]
            hi = [min(b, t + window) for b, t in zip(hi, self.truth, strict=True)]
        if key in self.pos:
            i = self.pos[key]
            if at_least is not None:
                lo[i] = max(lo[i], at_least)
            if at_most is not None:
                hi[i] = min(hi[i], at_most)
            if lo[i] > hi[i]:
                return None
            seed = set(range(len(self.rows))) if window is not None else set(self.var_rows[i])
            return self._solve([], lo, hi, seed, budget)
        coef, const = self._expand({key: 1})
        if not coef:
            v = const
            ok = (at_least is None or v >= at_least) and (at_most is None or v <= at_most)
            return list(self.truth) if ok else None
        row = (tuple(coef), tuple(coef.values()), None if at_least is None else at_least - const,
               None if at_most is None else at_most - const)
        return self._solve([row], lo, hi, set(range(len(self.rows))) if window is not None else set(), budget)

    def assignment(self, point: list[int]) -> dict:
        """Every null entry's value at a point."""
        return {k: (point[self.pos[k]] if k in self.pos else self.value_of(k, point)) for k in self.keys}

    def witness(self, key, budget: int = SEARCH_BUDGET) -> dict | None:
        """A second integer table that differs from the truth in ``key`` (None: ``key`` is pinned, or within the
        window none was found: callers treat both as pinned)."""
        v = int(self.values[key])
        for at_least, at_most in ((None, v - 1), (v + 1, None)):
            point = self._bounded(key, at_least, at_most, budget, self.window)
            if point is not None:
                return self.assignment(point)
        return None

    def point_witness(self, key, budget: int = SEARCH_BUDGET) -> list[int] | None:
        """As witness(), the raw point (for re-use across protect() rounds)."""
        v = int(self.values[key])
        for at_least, at_most in ((None, v - 1), (v + 1, None)):
            point = self._bounded(key, at_least, at_most, budget, self.window)
            if point is not None:
                return point
        return None

    def satisfies(self, full: dict) -> bool:
        """Does a full assignment (base key -> value, printed keys at the truth) satisfy this problem?"""
        point = []
        for k, lo, hi in zip(self.base, self.lo, self.hi, strict=True):
            v = full.get(k)
            if v is None or not lo <= v <= hi:
                return False
            point.append(v)
        for idx, coef, rlo, rhi in self.rows:
            s = sum(c * point[i] for i, c in zip(idx, coef, strict=True))
            if (rlo is not None and s < rlo) or (rhi is not None and s > rhi):
                return False
        return True

    def pinned(self, budget: int = SEARCH_BUDGET, witnesses: list | None = None,
               only: Iterable | None = None) -> tuple[set, set]:
        """(pinned, undecided) nulls (of ``only``, when given). One witness frees every null it changes, so few
        searches are needed; a list given as ``witnesses`` holds earlier full assignments (they are re-checked, and new
        ones are appended)."""
        free: set = set()
        pinned: set = set()
        undecided: set = set()
        if witnesses is not None:
            for full in witnesses:
                if self._agrees(full) and self.satisfies(full):
                    point = [full[k] for k in self.base]
                    free |= {k for k in self.keys if self._differs(k, point)}
        wanted = None if only is None else set(only)
        for key in self.keys:
            if key in free or (wanted is not None and key not in wanted):
                continue
            lo, hi = self.root_range(key)
            if lo == hi or self.span_pinned(key):   # the fixpoint or the published equations alone fix it
                pinned.add(key)
                continue
            try:
                point = self.point_witness(key, budget)
            except Undecided:
                undecided.add(key)
                continue
            if point is None:
                pinned.add(key)
                continue
            free |= {k for k in self.keys if self._differs(k, point)}
            if witnesses is not None:
                witnesses.append(self._full(point))
        return pinned, undecided

    def root_range(self, key) -> tuple[int, int]:
        """The range of an entry under the root propagation fixpoint (an over-approximation of the feasible range:
        equal ends prove it pinned)."""
        if key in self.pos:
            i = self.pos[key]
            return self.lo[i], self.hi[i]
        coef, const = self._expand({key: 1})
        mn = const + sum(c * (self.lo[i] if c > 0 else self.hi[i]) for i, c in coef.items())
        mx = const + sum(c * (self.hi[i] if c > 0 else self.lo[i]) for i, c in coef.items())
        return mn, mx

    def _differs(self, key, point: list[int]) -> bool:
        if key in self.pos:
            return point[self.pos[key]] != int(self.values[key])
        return self.value_of(key, point) != int(self.values[key])

    def _full(self, point: list[int]) -> dict:
        return {**{k: int(v) for k, v in self.values.items() if k not in self.forms}, **dict(zip(self.base, point,
                                                                                                strict=True))}

    def _agrees(self, full: dict) -> bool:
        """A stored assignment is usable only if every PRINTED base entry still sits at its true value."""
        return all(full.get(k) == int(v) for k, v in self.values.items() if k not in self.forms and k not in self.pos)

    def feasible_range(self, key, budget: int = SEARCH_BUDGET) -> tuple[int, int]:
        """Exact integer [min, max] of ``key`` over every table that agrees with the published numbers."""
        v = int(self.values[key])
        lo_bound = int(self.lo[self.pos[key]]) if key in self.pos else 0
        a, b = lo_bound, v                    # min: smallest m with a solution key <= m (monotone in m)
        while a < b:
            m = (a + b) // 2
            a, b = (a, m) if self._bounded(key, None, m, budget) is not None else (m + 1, b)
        low = a
        a, b = v, max(self.hi, default=v)     # max: largest m with a solution key >= m
        b = max(b, v) * max(1, len(self.forms.get(key, {1: 1})))
        while a < b:
            m = (a + b + 1) // 2
            a, b = (m, b) if self._bounded(key, m, None, budget) is not None else (a, m - 1)
        return low, a


@dataclass
class Table:
    """A publication as the attacker model sees it (the module docstring has the rule).

    values        every entry and auxiliary unknown: true integer values
    equations     extra published relations ``{key: coefficient}`` with ``sum(coefficient * value) == 0``
    inequalities  ``sum(coefficient * value) <= 0`` (a lapses count is at most its n; exposed <= population)
    forms         derived entry -> {base entry: coefficient} (a sum: margins, boxes, totals)
    always_shown  printed whatever happens (the total of a global event; the renewal count of graph_describe)
    public        known to everyone (the current renewals): never null as a protection, never a complement; printed
                  unless its group's breakdown is withheld
    lines         head -> the entries inside it: a null head nulls them all (0 included), and they may then be 0
    numerator_of  n entry -> its lapses entry (null together; a numerator is never a complement on its own)
    margins       heads that stay sensitive when pinned (a printed head would change its line's lower bounds)
    lapses        lapses counts (a rate's numerator, printed with its n even when small): a null one may be 0
    aux           unknowns the attacker never sees printed (e.g. a union's size): never targets
    groups        entry -> its table (a withheld breakdown nulls a whole group but its always-shown total)
    withhold      groups whose breakdown is withheld by rule (a global event under MIN_CELL renewals: PLAN Q6)
    last_resort   complements chosen only when no other entry would do (an all-plans lapses total)
    min_cell      the small-cell threshold (5)

    The publication rule (and so the attacker's lower bounds): a count of 1-4 is null; a null head nulls its line; a
    complement is a printed, non-zero, non-public entry; a 0 is printed everywhere else; a null the printed numbers fix
    anyway is printed, unless it is sensitive or its group is withheld. Hence a null head is >= 1, a null inside a null
    line (or in a withheld group, or a lapses count) is >= 0 and every other null is >= 1.
    """

    values: dict
    equations: list[dict] = field(default_factory=list)
    inequalities: list[dict] = field(default_factory=list)
    forms: dict = field(default_factory=dict)
    always_shown: frozenset = frozenset()
    public: frozenset = frozenset()
    lines: dict = field(default_factory=dict)
    numerator_of: dict = field(default_factory=dict)
    margins: frozenset = frozenset()
    lapses: frozenset = frozenset()
    aux: frozenset = frozenset()
    groups: dict = field(default_factory=dict)
    withhold: frozenset = frozenset()
    last_resort: frozenset = frozenset()
    seed: frozenset = frozenset()
    fixed_shape: bool = True
    min_cell: int = MIN_CELL

    def __post_init__(self) -> None:
        # every published relation as a list of keys (a form: its head and parts), for the complement search
        rel = [list(eq) for eq in self.equations]
        rel += [[d, *form] for d, form in self.forms.items()]
        self._relations = rel
        by_key: dict = defaultdict(list)
        for r, keys in enumerate(rel):
            for k in keys:
                by_key[k].append(r)
        self._by_key = by_key
        self._inside: dict = defaultdict(set)
        for head, keys in self.lines.items():
            for k in keys:
                self._inside[k].add(head)
        self._model_of = {num: model for model, num in self.numerator_of.items()}

    def cap(self) -> int:
        return max([1, *self.values.values()])

    def group_of(self, k):
        """The table an entry belongs to ("" for an ungrouped entry: a stand-alone table is one group)."""
        return self.groups.get(k, "")

    def group_keys(self, groups: Iterable) -> set:
        groups = set(groups)
        return {k for k in self.values if self.group_of(k) in groups and self.hideable(k)}

    def lower(self, hidden: set, withheld: Iterable = ()) -> dict:
        """The lower bound the attacker can assume for each null under the publication rule."""
        withheld = set(withheld)
        out = {}
        for k in hidden:
            if k in self.aux or k in self.lapses or self.group_of(k) in withheld:
                out[k] = 0
            elif not self.fixed_shape:
                out[k] = 1
            else:
                out[k] = 0 if self._inside[k] & hidden else 1
        return out

    def problem(self, hidden: set, withheld: Iterable = (), *, everything_withheld: bool = False,
                window: int | None = WITNESS_WINDOW) -> IntegerProblem:
        order = sorted(hidden, key=repr)
        low = dict.fromkeys(hidden, 0) if everything_withheld else self.lower(set(hidden), withheld)
        return IntegerProblem(self.values, self.equations, self.inequalities, order, low, self.cap(), self.forms,
                              sorted(self.aux, key=repr), window)

    def hideable(self, k) -> bool:
        return k not in self.always_shown and k not in self.public and k not in self.aux

    def close(self, hidden: set) -> set:
        """Ties (a null head nulls the zeros of its line, a null n its numerator), then classic complementary
        suppression: a published relation may not hold exactly one null; the cheapest printed non-zero entry of it is
        nulled with it."""
        hidden = {k for k in hidden if self.hideable(k)}
        changed = True
        while changed:
            changed = False
            for m in sorted(hidden & set(self.lines), key=repr):
                add = {c for c in self.lines[m]
                       if self.hideable(c) and self.values[c] == 0 and c not in self.lapses} - hidden
                if add:
                    hidden |= add
                    changed = True
            for model, num in sorted(self.numerator_of.items(), key=repr):
                if model in hidden and num not in hidden and self.hideable(num):
                    hidden.add(num)
                    changed = True
            for keys in self._relations:
                inside = [k for k in keys if k in hidden]
                if len(inside) != 1 or inside[0] in self.lapses:
                    continue
                cands = self.candidates(keys, hidden)
                if cands:
                    hidden.add(cands[0])
                    changed = True
        return hidden

    def candidates(self, keys, hidden: set) -> list:
        """Printed entries that may be nulled as a complement among ``keys``, cheapest first (smallest value; the
        last_resort ones after all others)."""
        numerator_model = {num: model for model, num in self.numerator_of.items()}
        out = set()
        for k in keys:
            if k in hidden or not self.hideable(k) or self.values[k] <= 0:
                continue
            if k in numerator_model:          # a numerator goes only with its model count
                model = numerator_model[k]
                if model not in hidden and self.hideable(model) and self.values[model] > 0:
                    out.add(model)
            else:
                out.add(k)
        return sorted(out, key=lambda k: (k in self.last_resort, self.values[k], repr(k)))

    def complements(self, bad: list, hidden: set) -> set:
        """Complements for pinned nulls, one per bad null whose relations got none yet this round: the cheapest
        candidate of the SMALLEST relation that holds it (local first). When no bad null has one, the cheapest
        candidate of the smallest relation that holds any null. Empty when nothing printed is left to null."""
        chosen: set = set()
        for b in bad:
            near = self._neighbourhood(b)
            if any(k in chosen for rels in near for r in rels for k in self._relations[r]):
                continue
            for rels in near:                        # the relations of b, then those of their entries (2 hops)
                done = False
                for r in rels:
                    cands = self.candidates(self._relations[r], hidden | chosen)
                    if cands:
                        chosen.add(cands[0])
                        done = True
                        break
                if done:
                    break
        if chosen:
            return chosen
        rels = sorted({r for h in hidden for r in self._by_key.get(h, ())}, key=lambda r: (len(self._relations[r]), r))
        for r in rels:
            cands = self.candidates(self._relations[r], hidden)
            if cands:
                return {cands[0]}
        return set()

    def _neighbourhood(self, b) -> list[list[int]]:
        """[relations holding b (or a part of b's form), relations holding an entry of those], smallest first."""
        def order(rs):
            return sorted(rs, key=lambda r: (len(self._relations[r]), r))

        first = set(self._by_key.get(b, ()))
        for part in self.forms.get(b, {}):
            first |= set(self._by_key.get(part, ()))
        model = self._model_of.get(b)
        if model is not None:
            first |= set(self._by_key.get(model, ()))
        second = {r2 for r in first for k in self._relations[r] for r2 in self._by_key.get(k, ())} - first
        return [order(first), order(second)]

    def primary(self) -> set:
        """Counts of 1-4 (never a numerator: it is printed with its model count, as a rate's numerator is)."""
        numerators = set(self.numerator_of.values()) | set(self.lapses)
        return {k for k, v in self.values.items() if 0 < v < self.min_cell and self.hideable(k) and k not in numerators}

    def sensitive(self, k, hidden: set) -> bool:
        """A pinned null that must not stay computable: a count of 1-4 or a head (not a numerator), or a numerator whose
        model count (or, for a lapses total, whose line) is null, or a margin whose line holds a null 0 that no other
        null head covers (printing the margin would raise that 0's lower bound to 1: a false bound that pins its
        neighbours). A pinned cell of 0 or >= min_cell, or a numerator beside its printed model count, adds nothing
        when printed; protect() prints it."""
        if k in self.aux or not self.hideable(k):
            return False
        if self.uncovered_zeros(k, hidden):
            return True
        model = self._model_of.get(k)
        if model is not None:
            return model in hidden and 0 < self.values[model] < self.min_cell
        if k in self.lapses:
            return any(h in hidden and 0 < self.values[h] < self.min_cell for h in self._inside[k])
        return 0 < self.values[k] < self.min_cell

    def uncovered_zeros(self, k, hidden: set) -> bool:
        """Is ``k`` a margin whose line holds a null 0 (not a lapses count) under no other null head?"""
        if k not in self.margins or k not in self.lines:
            return False
        return any(c in hidden and c not in self.lapses and self.values[c] == 0 and not (self._inside[c] & hidden) - {k}
                   for c in self.lines[k])

    def maybe_sensitive(self, k) -> bool:
        """Could ``k`` be sensitive for some null set (a count of 1-4, a numerator / lapses count of one, or a margin
        over a 0)?"""
        if k in self.aux or not self.hideable(k):
            return False
        if k in self.margins and any(self.values[c] == 0 and c not in self.lapses for c in self.lines.get(k, ())):
            return True
        model = self._model_of.get(k)
        if model is not None:
            return 0 < self.values[model] < self.min_cell
        if k in self.lapses:
            return any(0 < self.values[h] < self.min_cell for h in self._inside[k])
        return 0 < self.values[k] < self.min_cell

    def tied(self, k, hidden: set) -> bool:
        """A numerator stays null while its model count is null (whatever the proof says, it is shown with its n)."""
        model = self._model_of.get(k)
        return model is not None and model in hidden


@dataclass
class Protected:
    """What protect() decided: the null entries, the withheld groups and the proof's bookkeeping."""

    hidden: set
    withheld: bool | set          # tables (bool) or the set of withheld groups (publications)
    rounds: int
    pinned: set                   # always empty on return (kept for the report)
    undecided: set
    external: set = field(default_factory=set)   # sensitive, but fixed by public and always-shown numbers alone
    known: set = field(default_factory=set)      # every entry fixed by public and always-shown numbers alone


def _known(table: Table, budget: int) -> tuple[set, set]:
    """(known, external): the entries an attacker computes from the always-shown and public numbers alone (every other
    entry null, every bound 0), and the sensitive ones among them. They are public already: never a protection, and a
    null there is not a disclosure of this publication."""
    hidden = {k for k in table.values if table.hideable(k)}
    if not hidden:
        return set(), set()
    prob = table.problem(hidden, everything_withheld=True, window=None)
    pins = set()                       # only what bounds propagation PROVES: a key left out is merely not 'known'
    for k in prob.keys:
        lo, hi = prob.root_range(k)
        if lo == hi:
            pins.add(k)
    external = {k for k in pins if table.values[k] > 0 and table.sensitive(k, hidden - pins)}
    return pins, external


def protect(table: Table, budget: int = PROTECT_BUDGET, max_rounds: int = 400) -> Protected:
    """Primary + complementary suppression, verified exactly: loop until no sensitive null is pinned.

    0. what the always-shown and public numbers fix on their own is public already (``known``): printed (unless its
       group is withheld), never a complement; the sensitive ones are returned in ``external``;
    1. primary: every hideable count of 1-4, plus every entry of a group withheld by rule; then Table.close();
    2. the exact check (IntegerProblem.pinned, witnesses kept across rounds): a pinned null that is sensitive
       (Table.sensitive) or undecided, or a proven pin of a tied numerator (beside a null model count: it is never
       printed), gets one more complement, the cheapest printed entry of the smallest published
       relation that holds it (Table.complement), then 2 again;
    3. a pinned null that is not sensitive (a cell of 0 or >= 5) is printed, unless its group is withheld: it adds
       nothing an attacker cannot compute, and a null must never be computable; a margin over a null 0 is
       sensitive instead (Table.sensitive); the numerator of a model count printed here is printed with it when the
       exact check allows; then 2 again;
    4. if a sensitive null stays pinned and nothing is left to null, its group's breakdown is withheld (every entry of
       the group but the always-shown and public ones null, every bound there 0) and the check runs again.
    """
    known, external = _known(table, budget)
    if known:
        table = replace(table, public=table.public | frozenset(known))
    withheld = set(table.withhold)
    hidden = table.close(table.primary() | table.group_keys(withheld) | {k for k in table.seed if table.hideable(k)})
    witnesses: list = []
    for rounds in range(1, max_rounds + 1):
        if not hidden:
            return Protected(set(), withheld, rounds, set(), set(), external, known)
        prob = table.problem(hidden, withheld)
        maybe = {k for k in hidden if table.maybe_sensitive(k)}
        pins, undecided = prob.pinned(budget, witnesses, only=maybe)    # the ones that matter first
        stays = hidden - pins
        bad = sorted({k for k in pins if table.sensitive(k, stays)} | undecided, key=repr)
        if not bad:
            more, _unsure = prob.pinned(budget, witnesses, only=hidden - maybe)   # undecided ones simply stay null
            pins |= more
            stays = hidden - pins
            # a numerator beside a null model count is never printed (it is shown with its n), so a PROVEN pin of
            # one needs a complement like a sensitive null: a null is never computable
            bad = sorted({k for k in more if table.tied(k, stays)}, key=repr)
        if bad:
            cs = table.complements(bad, hidden)
            if cs:
                hidden = table.close(hidden | cs)
                continue
            groups = {table.group_of(k) for k in bad} - withheld
            if not groups:
                raise RuntimeError(f"cannot protect {bad[:3]}: nothing left to null and its table is withheld")
            withheld |= groups
            hidden = table.close(hidden | table.group_keys(withheld))
            continue
        printable = {k for k in pins if table.group_of(k) not in withheld and not table.tied(k, stays)}
        if printable:
            # a numerator nulled only by its tie to a model count printed here goes back with it (a rate's numerator
            # is printed with its n), when the exact check says that pins nothing sensitive and no null that must stay
            # null (a numerator tied to a null model count)
            nums = {num for model, num in table.numerator_of.items()
                    if model in printable and num in hidden and table.group_of(num) not in withheld}
            if nums:
                trial = hidden - printable - nums
                tprob = table.problem(trial, withheld)
                look = {k for k in trial if table.maybe_sensitive(k) or table.tied(k, trial)}
                tpins, tund = tprob.pinned(budget, witnesses, only=look)
                stays_t = trial - tpins
                if tund or any(table.sensitive(k, stays_t) or table.tied(k, stays_t) for k in tpins):
                    nums = set()
            hidden -= printable | nums
            continue
        return Protected(hidden, withheld, rounds, set(), set(), external, known)
    raise RuntimeError(f"protect() did not converge in {max_rounds} rounds")


def disclosure_report(table: Table, hidden: set, withheld: Iterable = (), ranges: bool = True,
                      budget: int = SEARCH_BUDGET) -> dict:
    """For an adversary or a checker: each null's exact feasible integer range given the published numbers."""
    prob = table.problem(set(hidden), withheld)
    pins, undecided = prob.pinned(budget)
    out = {"nulls": len(hidden), "pinned": sorted(map(repr, pins)), "undecided": sorted(map(repr, undecided))}
    if ranges:
        out["ranges"] = {repr(k): prob.feasible_range(k, budget) for k in prob.keys}
    return out


def recoverable(table: Table, hidden: set, withheld: bool | Iterable = (), budget: int = SEARCH_BUDGET) -> set:
    """Nulls whose exact value follows from the published numbers (integer, non-negative, the rule's lower bounds);
    undecided nulls count as recoverable. ``withheld``: True for a fully withheld table (every bound 0), or the
    withheld groups."""
    if not hidden:
        return set()
    if withheld is True:
        prob = table.problem(set(hidden), everything_withheld=True)
    else:
        prob = table.problem(set(hidden), () if withheld is False else withheld)
    pins, undecided = prob.pinned(budget)
    return pins | undecided


def printed_value(table: Table, got: Protected, key) -> int | None:
    """What a protected table prints for one entry: its value, or None (null: suppressed, or its group withheld)."""
    if key in table.always_shown:
        return table.values[key]
    withheld = got.withheld if isinstance(got.withheld, set) else ({""} if got.withheld else set())
    if key in got.hidden or table.group_of(key) in withheld:
        return None
    return table.values[key]


def leaks(table: Table, got: Protected, budget: int = SEARCH_BUDGET) -> set:
    """The SENSITIVE nulls of a protected publication that the printed numbers pin, other than those the public and
    always-shown numbers alone fix (``got.known``): empty when the protection holds."""
    withheld = got.withheld if isinstance(got.withheld, set) else ({""} if got.withheld else set())
    t = replace(table, public=table.public | frozenset(got.known))
    hidden = {k for k in got.hidden if k not in got.known}
    if not hidden:
        return set()
    maybe = {k for k in hidden if t.maybe_sensitive(k)}
    pins, undecided = t.problem(hidden, withheld).pinned(budget, only=maybe)
    return {k for k in pins if t.sensitive(k, hidden - pins)} | undecided


# --------------------------------------------------------------------------- building publications
class TableBuilder:
    """Accumulates the entries and relations of a publication (keys are tuples; a prefix keeps tables apart)."""

    def __init__(self) -> None:
        self.values: dict = {}
        self.forms: dict = {}
        self.equations: list[dict] = []
        self.inequalities: list[dict] = []
        self.always: set = set()
        self.public: set = set()
        self.lines: dict = defaultdict(list)
        self.numerator_of: dict = {}
        self.margins: set = set()
        self.lapses: set = set()
        self.aux: set = set()
        self.groups: dict = {}
        self.withhold: set = set()
        self.last_resort: set = set()
        self.seed: set = set()

    def atom(self, key, value: int, *, group=None, public: bool = False, lapses: bool = False,
             aux: bool = False) -> None:
        if key in self.values:
            raise ValueError(f"duplicate entry {key!r}")
        self.values[key] = int(value)
        if group is not None:
            self.groups[key] = group
        if public:
            self.public.add(key)
        if lapses:
            self.lapses.add(key)
        if aux:
            self.aux.add(key)

    def form(self, key, parts: dict, *, group=None, always: bool = False, public: bool = False,
             lapses: bool = False, margin: bool = False) -> None:
        """A derived entry: the sum of ``parts`` (base keys with coefficients); its value follows."""
        if key in self.values:
            raise ValueError(f"duplicate entry {key!r}")
        expanded: dict = defaultdict(int)
        for k, c in parts.items():
            for b, cb in self.forms.get(k, {k: 1}).items():
                expanded[b] += c * cb
        expanded = {b: c for b, c in expanded.items() if c}
        self.forms[key] = expanded
        self.values[key] = sum(c * self.values[b] for b, c in expanded.items())
        if group is not None:
            self.groups[key] = group
        if always:
            self.always.add(key)
        if public:
            self.public.add(key)
        if lapses:
            self.lapses.add(key)
        if margin:
            self.margins.add(key)

    def table(self) -> Table:
        return Table(values=dict(self.values), equations=list(self.equations), inequalities=list(self.inequalities),
                     forms=dict(self.forms), always_shown=frozenset(self.always), public=frozenset(self.public),
                     lines={k: list(v) for k, v in self.lines.items()}, numerator_of=dict(self.numerator_of),
                     margins=frozenset(self.margins), lapses=frozenset(self.lapses), aux=frozenset(self.aux),
                     groups=dict(self.groups), withhold=frozenset(self.withhold),
                     last_resort=frozenset(self.last_resort), seed=frozenset(self.seed))


def add_incident(b: TableBuilder, counts: dict, plans: tuple[str, ...], prefix: tuple = (), group=None) -> None:
    """One incident's table: cells plan x route (model, cancel_flow, dunning, current), each plan's model lapses, plan
    rows, route columns, the lapses total and the always-shown total. ``counts``: (plan, route) -> renewals and
    ("lapses", plan) -> model lapses. The current cells are public by design (named_renewal_member)."""
    def k(*parts):
        return (*prefix, *parts)

    for p in plans:
        for r in EXPOSURE_ROUTES:
            b.atom(k("cell", p, r), counts.get((p, r), 0), group=group, public=r == "current")
        b.atom(k("lapses", p), counts.get(("lapses", p), 0), group=group, lapses=True)
        b.inequalities.append({k("lapses", p): 1, k("cell", p, "model"): -1})
        b.numerator_of[k("cell", p, "model")] = k("lapses", p)
    for p in plans:
        b.form(k("plan", p), {k("cell", p, r): 1 for r in EXPOSURE_ROUTES}, group=group, margin=True)
        b.lines[k("plan", p)] = [*(k("cell", p, r) for r in EXPOSURE_ROUTES), k("lapses", p)]
    for r in EXPOSURE_ROUTES:
        b.form(k("route", r), {k("cell", p, r): 1 for p in plans}, group=group, margin=r != "current",
               public=r == "current")
        b.lines[k("route", r)] = [k("cell", p, r) for p in plans]
    b.form(k("lapses_total"), {k("lapses", p): 1 for p in plans}, group=group, lapses=True)
    b.inequalities.append({k("lapses_total"): 1, k("route", "model"): -1})
    b.lines[k("route", "model")] += [*(k("lapses", p) for p in plans), k("lapses_total")]
    b.numerator_of[k("route", "model")] = k("lapses_total")
    b.last_resort.add(k("lapses_total"))
    b.form(k("total"), {k("cell", p, r): 1 for p in plans for r in EXPOSURE_ROUTES}, group=group, always=True)
    if b.values[k("total")] < MIN_CELL and group is not None:
        b.withhold.add(group)


def add_pricing(b: TableBuilder, counts: dict, plans: tuple[str, ...], prefix: tuple = (), group=None) -> None:
    """One pricing change's table: cells plan x known_by_as_of x route, each model cell's lapses, the two
    known_by_as_of sides and the always-shown total. ``counts``: (plan, known, route) -> renewals and
    ("lapses", plan, known) -> model lapses. The current cells are public by design."""
    def k(*parts):
        return (*prefix, *parts)

    for p in plans:
        for kn in (True, False):
            for r in EXPOSURE_ROUTES:
                b.atom(k("cell", p, kn, r), counts.get((p, kn, r), 0), group=group, public=r == "current")
            b.atom(k("lapses", p, kn), counts.get(("lapses", p, kn), 0), group=group, lapses=True)
            b.inequalities.append({k("lapses", p, kn): 1, k("cell", p, kn, "model"): -1})
            b.numerator_of[k("cell", p, kn, "model")] = k("lapses", p, kn)
    for kn in (True, False):
        b.form(k("split", kn), {k("cell", p, kn, r): 1 for p in plans for r in EXPOSURE_ROUTES}, group=group,
               margin=True)
        b.lines[k("split", kn)] = [*(k("cell", p, kn, r) for p in plans for r in EXPOSURE_ROUTES),
                                   *(k("lapses", p, kn) for p in plans)]
    b.form(k("total"), {k("split", True): 1, k("split", False): 1}, group=group, always=True)
    if b.values[k("total")] < MIN_CELL and group is not None:
        b.withhold.add(group)


# --------------------------------------------------------------------------- the lapse-rate cubes
def band_index(label: str) -> int:
    return BANDS.index(label)


def lr_key(plan, first, band, part: str) -> tuple:
    """A box of the plan x first-after x band cube: each coordinate a value or ALL (band: an index into BANDS)."""
    return ("lr", plan, first, band, part)


def flag_key(inc, ov, part: str) -> tuple:
    """A box of the incident x overage cube; the whole cube is the lapse-rate total."""
    if inc == ALL and ov == ALL:
        return lr_key(ALL, ALL, ALL, part)
    return ("lrf", inc, ov, part)


def cube_boxes(dims: list[list]) -> list[tuple]:
    """Every box of a cube: each coordinate one value or ALL (the reachable answers of a decomposable family)."""
    return list(itertools.product(*([ALL, *values] for values in dims)))


def box_atoms(box: tuple, dims: list[list]) -> list[tuple]:
    return list(itertools.product(*(values if v == ALL else (v,) for v, values in zip(box, dims, strict=True))))


def box_inside(a: tuple, b: tuple) -> bool:
    """Box a lies inside box b."""
    return all(y == ALL or x == y for x, y in zip(a, b, strict=True))


def add_cube(b: TableBuilder, dims: list[list], atoms: dict, key_of: Callable, group=None) -> list[tuple]:
    """Every box of one cube as entries, n and lapses: the atoms (``atoms``: atom -> (n, lapses)) and every margin as a
    sum of atoms. A box's line is every box inside it; a lapses entry is its box's numerator; a lapses count is at most
    its n. ``key_of(box, part)`` names the entries (the all-ALL box may be shared with another cube)."""
    boxes = cube_boxes(dims)
    for atom in itertools.product(*dims):
        n, lap = atoms.get(atom, (0, 0))
        b.atom(key_of(atom, "n"), n, group=group)
        b.atom(key_of(atom, "l"), lap, group=group, lapses=True)
        b.inequalities.append({key_of(atom, "l"): 1, key_of(atom, "n"): -1})
    for box in boxes:
        if ALL not in box:
            continue
        for part in ("n", "l"):
            key = key_of(box, part)
            parts = {key_of(a, part): 1 for a in box_atoms(box, dims)}
            if key in b.values:                      # a shared total: one more relation, not a second entry
                b.equations.append({**parts, key: -1})
            else:
                b.form(key, parts, group=group, lapses=part == "l", margin=part == "n")
    for box in boxes:
        b.numerator_of[key_of(box, "n")] = key_of(box, "l")
        inner = [x for x in boxes if x != box and box_inside(x, box)]
        if inner:
            b.lines.setdefault(key_of(box, "n"), [])
            b.lines[key_of(box, "n")] += [key_of(x, part) for x in inner for part in ("n", "l")]
    return boxes


def box_values(box: tuple, dims: list[list], atoms: dict) -> tuple[int, int]:
    n = lap = 0
    for a in box_atoms(box, dims):
        x, y = atoms.get(a, (0, 0))
        n, lap = n + x, lap + y
    return n, lap


def hypercube_complements(dims: list[list], atoms: dict, key_of: Callable, already: set = frozenset()) -> set:
    """The hypercube method for a cube whose every margin is published: each box of 1-4 renewals (S) is protected by
    an alternating +/-1 move on the 2^d atoms of a hypercube (d = the dimensions S fixes; S's value and one other value
    on each; one fixed value on every dimension S sums over). The move leaves every box unchanged that sums over a
    dimension of the hypercube, so only the boxes holding exactly one moved atom change: S and its 2^d * 2^(free) - 1
    companions, which are nulled with it. The hypercube is chosen so that the move is feasible in both directions the
    rule needs (n: remove or add a renewed member, every decremented null staying >= 1; lapses: relabel or move a
    lapsed member) and nulls the fewest renewals; protect() then proves the result exactly. Returns the keys to null."""
    hidden = set(already)
    small = [box for box in cube_boxes(dims)
             if 0 < box_values(box, dims, atoms)[0] < MIN_CELL and any(v != ALL for v in box)]
    small.sort(key=lambda box: (box_values(box, dims, atoms)[0], repr(box)))
    for target in small:
        fixed_dims = [d for d, v in enumerate(target) if v != ALL]
        free_dims = [d for d, v in enumerate(target) if v == ALL]
        best = None
        for alts in itertools.product(*([x for x in dims[d] if x != target[d]] for d in fixed_dims)):
            for anchor in itertools.product(*(dims[d] for d in free_dims)):
                plan = _hypercube(target, fixed_dims, free_dims, alts, anchor, dims, atoms)
                if plan is None:
                    continue
                cost = sum(box_values(box, dims, atoms)[0] for box in plan if key_of(box, "n") not in hidden)
                if best is None or (cost, repr(plan)) < best[0]:
                    best = ((cost, repr(plan)), plan)
        if best is not None:
            hidden |= {key_of(box, part) for box in best[1] for part in ("n", "l")}
    return hidden


def _hypercube(target, fixed_dims, free_dims, alts, anchor, dims, atoms) -> list[tuple] | None:
    """The boxes one hypercube move changes, or None when the move is infeasible (module docstring of the method)."""
    corners = []                                     # (atom, sign): sign +1 on the target's own corner
    for choice in itertools.product((0, 1), repeat=len(fixed_dims)):
        atom = list(target)
        for d, alt, c in zip(fixed_dims, alts, choice, strict=True):
            atom[d] = alt if c else target[d]
        for d, v in zip(free_dims, anchor, strict=True):
            atom[d] = v
        corners.append((tuple(atom), -1 if sum(choice) % 2 else 1))
    changed: dict[tuple, int] = {}
    for atom, sign in corners:
        for keep in itertools.product((0, 1), repeat=len(free_dims)):
            box = list(atom)
            for d, k in zip(free_dims, keep, strict=True):
                if not k:
                    box[d] = ALL
            changed[tuple(box)] = sign
    vals = {box: box_values(box, dims, atoms) for box in changed}

    def n_move(o: int) -> bool:                      # move renewed members: n changes, lapses do not
        for atom, sign in corners:
            if o * sign < 0:
                n, lap = atoms.get(atom, (0, 0))
                if n - lap < 1:
                    return False
        return all(vals[box][0] >= 2 for box, sign in changed.items() if o * sign < 0)

    def l_move(o: int) -> bool:                      # relabel (n fixed) or move lapsed members (n and lapses)
        relabel = all((atoms.get(a, (0, 0))[0] - atoms.get(a, (0, 0))[1] >= 1) if o * s > 0 else
                      (atoms.get(a, (0, 0))[1] >= 1) for a, s in corners)
        lapsed = all(atoms.get(a, (0, 0))[1] >= 1 for a, s in corners if o * s < 0) and \
            all(vals[box][0] >= 2 for box, sign in changed.items() if o * sign < 0)
        return relabel or lapsed

    if not (n_move(1) or n_move(-1)) or not (l_move(1) or l_move(-1)):
        return None
    return sorted(changed, key=repr)


def add_lapse_cubes(b: TableBuilder, cube: dict, flags: dict, plans: tuple[str, ...], group=None) -> None:
    """The lapse-rate family: every box of plan x first-after x band and of incident x overage, n and lapses, the
    total shared. ``cube``: (plan, first, band index) -> (n, lapses); ``flags``: (inc, ov) -> (n, lapses). Each cube's
    small boxes get hypercube complements (Table.seed)."""
    main = [list(plans), [0, 1], list(range(len(BANDS)))]
    flag = [[0, 1], [0, 1]]

    def main_key(box, part):
        return lr_key(*box, part)

    def flag_key_of(box, part):
        return flag_key(*box, part)

    add_cube(b, main, cube, main_key, group)
    # the population total is always shown (the denominator every answer starts from), unless it is itself small
    if b.values[lr_key(ALL, ALL, ALL, "n")] >= MIN_CELL:
        b.always |= {lr_key(ALL, ALL, ALL, "n"), lr_key(ALL, ALL, ALL, "l")}
    add_cube(b, flag, flags, flag_key_of, group)
    b.seed |= hypercube_complements(main, cube, main_key)
    b.seed |= hypercube_complements(flag, flags, flag_key_of)


# --------------------------------------------------------------------------- the build's publication
@dataclass
class Population:
    """The true counts the publication is made of (pandas over the build's Parquet; ``population_counts``)."""

    plans: tuple[str, ...]
    cube: dict          # (plan, first, band index) -> (n, lapses), model renewals
    flags: dict         # (incident flag, overage flag) -> (n, lapses), model renewals
    routes: dict        # (plan, route, outcome) -> renewals, every route but model
    incidents: dict     # incident id -> {(plan, route): n, ("lapses", plan): lapses}
    pricing: dict       # change id -> {(plan, known, route): n, ("lapses", plan, known): lapses}
    covered: dict       # plan -> (model renewals with the incident flag, their lapses): the union, never printed
    one_edge: bool      # every renewal has at most one FIRST_RENEWAL_AFTER edge (the pricing tables add up exactly)


def population_counts(ctx) -> Population:
    """The build's counts: model renewals by plan x first-after x band and incident x overage, the other routes by
    plan, and every incident's and pricing change's exposure table (the same rules as the exposure templates)."""
    ren = ctx.renewals()
    plans = tuple(sorted(set(ctx.nodes("Plan")["plan_tier"]) | set(ren["plan_tier"])))
    model = ren[ren["route"] == POPULATION_ROUTE]
    band = model["limit_hits_14d"].map(lambda v: band_index(limit_hits_band(int(v))))
    keys = pd.DataFrame({"plan": model["plan_tier"].astype(str), "first": model["first_renewal_after_pricing_change"]
                         .astype(int), "band": band.astype(int), "churned": model["churned"].astype(int),
                         "inc": model["incident_exposed_28d"].astype(int), "ov": model["overage_toggled_off"]
                         .astype(int)})
    cube = {(str(p), int(f), int(b_)): (len(g), int(g["churned"].sum()))
            for (p, f, b_), g in keys.groupby(["plan", "first", "band"], sort=True)}
    flags = {(int(i), int(o)): (len(g), int(g["churned"].sum())) for (i, o), g in keys.groupby(["inc", "ov"],
                                                                                              sort=True)}
    covered = {str(p): (len(g), int(g["churned"].sum())) for p, g in keys[keys["inc"] == 1].groupby("plan")}
    other = ren[ren["route"] != POPULATION_ROUTE]
    routes = {(str(p), str(r), str(o)): int(n) for (p, r, o), n in
              other.groupby(["plan_tier", "route", "outcome"], sort=True).size().items()}
    churned = ren["churned"].astype(int)
    route = ren["route"].map(lambda r: "current" if r in CURRENT_ROUTES else str(r))
    # incidents: active on an incident day inside the renewal's own window (as_of - 28, as_of]
    x = ctx.edges("EXPOSED_TO")[["src", "dst", "event_date"]]
    hr = ctx.edges("HAS_RENEWAL")[["src", "dst"]].rename(columns={"dst": "renewal_id"})
    x = x.merge(hr, on="src")
    as_of = ren["as_of"].reindex(x["renewal_id"]).to_numpy()
    when = x["event_date"].to_numpy()
    window = np.array([(a - dt.timedelta(days=28)) < w <= a for a, w in zip(as_of, when, strict=True)], dtype=bool)
    x = x[window]
    incidents: dict = {}
    for inc in sorted(ctx.nodes("Incident")["incident_id"]):
        ids = sorted(set(x.loc[x["dst"] == inc, "renewal_id"]))
        incidents[str(inc)] = _exposure_counts(ren.loc[ids], route.loc[ids], churned.loc[ids], ())
    f = ctx.edges("FIRST_RENEWAL_AFTER")[["src", "dst", "known_by_as_of"]]
    one_edge = bool(f["src"].is_unique)
    pricing: dict = {}
    for pc in sorted(ctx.nodes("PricingChange")["change_id"]):
        g = f[f["dst"] == pc]
        ids = list(g["src"])
        known = pd.Series(g["known_by_as_of"].astype(bool).to_numpy(), index=ids)
        pricing[str(pc)] = _exposure_counts(ren.loc[ids], route.loc[ids], churned.loc[ids], (known,))
    return Population(plans, cube, flags, routes, incidents, pricing, covered, one_edge)


def _exposure_counts(rows: pd.DataFrame, route: pd.Series, churned: pd.Series, extra: tuple) -> dict:
    out: dict = defaultdict(int)
    keys = [rows["plan_tier"].astype(str).to_numpy(), *(e.reindex(rows.index).to_numpy() for e in extra)]
    for i in range(len(rows)):
        k = tuple(v[i].item() if hasattr(v[i], "item") else v[i] for v in keys)
        r = route.iloc[i]
        out[(*k, r)] += 1
        if r == "model":
            out[("lapses", *k)] += int(churned.iloc[i])
    return dict(out)


def population_table(pop: Population) -> Table:
    """Everything the aggregate tools print about the population, as one Table (module docstring)."""
    b = TableBuilder()
    plans = pop.plans
    add_lapse_cubes(b, pop.cube, pop.flags, plans, group="population")
    combos = sorted({(r, o) for _, r, o in pop.routes} | {c for c in ROUTE_OUTCOMES if c[0] != POPULATION_ROUTE})
    for p in plans:
        for r, o in combos:
            b.atom(("rc", p, r, o), pop.routes.get((p, r, o), 0), group="population", public=r in PUBLIC_ROUTES)
    for r, o in combos:
        b.form(("rc", ALL, r, o), {("rc", p, r, o): 1 for p in plans}, group="population", public=r in PUBLIC_ROUTES,
               margin=r not in PUBLIC_ROUTES)
        b.lines[("rc", ALL, r, o)] = [("rc", p, r, o) for p in plans]
    b.form(("renewals",), {lr_key(ALL, ALL, ALL, "n"): 1, **{("rc", p, r, o): 1 for p in plans for r, o in combos}},
           group="population", always=True)
    for inc, counts in sorted(pop.incidents.items()):
        add_incident(b, counts, plans, ("ix", inc), group=("incident", inc))
    for pc, counts in sorted(pop.pricing.items()):
        add_pricing(b, counts, plans, ("pc", pc), group=("pricing", pc))
    _couple(b, pop, combos)
    return b.table()


def _couple(b: TableBuilder, pop: Population, combos: list) -> None:
    """How the tables relate (what an attacker knows of the structure; module docstring)."""
    plans = pop.plans
    nonmodel = {"cancel_flow": [c for c in combos if c[0] == "cancel_flow"],
                "dunning": [c for c in combos if c[0] == "dunning"]}
    # pricing: a first renewal after a change has one FIRST_RENEWAL_AFTER edge, so per plan the model cells of every
    # change add up to the first-after box of the lapse rates (lapses too); other routes are at most the route count
    for p in plans:
        for part, name in (("n", "cell"), ("l", "lapses")):
            cells = [("pc", pc, name, p, kn, "model") if name == "cell" else ("pc", pc, name, p, kn)
                     for pc in pop.pricing for kn in (True, False)]
            if pop.one_edge:             # sum over the changes == the first-after box
                b.equations.append({**dict.fromkeys(cells, 1), lr_key(p, 1, ALL, part): -1})
            else:                        # a renewal may count twice: the sum is at least the box
                b.inequalities.append({**dict.fromkeys(cells, -1), lr_key(p, 1, ALL, part): 1})
        if pop.one_edge:
            for r, cs in nonmodel.items():
                cells = {("pc", pc, "cell", p, kn, r): 1 for pc in pop.pricing for kn in (True, False)}
                if cells:
                    b.inequalities.append({**cells, **{("rc", p, *c): -1 for c in cs}})
    # incidents: exposed <= the population, per plan and route; the flag is the union of the incidents per plan
    for inc in pop.incidents:
        for p in plans:
            b.inequalities.append({("ix", inc, "cell", p, "model"): 1, lr_key(p, ALL, ALL, "n"): -1})
            b.inequalities.append({("ix", inc, "lapses", p): 1, lr_key(p, ALL, ALL, "l"): -1})
            b.inequalities.append({("ix", inc, "cell", p, "model"): 1, ("ix", inc, "lapses", p): -1,
                                   lr_key(p, ALL, ALL, "n"): -1, lr_key(p, ALL, ALL, "l"): 1})
            for r, cs in nonmodel.items():
                b.inequalities.append({("ix", inc, "cell", p, r): 1, **{("rc", p, *c): -1 for c in cs}})
    if pop.incidents:
        for p in plans:
            n, lap = pop.covered.get(p, (0, 0))
            b.atom(("aux", "covered", p, "n"), n, aux=True)
            b.atom(("aux", "covered", p, "l"), lap, aux=True)
            un, ul = ("aux", "covered", p, "n"), ("aux", "covered", p, "l")
            b.inequalities.append({ul: 1, un: -1})
            b.inequalities.append({un: 1, lr_key(p, ALL, ALL, "n"): -1})
            b.inequalities.append({ul: 1, lr_key(p, ALL, ALL, "l"): -1})
            b.inequalities.append({un: 1, ul: -1, lr_key(p, ALL, ALL, "n"): -1, lr_key(p, ALL, ALL, "l"): 1})
            for inc in pop.incidents:          # the union holds every incident's share: largest <= union
                m, lap_ = ("ix", inc, "cell", p, "model"), ("ix", inc, "lapses", p)
                b.inequalities.append({m: 1, un: -1})
                b.inequalities.append({lap_: 1, ul: -1})
                b.inequalities.append({m: 1, lap_: -1, un: -1, ul: 1})
            every = list(pop.incidents)         # ... and union <= the sum of the shares
            b.inequalities.append({un: 1, **{("ix", inc, "cell", p, "model"): -1 for inc in every}})
            b.inequalities.append({ul: 1, **{("ix", inc, "lapses", p): -1 for inc in every}})
            b.inequalities.append({un: 1, ul: -1, **{("ix", inc, "cell", p, "model"): -1 for inc in every},
                                   **{("ix", inc, "lapses", p): 1 for inc in every}})
        for part in ("n", "l"):                 # and the unions add up to the flag
            b.equations.append({**{("aux", "covered", p, part): 1 for p in plans}, flag_key(1, ALL, part): -1})


class Publication:
    """One build's protected publication: what every aggregate answer prints (``value``) and what it withholds."""

    def __init__(self, pop: Population, table: Table, got: Protected) -> None:
        self.pop, self.table, self.got = pop, table, got
        self.withheld_groups = got.withheld if isinstance(got.withheld, set) else set()

    def value(self, key) -> int | None:
        """The printed value of an entry, or None (null)."""
        return printed_value(self.table, self.got, key)

    def truth(self, key) -> int:
        return self.table.values[key]

    def withheld(self, group) -> bool:
        return group in self.withheld_groups

    def external(self, keys: Iterable) -> int:
        """How many of ``keys`` are printed although sensitive: the public numbers alone already fix them."""
        return sum(1 for k in keys if k in self.got.external and self.value(k) is not None)


def publication(ctx) -> Publication:
    """The build's publication, computed once per context (a build is immutable while it is served)."""
    def build() -> Publication:
        pop = population_counts(ctx)
        table = population_table(pop)
        return Publication(pop, table, protect(table))
    return ctx.cached("population_publication", build)


# --------------------------------------------------------------------------- metric tools
def limit_hits_band(value: int) -> str:
    for label, lo, hi in LIMIT_HIT_BANDS:
        if value >= lo and (hi is None or value <= hi):
            return label
    raise ValueError(value)


def lapse_rate_dimensions(group_by: list[str], plan_tier, first_after, lo, hi) -> tuple[set, str | None]:
    """(dimensions an answer touches, why it is refused or None). The family of metric_lapse_rate (module docstring)."""
    dims = set(group_by)
    if plan_tier is not None:
        dims.add("plan_tier")
    if first_after is not None:
        dims.add("first_renewal_after_pricing_change")
    if lo is not None or hi is not None:
        dims.add("limit_hits_14d_band")
    if (lo is not None or hi is not None) and band_of_range(lo, hi) is None:
        return dims, BAND_HINT
    flagged = dims & set(FLAG_CUBE_KEYS)
    if flagged and dims - set(FLAG_CUBE_KEYS):
        return dims, ("incident_exposed_28d and overage_toggled_off are grouped alone or together, over all model "
                      "renewals: drop the other keys and the plan_tier / first_renewal_after_pricing_change / "
                      "limit_hits_14d filters, or group by plan_tier, first_renewal_after_pricing_change and "
                      "limit_hits_14d_band instead")
    return dims, None


BAND_HINT = ("limit_hits_14d_min / limit_hits_14d_max select one band at a time (0, 1-2, 3-5, 6-9, 10+): e.g. min 3 "
             "and max 5, or min 10 alone; to compare bands, group_by limit_hits_14d_band")


def band_of_range(lo: int | None, hi: int | None):
    """The band a limit_hits_14d range selects (an index into BANDS), ALL for no range, None when it is not one band."""
    if lo is None and hi is None:
        return ALL
    for i, (_, start, end) in enumerate(LIMIT_HIT_BANDS):
        if (lo if lo is not None else 0) == start and hi == end:
            return i
    return None


def _cell_key(keys: list[str], cell: tuple, plan_tier, first_after, band) -> tuple:
    """The box of one cell (or, with no keys, of the total) as a key of the publication (n part)."""
    v = dict(zip(keys, cell, strict=True))
    if set(keys) & set(FLAG_CUBE_KEYS):
        return flag_key(v.get("incident_exposed_28d", ALL), v.get("overage_toggled_off", ALL), "n")
    p = v.get("plan_tier", ALL if plan_tier is None else plan_tier)
    f = v.get("first_renewal_after_pricing_change", ALL if first_after is None else int(bool(first_after)))
    if "limit_hits_14d_band" in v:
        return lr_key(p, f, band_index(v["limit_hits_14d_band"]), "n")
    return lr_key(p, f, band, "n")


def _lapses(key: tuple) -> tuple:
    return (*key[:-1], "l")


def metric_lapse_rate(ctx, *, group_by: list[str] | None = None, plan_tier: str | None = None,
                      first_renewal_after_pricing_change: bool | None = None, limit_hits_14d_min: int | None = None,
                      limit_hits_14d_max: int | None = None) -> dict:
    """Voluntary-lapse rate of model-routed renewals, filtered and grouped (n, lapses, rate, Wilson 95%)."""
    keys = list(group_by or [])
    _, why = lapse_rate_dimensions(keys, plan_tier, first_renewal_after_pricing_change, limit_hits_14d_min,
                                   limit_hits_14d_max)
    if why:
        raise ToolArgumentError(f"invalid arguments for metric_lapse_rate: {why}")
    pub = publication(ctx)
    band = band_of_range(limit_hits_14d_min, limit_hits_14d_max)
    filters: dict = {}
    if plan_tier is not None:
        filters["plan_tier"] = plan_tier
    if first_renewal_after_pricing_change is not None:
        filters["first_renewal_after_pricing_change"] = bool(first_renewal_after_pricing_change)
    if limit_hits_14d_min is not None:
        filters["limit_hits_14d_min"] = int(limit_hits_14d_min)
    if limit_hits_14d_max is not None:
        filters["limit_hits_14d_max"] = int(limit_hits_14d_max)
    domains = []
    for k in keys:
        dom = _domain(pub, k)
        if k == "limit_hits_14d_band" and band != ALL:
            dom = [BANDS[band]]
        domains.append(dom)

    def rate_of(key: tuple) -> dict:
        return _rate(pub.value(key), pub.value(_lapses(key)))

    cells = []
    shown = []
    for cell in itertools.product(*domains):
        key = _cell_key(keys, cell, plan_tier, first_renewal_after_pricing_change, band)
        row = {k: _group_value(k, v) for k, v in zip(keys, cell, strict=True)}
        row.update(rate_of(key))
        cells.append(row)
        shown.append(key)
    total_key = _cell_key([], (), plan_tier, first_renewal_after_pricing_change, band)
    total = rate_of(total_key)
    data = {"population": POPULATION, "filters": filters, "group_by": keys, "total": total, "cells": cells}
    if "limit_hits_14d_band" in keys:
        data["bands"] = [label for label, _, _ in LIMIT_HIT_BANDS]
    caveats = ["Descriptive rates over model-routed renewals (synthetic data), not a risk estimate and not causal: "
               "an association put there by the data generator is still only an association.",
               "Every rate carries n and a Wilson 95% interval; read the interval, not the point estimate.",
               f"An n under {MIN_CELL} is null, and so is enough of the rest that no null can be computed back from "
               f"this or any other answer of the population tools (each null keeps several possible values). A "
               f"filter is a slice: it prints what the grouped answer prints for that value."]
    if any(c["suppressed"] for c in cells) or total["suppressed"]:
        caveats.append("Some cells are suppressed: widen the filters or group by fewer keys.")
    n_ext = pub.external([total_key, *shown])
    if n_ext:
        caveats.append(f"{n_ext} count(s) here are printed although small: the public numbers (the current renewals "
                       f"and the always-shown totals) already fix them.")
    if total["n"] == 0:          # only a PRINTED 0: the caveat must never give a null total back
        caveats.append("No model-routed renewal matches these filters.")
    return ctx.envelope(data, caveats)


def _domain(pub: Publication, key: str) -> list:
    """Every value a grouping key can take (the fixed shape of a lapse-rate answer)."""
    if key == "plan_tier":
        return list(pub.pop.plans)
    if key == "limit_hits_14d_band":
        return list(BANDS)
    return [0, 1]


def _group_value(key: str, value):
    return bool(value) if key in FLAG_KEYS else value


def _rate(n: int | None, lapses: int | None) -> dict:
    """n / lapses / rate / wilson_95 / suppressed for one printed (or partly null) rate cell."""
    if n is None or lapses is None:
        return {"n": n, "lapses": lapses if n is not None else None, "rate": None, "wilson_95": None,
                "suppressed": True}
    return rate_cell(n, lapses, False)


def metric_route_counts(ctx, *, plan_tier: str | None = None) -> dict:
    """Renewals per route and outcome, for one plan or all plans (one publication for the build)."""
    pub = publication(ctx)
    t = pub.table
    p = ALL if plan_tier is None else plan_tier
    n_key, l_key = lr_key(p, ALL, ALL, "n"), lr_key(p, ALL, ALL, "l")
    n, lap = pub.value(n_key), pub.value(l_key)
    rows = []
    for r, o in [*ROUTE_OUTCOMES, *sorted({(r, o) for _, r, o in pub.pop.routes} - set(ROUTE_OUTCOMES))]:
        if r == POPULATION_ROUTE:
            value = (None if n is None or lap is None else n - lap) if o == "renewed" else lap
            truth = (t.values[n_key] - t.values[l_key]) if o == "renewed" else t.values[l_key]
        else:
            key = ("rc", p, r, o)
            value, truth = pub.value(key), t.values.get(key, 0)
        rows.append({"route": r, "outcome": o, "renewals": value, "suppressed": value is None, "_truth": truth})
    routes_present = sorted({x["route"] for x in rows if x["_truth"] != 0})
    for x in rows:
        del x["_truth"]
    data = {"population": "all renewals of the build, by route and outcome (outcomes as of the build's data_end)",
            "plan_tier": plan_tier or "all", "total_renewals": pub.value(("renewals",)), "counts": rows,
            "route_meaning": {r: ROUTE_MEANING.get(r, "") for r in routes_present}}
    caveats = ["Counts, not rates: for a lapse rate with n and a Wilson interval use metric_lapse_rate.",
               f"Every route and outcome is listed. A count under {MIN_CELL} is null, and so is enough of the rest "
               f"that no null can be computed back from this or any other answer of the population tools.",
               "The model rows are the lapse-rate numbers of the same plan (renewed = n - lapses), null together.",
               "score_today and pending are the current renewals: public by design (graph_find serves each one with "
               "its route), so their counts are exact."]
    return ctx.envelope(data, caveats)


def metric_feature_card(ctx, *, feature: str) -> dict:
    """The feature card of one of the 22 gold features (spec.FEATURE_CARDS), with its PIT window."""
    card = dict(spec.FEATURE_CARDS[feature])
    edge = card.get("backing_edge")
    window = spec.PIT_WINDOWS.get(edge) if edge else None
    contract = ctx.contract
    data = {"feature": feature, **card,
            "graph_window_rule": window.note if window else None,
            "build_contract": {"state": contract.get("state"), "strict": contract.get("strict"),
                               "checked_at": contract.get("checked_at")}}
    caveats = []
    if card["verification"] == "graph-verified":
        caveats.append(f"graph-verified: the graph contract re-derives {feature} from {edge} edges for every renewal "
                       f"and requires 0 mismatches against gold (build contract: {contract.get('state')}).")
    else:
        caveats.append(f"declared: {feature} is computed from source tables the graph aggregates instead of "
                       f"materialising; its window comes from the gold SQL (see lineage_pit).")
    if feature == "first_renewal_after_pricing_change":
        f = ctx.edges("FIRST_RENEWAL_AFTER")
        unknown = f.loc[~f["known_by_as_of"].astype(bool)]
        data["declared_exception"] = {
            "rule": "gold rule renewal_date - 30 <= effective_date < renewal_date (not as_of)",
            "edges_known_by_as_of_false": int(len(unknown)),
            "edges_total": int(len(f)),
            "by_pricing_change": {str(k): int(v) for k, v in sorted(unknown.groupby("dst").size().items())}}
        caveats.append("Declared exception: this flag can read a pricing change that took effect after the renewal's "
                       "as_of (T-7); such edges carry known_by_as_of=false and are served flagged, never hidden.")
    elif card["pit_status"] == "declared_exception":
        caveats.append("Declared exception: its upper bound is the renewal_date, not as_of; lineage_pit gives the "
                       "measured number of rows it affects.")
    return ctx.envelope(data, caveats)


TOOLS = {"metric_lapse_rate": metric_lapse_rate, "metric_route_counts": metric_route_counts,
         "metric_feature_card": metric_feature_card}
