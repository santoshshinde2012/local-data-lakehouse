"""Feature cohorts: NetworkX Louvain and Leiden communities over SIMILAR_TO (outside the contract).

Spec ``cohorts/renewal-v1``:
  graph      the undirected union of the SIMILAR_TO edges among the reference set (route =
             'model', the kNN candidates): one edge {a, b} when a -> b or b -> a, weight
             1 / (1 + dist) (dist is symmetric, so both directions carry the same weight)
  detection  networkx.community.louvain_communities and leiden_communities (metric
             'modularity'), seed 42, resolution 1.0 (NetworkX 3.7, BSD-3; igraph / leidenalg
             are GPL and not used). Nodes are integers in renewal_id order and edges are added
             in sorted order, so a fixed seed gives the same partition in every process
             (string nodes would make set iteration depend on PYTHONHASHSEED)
  labels     per algorithm, communities sorted by size (descending), then by their smallest
             renewal_id: ``leiden-01`` is the largest Leiden cohort
  others     a non-reference renewal (dunning, cancel_flow, score_today, pending) takes the
             cohort of its rank-1 SIMILAR_TO neighbour (always a reference renewal):
             ``assigned_via = nearest_reference``

Output (never part of the graph contract; the contract neither reads nor checks it):
  <build_dir>/cohorts.parquet        one row per renewal: renewal_id, plan_tier, is_reference,
                                     leiden, louvain, assigned_via, via_renewal_id, spec_version
                                     (no outcome column: rates are computed at question time)
  <build_dir>/manifest.json["cohorts"]  the sidecar summary: library + version, seed, resolution,
                                     weight rule, per algorithm the number of cohorts, modularity,
                                     sizes (published ones largest first, then null per withheld
                                     cohort) and plan purity, the output sha256 and the sha256 of
                                     the two Parquet inputs as read (under the build lock, checked
                                     against the build's manifest).
  Both are registered in build.CARRY_OVER (register_carry_over("cohorts", ...)), so a
  byte-identical rebuild of the business graph keeps them.

Questions (pure functions over the Parquet; ``ctx`` is a build directory or any object with
``build_dir``):
  cohort_summary(ctx, cohort_id=None, renewal_id=None, algorithm=None) -> (data, caveats)
      n, voluntary lapses, rate and Wilson 95% interval over MODEL rows only; plan mix; the
      top distinguishing features (mean z-score of the members against the reference
      population, with the persisted SIMILAR_TO scaler; none for a withheld cohort). For a named renewal the
      outcome_visibility rule of the neighbour tool applies: a historical renewal only counts
      member outcomes observed on or before its own as_of, and never its own outcome.
  cohort_list(ctx, algorithm="leiden") -> (data, caveats)

Small cells (MIN_CELL = 5): no count of fewer than 5 renewals is printed (null), and no withheld
count follows from the printed numbers by arithmetic (sums, differences, means):
  * withheld cohorts (withheld_cohorts): the ones under MIN_CELL, plus complements. The population
    templates publish each plan's model n and lapses as of today (queries.first_renewal_after_by_plan,
    routes; metrics.py) and cohorts nest inside plans (SIMILAR_TO is blocked by plan), so a plan
    never holds exactly one withheld cohort beside published ones, and its withheld cohorts together
    (the plan total minus its published cohorts) hold at least MIN_CELL renewals: else the smallest
    published cohort of that plan is withheld too, until both hold. A cohort that is its plan's
    whole block (tiny: ultra, 4 model renewals) is the plan's own population cell: no complement can
    hide it, and suppressing that cell is the population tools' job. A withheld cohort whose model
    renewals are exactly a cell metric_lapse_rate prints (lapse_rate_cells: its shared plan and flags,
    its limit_hits_14d range; 5+ renewals) counts as published in that arithmetic, since metrics
    prints its n and lapses: the plan's other withheld cohorts must cover each other without it;
  * the per-cohort count of assigned non-reference renewals (assigned_cells) gets the same rule
    against the population route totals of the build (routes) AND of each plan
    (metric_route_counts(plan_tier=...): cancel_flow, dunning and the exact score_today / pending
    counts): neither the build nor a plan holds exactly one hidden cell beside published ones, or
    hidden cells holding 1 to MIN_CELL - 1 renewals together. A published cohort's plan mix gets
    it against its size (suppress_cells);
  * a withheld cohort prints its id, its plan and its assigned non-reference cell, nothing else: no
    size, plan mix or outcome count in any view, no top features (a member mean of an integer
    feature, times the size, is near a whole number: with the label order it pinned a withheld
    size exactly), and one name and one caveat whether it is small or a complement (its label still
    ranks it by size, so a plan's last withheld cohort is a small one);
  * on the page: model_renewals = n + not_yet_observed + [named renewal excluded], and the plan
    mix sums to model_renewals;
  * a past view (a named historical renewal: outcomes observed by its as_of, never its own) counts
    a subset of today's cell, so its n and lapses are lower bounds of today's. It differs from
    today's published cell by D = the members not yet observed at that as_of plus the named renewal
    itself, and is published only when D is 0 or at least MIN_CELL: else today's lapses minus its
    lapses would print the outcome of fewer than MIN_CELL renewals (the named renewal's own among
    them). A withheld cohort's past views are withheld whatever D is (they would bound its size).
  What it does not stop (documented limits: the cohort rule is checked as arithmetic over printed
  numbers, while metrics.protect() proves metrics answers over integers with bounds; adopting it
  here is future work): labels rank cohorts by size across plans, so the published cohorts ranked around a
  withheld one bound its size, and with the plan totals tight bounds (equal or interleaved
  neighbours) can pin it; counts are non-negative and lapses at most n, so a plan whose withheld
  cohorts hold no lapse (or only lapses) in total says so for each of them; differencing two past
  views of one cohort (two sources with nearby as_of values); metrics cells that are not one
  cohort but a union or difference of cohorts and other cells (two metric_lapse_rate answers whose
  populations differ by part of a withheld cohort: metrics' own documented limit on differencing
  across calls), and a plan whose withheld cohorts are all metrics cells but a small one (no
  published cohort is left to withhold beside it); and counting members one renewal at a
  time: cohort_summary(renewal_id=...) names a renewal's cohort, and the neighbour tool's
  SIMILAR_TO edges with the recorded algorithm, seed and NetworkX version reproduce every cohort, so
  cohort sizes are descriptive, not secret. What stays out of every printed number is a count of
  fewer than MIN_CELL renewals and any outcome count of a withheld cohort, counting the population
  templates' plan and route totals and the metric_lapse_rate cells a cohort coincides with.
  tests/graph/test_cohorts.py checks it as a property over random cohort tables (exact recovery by
  linear algebra over every printed number: the cohort tools, each plan's model n and lapses, the
  non-reference route counts of the build and of each plan, and the metrics cells some cohorts are
  made to coincide with). No withheld cohort occurs at MIN_CELL 5 in s42 (every cohort has 55+
  renewals); tiny withholds ultra's whole block. On the shipped data no withheld cohort coincides
  with a metrics cell at any MIN_CELL from 5 to 60 (s42: leiden-10 / -11 and louvain-09 / -12 coincide,
  all published), so the rule changes nothing there today.

Honesty: cohorts rediscover feature segments (the generator's cohorts; plan purity is 1.0
because SIMILAR_TO is blocked by plan). They are labels, not structure: subscriptions have no
relationships to each other.

Seed 42, N=8000 (weighted, as specified): Louvain 15 cohorts (modularity 0.8065), Leiden 15 (0.8056).
PLAN 2.2's "Louvain 15 / Leiden 14" was measured on the unweighted graph, which gives 15 / 14 here
too; the plan's named cohorts come back under the weights (overage-off 30 / 143 in both, cap
pressure pro 36 / 152 = 23.7% in Louvain against the plan's 24.5%). Checked by
tests/graph/test_cohorts.py::test_seed_42_numbers_against_the_plan.

Build: python scripts/build_graph_cohorts.py --profile <p> | --build <dir> (Makefile alias: make
graph-cohorts). Tests: tests/graph/test_cohorts.py.
"""
from __future__ import annotations

import contextlib
import difflib
import functools
import hashlib
import math
import os
import re
import time
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import metrics, spec, store

COHORT_SPEC_VERSION = "cohorts/renewal-v1"
COHORTS_FILE = "cohorts.parquet"
MANIFEST_KEY = "cohorts"             # build.CARRY_OVER manifest key: carried over together with cohorts.parquet
ALGORITHMS = ("leiden", "louvain")
DEFAULT_ALGORITHM = "leiden"
SEED = 42
RESOLUTION = 1.0
LEIDEN_METRIC = "modularity"
LEIDEN_THETA = 0.01                  # the NetworkX default, recorded
WEIGHT_RULE = "1 / (1 + dist)"
MIN_CELL = 5                         # cells with fewer rows are suppressed
TOP_FEATURES = 5
CURRENT_ROUTES = ("score_today", "pending")
ASSIGNED_MEMBER, ASSIGNED_NEAREST, ASSIGNED_NONE = "community", "nearest_reference", "none"
CAVEAT = "Cohorts rediscover feature segments; labels, not structure."
WITHHELD_NAME = "withheld cohort (small, or the complement of a small one)"
COHORT_ID_RE = re.compile(r"^(leiden|louvain)-(\d{2,4})$")
RENEWAL_ID_RE = re.compile(r"^sub_[a-z0-9_]+:\d{4}-\d{2}-\d{2}$")
WILSON_Z = 1.959963984540054
_JUNK = ("", "null", "none")
_CODE = "src/lakehouse_graph/cohorts.py"
INPUT_FILES = ("parquet/nodes_Renewal.parquet", "parquet/edges_SIMILAR_TO.parquet")   # what the cohorts are made from
# What metric_lapse_rate can select model renewals by (metrics.py): plan_tier and first_renewal_after as filters
# or keys, incident_exposed_28d / overage_toggled_off as keys, any limit_hits_14d range as a filter.
LAPSE_RATE_COLUMNS = ("route", "plan_tier", *metrics.FLAG_KEYS, "limit_hits_14d")

SCHEMA = pa.schema([
    pa.field("renewal_id", spec.STR, nullable=False), pa.field("plan_tier", spec.STR),
    pa.field("is_reference", spec.BOOL), pa.field("leiden", spec.STR), pa.field("louvain", spec.STR),
    pa.field("assigned_via", spec.STR), pa.field("via_renewal_id", spec.STR), pa.field("spec_version", spec.STR)])


class CohortsUnavailable(RuntimeError):
    """The build has no cohorts.parquet (run scripts/build_graph_cohorts.py --build <dir>)."""


# --------------------------------------------------------------------------- pure functions
def wilson(k: int, n: int, z: float = WILSON_Z) -> list[float] | None:
    """Wilson score interval for k successes in n (95% by default); None when n is 0."""
    if n <= 0:
        return None
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return [round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4)]


def reference_graph(renewals: pd.DataFrame, similar: pd.DataFrame) -> tuple[nx.Graph, list[str]]:
    """(graph, ids): the undirected SIMILAR_TO union among reference renewals, integer nodes.

    ``ids[i]`` is the renewal_id of node i (renewal_id order); weight = 1 / (1 + dist).
    """
    ids = sorted(renewals.loc[renewals["route"] == spec.REFERENCE_ROUTE, "renewal_id"])
    pos = {rid: i for i, rid in enumerate(ids)}
    e = similar[similar["src"].isin(pos) & similar["dst"].isin(pos)]
    weight: dict[tuple[int, int], float] = {}
    for a, b, dist in zip(e["src"].map(pos), e["dst"].map(pos), e["dist"], strict=True):
        if a == b:
            continue
        key = (a, b) if a < b else (b, a)
        w = 1.0 / (1.0 + float(dist))
        weight[key] = max(weight.get(key, w), w)   # equal both ways (symmetric d2); max keeps it order-free
    g = nx.Graph()
    g.add_nodes_from(range(len(ids)))
    g.add_weighted_edges_from(((u, v, w) for (u, v), w in sorted(weight.items())), weight="weight")
    return g, ids


def detect(g: nx.Graph, algorithm: str, *, seed: int = SEED, resolution: float = RESOLUTION) -> list[set[int]]:
    if algorithm == "louvain":
        return nx.community.louvain_communities(g, weight="weight", resolution=resolution, seed=seed)
    if algorithm == "leiden":
        return nx.community.leiden_communities(g, weight="weight", resolution=resolution, seed=seed,
                                               metric=LEIDEN_METRIC, theta=LEIDEN_THETA)
    raise ValueError(f"algorithm {algorithm!r}: expected one of {', '.join(ALGORITHMS)}")


def cohort_ids(communities: list[set[int]], ids: list[str], algorithm: str) -> dict[str, str]:
    """renewal_id -> '<algorithm>-NN', NN = rank by size (descending), then smallest member id."""
    ordered = sorted(communities, key=lambda c: (-len(c), min(ids[i] for i in c)))
    width = max(2, len(str(len(ordered))))
    return {ids[i]: f"{algorithm}-{rank:0{width}d}" for rank, c in enumerate(ordered, start=1) for i in c}


def plan_purity(communities: list[set[int]], plans: list[str]) -> float:
    """Share of reference renewals whose cohort's majority plan is their own (1.0: blocked by plan)."""
    total = sum(len(c) for c in communities)
    majority = sum(pd.Series([plans[i] for i in c]).value_counts().iloc[0] for c in communities if c)
    return round(majority / total, 4) if total else 1.0


def _ids(frame: pd.DataFrame) -> pd.Index:
    """The renewal ids of a cohorts table or Renewal frame (a renewal_id column, else the index)."""
    return pd.Index(frame["renewal_id"]) if "renewal_id" in frame.columns else frame.index


def lapse_rate_cells(table: pd.DataFrame, algorithm: str, renewals: pd.DataFrame) -> frozenset[str]:
    """Cohorts whose model renewals are exactly a cell metric_lapse_rate can print (n and lapses, as of
    today), with MIN_CELL (metrics) or more of them.

    ``renewals``: the build's Renewal rows (renewal_id column or index) with LAPSE_RATE_COLUMNS. A cohort's
    tightest metrics cell keeps every value its members share (plan, first renewal after a cut, incident
    exposure, overage off) and the range of their limit_hits_14d: plan, first-after and the range are
    filters and the two other flags group_by keys, so metric_lapse_rate prints that cell, and every cell
    it can print around the cohort contains it. The cohort IS a printable cell exactly when its tightest
    cell holds no other model renewal.
    """
    model = renewals.set_index("renewal_id") if "renewal_id" in renewals.columns else renewals
    model = model[model["route"] == spec.REFERENCE_ROUTE]
    ref = table[table["is_reference"].astype(bool) & table[algorithm].notna()]
    out = set()
    for cid, members in ref.groupby(algorithm):
        rows = model.loc[model.index.intersection(_ids(members))]
        if len(rows) < metrics.MIN_CELL or len(rows) != len(members):
            continue
        cell = model
        for key in ("plan_tier", *metrics.FLAG_KEYS):
            values = rows[key].unique()
            if len(values) == 1:
                cell = cell[cell[key] == values[0]]
        hits = cell["limit_hits_14d"]
        cell = cell[(hits >= rows["limit_hits_14d"].min()) & (hits <= rows["limit_hits_14d"].max())]
        if len(cell) == len(rows):
            out.add(str(cid))
    return frozenset(out)


def withheld_cohorts(table: pd.DataFrame, algorithm: str, renewals: pd.DataFrame | None = None) -> frozenset[str]:
    """Cohorts whose model-renewal count is withheld: under MIN_CELL, plus their complements.

    ``table`` has the columns of cohorts.parquet (``algorithm``, plan_tier, is_reference). The
    population templates publish each plan's model totals (n and lapses, as of today), so the plan
    total minus its published cohorts is the withheld cohorts of the plan taken together: with one
    withheld cohort that gives it back, and withheld cohorts holding fewer than MIN_CELL renewals
    together are a small cell themselves. While a plan has a published cohort and either, its
    smallest published cohort (by size, then id) is withheld too. A plan whose cohorts are all
    withheld has nothing left to pair them with (they are the plan's population cell).
    With ``renewals`` (the build's Renewal rows, LAPSE_RATE_COLUMNS), a withheld cohort that is exactly a
    cell metric_lapse_rate prints (lapse_rate_cells) counts as published in that arithmetic: its n and
    lapses are printed there, so it hides nothing beside the others.
    """
    ref = table[table["is_reference"].astype(bool) & table[algorithm].notna()]
    sizes = {str(c): int(n) for c, n in ref.groupby(algorithm).size().items()}
    groups: dict[str, list[str]] = {}
    in_plan: dict[tuple[str, str], int] = {}
    for (cid, plan), n in ref.groupby([algorithm, "plan_tier"]).size().items():
        groups.setdefault(str(plan), []).append(str(cid))
        in_plan[(str(cid), str(plan))] = int(n)
    printed = lapse_rate_cells(table, algorithm, renewals) if renewals is not None else frozenset()
    hidden = {c for c, n in sizes.items() if n < MIN_CELL}
    changed = True
    while changed:
        changed = False
        for plan in sorted(groups):
            members = groups[plan]
            shown = sorted((sizes[c], c) for c in members if c not in hidden)
            inside = [c for c in members if c in hidden and c not in printed]
            together = sum(in_plan[(c, plan)] for c in inside)
            if shown and inside and (len(inside) == 1 or together < MIN_CELL):
                hidden.add(shown[0][1])
                changed = True
    return frozenset(hidden)


def assigned_cells(table: pd.DataFrame, algorithm: str) -> dict[str, int | None]:
    """cohort -> its assigned non-reference count, None when withheld: under MIN_CELL, plus complements.

    The population tools print the non-reference renewals of the build (routes) and of each plan
    (metric_route_counts(plan_tier=...): its cancel_flow, dunning, score_today and pending counts, the
    current ones exact by design), and cohorts nest inside plans. So the build total, and each plan's
    total, minus its published cells is its hidden cells taken together: while exactly one cell of the
    build or of a plan is hidden, or its hidden cells hold 1 to MIN_CELL - 1 renewals together, the
    smallest published cell of that group is hidden too (0 names no renewal).
    """
    labels = [str(c) for c in sorted(table[algorithm].dropna().unique())]
    others = table[~table["is_reference"].astype(bool) & table[algorithm].notna()]
    counts = dict.fromkeys(labels, 0) | {str(c): int(n) for c, n in others[algorithm].value_counts().items()}
    plans: dict[str, list[str]] = {}
    for (cid, plan), _n in table[table[algorithm].notna()].groupby([algorithm, "plan_tier"]).size().items():
        plans.setdefault(str(plan), []).append(str(cid))
    share = {(str(c), str(p)): int(n) for (c, p), n in others.groupby([algorithm, "plan_tier"]).size().items()}
    groups = [(None, labels), *sorted(plans.items())]
    out = {c: _cell(n) for c, n in counts.items()}
    changed = True
    while changed:
        changed = False
        for plan, members in groups:
            hidden = [c for c in members if out[c] is None]
            shown = sorted((counts[c], c) for c in members if out[c] is not None)
            together = sum(counts[c] if plan is None else share.get((c, plan), 0) for c in hidden)
            if shown and hidden and (len(hidden) == 1 or 0 < together < MIN_CELL):
                out[shown[0][1]] = None
                changed = True
    return out


def published_sizes(table: pd.DataFrame, algorithm: str, renewals: pd.DataFrame | None = None) -> list[int | None]:
    """The manifest's size list: published sizes largest first, then one None per withheld cohort
    (a null's position would bound the withheld size between its neighbours)."""
    ref = table[table["is_reference"].astype(bool) & table[algorithm].notna()]
    withheld = withheld_cohorts(table, algorithm, renewals)
    shown = sorted((int(n) for c, n in ref.groupby(algorithm).size().items() if c not in withheld), reverse=True)
    return [*shown, *([None] * len(withheld))]


def assign(renewals: pd.DataFrame, similar: pd.DataFrame, *, seed: int = SEED,
           resolution: float = RESOLUTION) -> tuple[pd.DataFrame, dict]:
    """(cohorts table, per-algorithm stats) for one build's Renewal and SIMILAR_TO tables."""
    ren = renewals.sort_values("renewal_id", kind="mergesort").reset_index(drop=True)
    g, ids = reference_graph(ren, similar)
    plan_of = ren.set_index("renewal_id")["plan_tier"]
    plans = [plan_of[i] for i in ids]
    labels: dict[str, dict[str, str]] = {}
    stats: dict[str, dict] = {}
    for algorithm in ALGORITHMS:
        t0 = time.perf_counter()
        comms = detect(g, algorithm, seed=seed, resolution=resolution)
        seconds = time.perf_counter() - t0
        labels[algorithm] = cohort_ids(comms, ids, algorithm)
        stats[algorithm] = {
            "function": f"networkx.community.{algorithm}_communities",
            **({"metric": LEIDEN_METRIC, "theta": LEIDEN_THETA} if algorithm == "leiden" else {}),
            "communities": len(comms),
            "modularity": round(float(nx.community.modularity(g, comms, weight="weight")), 6),
            "sizes": None,                       # set below from the table: small cells withheld
            "plan_purity": plan_purity(comms, plans),
            "seconds": round(seconds, 2),
        }
    ref = set(ids)
    rank1 = similar[similar["rank"] == 1].set_index("src")["dst"]
    rows = []
    for rid, plan in zip(ren["renewal_id"], ren["plan_tier"], strict=True):
        if rid in ref:
            via, how = None, ASSIGNED_MEMBER
        else:
            via = rank1.get(rid)
            how = ASSIGNED_NEAREST if via in ref else ASSIGNED_NONE
            via = via if how == ASSIGNED_NEAREST else None
        key = rid if how == ASSIGNED_MEMBER else via
        rows.append({"renewal_id": rid, "plan_tier": plan, "is_reference": rid in ref,
                     **{a: (labels[a].get(key) if key else None) for a in ALGORITHMS},
                     "assigned_via": how, "via_renewal_id": via, "spec_version": COHORT_SPEC_VERSION})
    table = pd.DataFrame(rows, columns=[f.name for f in SCHEMA])
    rates = ren if set(LAPSE_RATE_COLUMNS) <= set(ren.columns) else None     # build_cohorts reads them
    for algorithm in ALGORITHMS:
        stats[algorithm]["sizes"] = published_sizes(table, algorithm, rates)
    graph_stats ={"nodes": g.number_of_nodes(), "undirected_edges": g.number_of_edges(),
                   "components": nx.number_connected_components(g)}
    return table, {"graph": graph_stats, "algorithms": stats,
                   "assigned": {k: int(v) for k, v in sorted(table["assigned_via"].value_counts().items())}}


def write_table(table: pd.DataFrame, path: Path) -> str:
    """Explicit schema, no pandas metadata, the business builder's writer options; returns sha256."""
    from .build import PARQUET_OPTIONS  # writer options only

    arrays = [pa.array(table[f.name].astype(object).where(table[f.name].notna(), None).tolist(), type=f.type)
              for f in SCHEMA]
    pq.write_table(pa.Table.from_arrays(arrays, schema=SCHEMA), path, **PARQUET_OPTIONS)
    return mf.sha256_file(path)


# --------------------------------------------------------------------------- build
def _graph_root_of(build_dir: Path) -> Path | None:
    """The graph root when ``build_dir`` is <root>/<profile>/builds/<id> (then the build lock applies)."""
    build_dir = build_dir.resolve()
    if build_dir.parent.name == "builds" and spec.is_profile(build_dir.parent.parent.name):
        return build_dir.parents[2]
    return None


def build_cohorts(build_dir: str | os.PathLike, *, seed: int = SEED, resolution: float = RESOLUTION,
                  lock_timeout: float = 600.0, log=print) -> tuple[Path, dict]:
    """Detect the cohorts of one business build and write cohorts.parquet + manifest.json["cohorts"].

    Deterministic: the same build, seed and resolution give byte-identical Parquet (an unchanged
    file is kept). Takes the graph root's build lock when ``build_dir`` is a profile build, and reads
    the manifest and the two Parquet inputs under it: the recorded ``inputs_sha256`` are the sha256
    of the bytes the cohorts were computed from, which must be the ones the build's manifest pins
    (CohortsUnavailable otherwise: the build was modified after it was made).
    """
    t0 = time.perf_counter()
    bdir = Path(build_dir).absolute()
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError(f"seed {seed!r}: expected an integer")
    if not (isinstance(resolution, (int, float)) and resolution > 0):
        raise ValueError(f"resolution {resolution!r}: expected a positive number")
    root = _graph_root_of(bdir)
    lock = store.BuildLock(root, timeout=lock_timeout, log=log) if root else contextlib.nullcontext()
    with lock:
        try:
            man = mf.read_manifest(bdir)
        except (OSError, ValueError) as e:
            raise CohortsUnavailable(f"{bdir} is not a graph build (no readable manifest.json: {e}); run make "
                                     f"graph-build first") from e
        raw = {rel: (bdir / rel).read_bytes() for rel in INPUT_FILES}       # hashed and parsed from the same bytes
        inputs = {rel: hashlib.sha256(data).hexdigest() for rel, data in raw.items()}
        pinned = {rel: man.get("files", {}).get(rel, {}).get("sha256") for rel in INPUT_FILES}
        differ = sorted(rel for rel in INPUT_FILES if inputs[rel] != pinned[rel])
        if differ:
            raise CohortsUnavailable(f"{bdir}: {', '.join(differ)} differ from the sha256 its manifest.json pins (the "
                                     f"build was changed after it was made); rebuild it (make graph-build) first")
        renewals = pq.read_table(pa.BufferReader(raw[INPUT_FILES[0]]),
                                 columns=["renewal_id", *dict.fromkeys(LAPSE_RATE_COLUMNS)]).to_pandas()
        similar = pq.read_table(pa.BufferReader(raw[INPUT_FILES[1]]),
                                columns=["src", "dst", "rank", "dist"]).to_pandas()
        table, stats = assign(renewals, similar, seed=seed, resolution=float(resolution))
        final = bdir / COHORTS_FILE
        tmp = bdir / f".{COHORTS_FILE}.tmp-{os.getpid()}"
        try:
            digest = write_table(table, tmp)
            unchanged = final.is_file() and mf.sha256_file(final) == digest
            if unchanged:
                tmp.unlink()
            else:
                os.replace(tmp, final)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        # timings stay out of the record (they vary run to run); built_at says when it was made
        record = {
            "spec": COHORT_SPEC_VERSION, "in_contract": False, "file": COHORTS_FILE, "rows": len(table),
            "sha256": digest, "business_build_id": man.get("business_build_id"), "inputs_sha256": inputs,
            "library": "networkx", "library_version": nx.__version__, "seed": seed, "resolution": float(resolution),
            "weight": WEIGHT_RULE, "reference": f"route = '{spec.REFERENCE_ROUTE}' (the SIMILAR_TO candidates)",
            "graph": stats["graph"],
            "algorithms": {a: {k: v for k, v in s.items() if k != "seconds"} for a, s in stats["algorithms"].items()},
            "default_algorithm": DEFAULT_ALGORITHM, "assigned": stats["assigned"],
            "code_sha256": mf.sha256_file(spec.repo_root() / _CODE), "built_at": mf.utc_now(),
        }
        old = man.get(MANIFEST_KEY) or {}
        if unchanged and {k: v for k, v in old.items() if k != "built_at"} == \
                {k: v for k, v in record.items() if k != "built_at"}:
            record = old
        else:
            mf.write_manifest(bdir, {**mf.read_manifest(bdir), MANIFEST_KEY: record})
    _load.cache_clear()
    took = time.perf_counter() - t0
    log(f"    {'unchanged' if unchanged else 'wrote'} {final} ({len(table):,} rows, sha256 {digest[:12]}, "
        f"{took:.1f} s); "
        + "; ".join(f"{a} {s['communities']} cohorts, modularity {s['modularity']:.4f} ({s['seconds']} s)"
                    for a, s in stats["algorithms"].items()))
    return final, record


# --------------------------------------------------------------------------- questions
def _build_dir(ctx) -> Path:
    if isinstance(ctx, (str, os.PathLike)):
        return Path(ctx)
    bdir = getattr(ctx, "build_dir", None)
    if bdir is None:
        raise ValueError("ctx must be a build directory or an object with build_dir")
    return Path(bdir)


def _stamp(path: Path) -> tuple:
    st = path.stat()
    return (st.st_mtime_ns, st.st_size)


def _frames(build_dir: Path) -> dict:
    path = build_dir / COHORTS_FILE
    if not path.is_file():
        raise CohortsUnavailable(f"no {COHORTS_FILE} in {build_dir}: run python scripts/build_graph_cohorts.py "
                                 f"--build {build_dir} (or --profile <profile>)")
    return _load(str(build_dir), _stamp(path))


@functools.lru_cache(maxsize=4)
def _load(build_dir: str, _stamp_key: tuple) -> dict:
    b = Path(build_dir)
    cohorts = pq.read_table(b / COHORTS_FILE).to_pandas().set_index("renewal_id")
    cols = ["renewal_id", "plan_tier", "route", "as_of", "churned", "outcome_observed_on", *spec.FEATURES]
    ren = pq.read_table(b / "parquet/nodes_Renewal.parquet", columns=cols).to_pandas().set_index("renewal_id")
    for c in ("as_of", "outcome_observed_on"):
        ren[c] = pd.to_datetime(ren[c])
    scaler = pq.read_table(b / spec.SCALER_FILE).to_pandas()
    try:
        summary = mf.read_manifest(b).get(MANIFEST_KEY) or {}
    except (OSError, ValueError):
        summary = {}
    return {"cohorts": cohorts, "renewals": ren, "scaler": scaler, "summary": summary}


def _absent(value):
    """Small models send "", "null" or "None" for an optional argument they mean to leave out."""
    return None if isinstance(value, str) and value.strip().lower() in _JUNK else value


def _check_algorithm(algorithm) -> str:
    if algorithm not in ALGORITHMS:
        raise ValueError(f"invalid algorithm {algorithm!r}: expected one of {', '.join(ALGORITHMS)}")
    return algorithm


def _cell(n: int) -> int | None:
    return n if n >= MIN_CELL else None


def suppress_cells(counts: dict[str, int], total_published: bool) -> dict[str, int | None]:
    """Cells under MIN_CELL become None. When the total is published, total minus the published cells
    is the hidden cells taken together: while exactly one cell is hidden (that gives it back), or the
    hidden ones hold 1 to MIN_CELL - 1 renewals together (a small cell; 0 names no renewal), the
    smallest published cell is hidden too."""
    out = {k: _cell(v) for k, v in counts.items()}
    while total_published:
        hidden = [k for k, v in out.items() if v is None]
        shown = sorted((v, k) for k, v in out.items() if v is not None)
        together = sum(counts[k] for k in hidden)
        if not (shown and hidden and (len(hidden) == 1 or 0 < together < MIN_CELL)):
            break
        out[shown[0][1]] = None
    return out


def _small_cells(f: dict, algorithm: str) -> tuple[frozenset[str], dict[str, int | None]]:
    """(withheld cohorts, assigned non-reference cell per cohort) of one algorithm, memoised per
    frames and MIN_CELL (withheld_cohorts with the build's renewals, assigned_cells)."""
    key = (algorithm, MIN_CELL)
    memo = f.setdefault("small_cells", {})
    if key not in memo:
        table = f["cohorts"]
        memo[key] = (withheld_cohorts(table, algorithm, f["renewals"]), assigned_cells(table, algorithm))
    return memo[key]


def top_features(members: pd.DataFrame, scaler: pd.DataFrame, k: int = TOP_FEATURES) -> list[dict]:
    """The k features whose member mean z-score (persisted SIMILAR_TO scaler) is furthest from 0."""
    out = []
    for f, mean, std in zip(scaler["feature"], scaler["mean"], scaler["std"], strict=True):
        x = members[f].to_numpy(dtype=np.float64)
        z = float(((x - mean) / std).mean()) if std > 0 else 0.0
        out.append({"feature": f, "mean_z": round(z, 3), "direction": "higher" if z > 0 else "lower",
                    "cohort_mean": round(float(x.mean()), 4), "population_mean": round(float(mean), 4)})
    out.sort(key=lambda r: (-abs(r["mean_z"]), r["feature"]))
    return [r for r in out if r["mean_z"] != 0][:k]


def cohort_name(plan_mix: dict[str, int], features: list[dict]) -> str:
    """A deterministic descriptive name: the plan, then the two most distinguishing features."""
    plan = max(sorted(plan_mix), key=lambda p: plan_mix[p]) if plan_mix else "mixed"
    parts = [f"{f['direction']} {f['feature']}" for f in features[:2]]
    return f"{plan}: " + (", ".join(parts) if parts else "no distinguishing feature")


def _describe(f: dict, cohort_id: str, algorithm: str, *, source: pd.Series | None = None,
              source_id: str | None = None) -> tuple[dict, list[str]]:
    """The summary of one cohort; with a source renewal, its outcome-visibility rule applies."""
    cohorts, ren = f["cohorts"], f["renewals"]
    members = cohorts.index[cohorts[algorithm] == cohort_id]
    member_rows = ren.loc[members]
    ref = member_rows[member_rows["route"] == spec.REFERENCE_ROUTE]
    counted = ref
    visibility, not_yet, excluded_self = "today", 0, False
    if source is not None:
        excluded_self = source_id in counted.index
        counted = counted.drop(index=source_id, errors="ignore")
        if source["route"] not in CURRENT_ROUTES:
            visibility = "source_as_of"
            seen = counted["outcome_observed_on"].notna() & (counted["outcome_observed_on"] <= source["as_of"])
            not_yet = int((~seen).sum())
            counted = counted[seen]
    plan_counts = {p: int(v) for p, v in sorted(ref["plan_tier"].value_counts().items())}
    n, lapses = len(counted), int(counted["churned"].sum())
    withheld, assigned_cells = _small_cells(f, algorithm)
    size_hidden = cohort_id in withheld          # under MIN_CELL, or the complement of one in its plan
    # The members today's cell counts and this view does not: not yet observed at the source's as_of,
    # and the named renewal itself (0 in today's view). Today's cell is published (unless the size is
    # withheld), so this view's lapses subtracted from it would print the outcome of those renewals.
    behind = not_yet + int(excluded_self)
    if size_hidden:
        # every view: today's n is the size, and a past view's n + not_yet + self is the size too
        # (the plan's population totals would then give a smaller withheld cohort back)
        suppressed, why = True, "withheld"
    elif n < MIN_CELL:
        suppressed, why = True, "small"
    elif 0 < behind < MIN_CELL:
        suppressed, why = True, "behind"
    else:
        suppressed, why = False, None
    # model_renewals = n + not_yet + excluded_self: not_yet is hidden with n (else n comes back);
    # otherwise it is the difference of published cells, so it is published exactly.
    hide_not_yet = suppressed
    # a withheld cohort prints no member statistic (mean * size is near a whole number for an integer
    # feature: it pinned the size) and one name whether it is small or a complement
    features = [] if size_hidden or len(ref) < MIN_CELL else top_features(ref, f["scaler"])
    summary = f["summary"].get("algorithms", {}).get(algorithm, {})
    plan = ", ".join(plan_counts) or "no plan"
    data: dict[str, Any] = {
        "cohort_id": cohort_id, "algorithm": algorithm,
        "name": f"{plan}: {WITHHELD_NAME}" if size_hidden else cohort_name(plan_counts, features),
        "size": {"model_renewals": None if size_hidden else len(ref),
                 "assigned_non_reference": assigned_cells.get(cohort_id)},
        "outcomes": {"population": "model renewals (route = model): voluntary lapse or renewed",
                     "visibility": visibility, "n": None if suppressed else n,
                     "voluntary_lapses": None if suppressed else lapses,
                     "rate": None if suppressed else round(lapses / n, 4),
                     "wilson_95": None if suppressed else wilson(lapses, n),
                     "not_yet_observed": None if hide_not_yet else not_yet,
                     "excluded_named_renewal": excluded_self, "suppressed": suppressed},
        # the plan cells sum to the size: all withheld with it
        "plan_mix": dict.fromkeys(plan_counts) if size_hidden else suppress_cells(plan_counts, True),
        "top_features": features,
        "provenance": {"spec": COHORT_SPEC_VERSION, "library": f["summary"].get("library", "networkx"),
                       "library_version": f["summary"].get("library_version"), "seed": f["summary"].get("seed"),
                       "resolution": f["summary"].get("resolution"), "weight": f["summary"].get("weight"),
                       "modularity": summary.get("modularity"), "communities": summary.get("communities"),
                       "business_build_id": f["summary"].get("business_build_id"), "in_contract": False},
    }
    caveats = [CAVEAT,
               "Descriptive context, not a risk estimate: cohorts group renewals by feature similarity "
               "(SIMILAR_TO); subscriptions have no relationships to each other.",
               "Rates count model-routed renewals only (voluntary lapse label); plan purity is 1.0 by construction "
               "(SIMILAR_TO is blocked by plan).",
               f"Outside the graph contract: {data['provenance']['library']} "
               f"{data['provenance']['library_version']} {algorithm}, seed {data['provenance']['seed']}; another "
               "seed or library version can move renewals between cohorts."]
    if source is not None and visibility == "source_as_of":
        caveats.append(f"Only member outcomes observed on or before {source['as_of'].date().isoformat()} (the named "
                       f"renewal's as_of) are counted, and never the named renewal's own outcome.")
    elif source is not None:
        caveats.append("The named renewal is current (its as_of is the latest T-7): member outcomes are as of today; "
                       "its own outcome is never counted.")
    if why == "withheld":       # one caveat for a small cohort and its complement: it never says which
        caveats.append(f"This cohort is withheld (complementary suppression): it has fewer than {MIN_CELL} model "
                       f"renewals, or it is withheld with such a cohort of its plan, whose size the plan's population "
                       f"totals would otherwise give back by subtraction. Its size, plan mix, top features, n, "
                       f"lapses, rate, interval and the not-yet-observed count are withheld in every view.")
    elif why == "small":
        caveats.append(f"Fewer than {MIN_CELL} model renewals are counted: n, lapses, rate, interval and the "
                       f"not-yet-observed count (which would give n back by subtraction) are suppressed.")
    elif why == "behind":
        caveats.append(f"n, lapses, rate, interval and the not-yet-observed count are withheld although {MIN_CELL} "
                       f"or more model renewals are counted (complementary suppression): this view differs from the "
                       f"cohort's published counts as of today by fewer than {MIN_CELL} renewals (the members not "
                       f"yet observed at this as_of, and the named renewal itself), whose lapses would come back by "
                       f"subtraction.")
    elif visibility == "source_as_of":
        caveats.append("These counts are lower bounds of the cohort's counts as of today: they leave out the members "
                       "not yet observed at this as_of and the named renewal itself.")
    if any(v is None for v in data["plan_mix"].values()) or data["size"]["assigned_non_reference"] is None:
        caveats.append(f"Cells with fewer than {MIN_CELL} renewals, and any cell that would give one back by "
                       f"subtraction, are suppressed (null).")
    if features:
        caveats.append("Top features compare the members' T-7 feature means with the reference population "
                       "(z-scores of the persisted SIMILAR_TO scaler); members may be later renewals than the "
                       "named one (feature-space information only).")
    return data, caveats


def cohort_summary(ctx, cohort_id=None, renewal_id=None, algorithm=None) -> tuple[dict, list[str]]:
    """One cohort, by id (``leiden-03``) or by a renewal it contains; returns (data, caveats).

    ValueError on invalid input (both or neither of cohort_id / renewal_id, a malformed or unknown
    id, an algorithm that contradicts the cohort id). CohortsUnavailable without cohorts.parquet.
    """
    cohort_id, renewal_id, algorithm = _absent(cohort_id), _absent(renewal_id), _absent(algorithm)
    if (cohort_id is None) == (renewal_id is None):
        raise ValueError("give exactly one of cohort_id (for example leiden-01) or renewal_id (for example "
                         "sub_santosh:2026-10-07)")
    f = _frames(_build_dir(ctx))
    cohorts = f["cohorts"]
    if cohort_id is not None:
        m = COHORT_ID_RE.fullmatch(cohort_id) if isinstance(cohort_id, str) else None
        if not m:
            raise ValueError(f"invalid cohort_id {cohort_id!r}: expected <algorithm>-<NN> with algorithm one of "
                             f"{', '.join(ALGORITHMS)}, for example leiden-01")
        if algorithm is not None and _check_algorithm(algorithm) != m.group(1):
            raise ValueError(f"cohort_id {cohort_id!r} is a {m.group(1)} cohort but algorithm={algorithm!r}")
        algorithm = m.group(1)
        known = sorted(cohorts[algorithm].dropna().unique())
        if cohort_id not in known:
            near = difflib.get_close_matches(cohort_id, known, n=3, cutoff=0.6)
            raise ValueError(f"unknown cohort_id {cohort_id!r}: this build has {algorithm}-01 .. {known[-1]}"
                             + (f" (did you mean {', '.join(near)}?)" if near else ""))
        return _describe(f, cohort_id, algorithm)
    algorithm = _check_algorithm(algorithm or DEFAULT_ALGORITHM)
    if not isinstance(renewal_id, str) or not RENEWAL_ID_RE.fullmatch(renewal_id):
        raise ValueError(f"invalid renewal_id {renewal_id!r}: expected sub_<id>:<YYYY-MM-DD>, for example "
                         f"sub_santosh:2026-10-07")
    if renewal_id not in cohorts.index:
        raise ValueError(f"unknown renewal_id {renewal_id!r}: not in this build")
    row = cohorts.loc[renewal_id]
    cid = row[algorithm]
    if cid is None or (isinstance(cid, float) and math.isnan(cid)):
        raise ValueError(f"renewal {renewal_id!r} has no cohort (no reference renewal in its plan block)")
    data, caveats = _describe(f, cid, algorithm, source=f["renewals"].loc[renewal_id], source_id=renewal_id)
    via = row["via_renewal_id"]
    data = {"named_renewal": {"renewal_id": renewal_id, "is_reference": bool(row["is_reference"]),
                              "assigned_via": row["assigned_via"],
                              "via_renewal_id": via if isinstance(via, str) else None}, **data}
    if row["assigned_via"] == ASSIGNED_NEAREST:
        caveats.append("The named renewal is not in the reference set: it takes the cohort of its nearest reference "
                       "renewal (SIMILAR_TO rank 1).")
    return data, caveats


def cohort_list(ctx, algorithm: str = DEFAULT_ALGORITHM) -> tuple[dict, list[str]]:
    """Every cohort of one algorithm, largest first: id, name, sizes, lapse rate (as of today)."""
    algorithm = _check_algorithm(_absent(algorithm) or DEFAULT_ALGORITHM)
    f = _frames(_build_dir(ctx))
    rows = []
    for cid in sorted(f["cohorts"][algorithm].dropna().unique()):
        d, _ = _describe(f, cid, algorithm)
        rows.append({"cohort_id": cid, "name": d["name"], "model_renewals": d["size"]["model_renewals"],
                     "voluntary_lapses": d["outcomes"]["voluntary_lapses"], "rate": d["outcomes"]["rate"],
                     "wilson_95": d["outcomes"]["wilson_95"], "suppressed": d["outcomes"]["suppressed"]})
    summary = f["summary"].get("algorithms", {}).get(algorithm, {})
    data = {"algorithm": algorithm, "cohorts": rows, "communities": len(rows),
            "modularity": summary.get("modularity"), "seed": f["summary"].get("seed"),
            "library": f"networkx {f['summary'].get('library_version')}"}
    caveats = [CAVEAT, "Descriptive context, not a risk estimate; rates count model-routed renewals only.",
               f"Cohorts with fewer than {MIN_CELL} model renewals are suppressed (null), and so is the smallest "
               f"other cohort of the same plan when exactly one of its cohorts is, or when its suppressed cohorts "
               f"hold fewer than {MIN_CELL} together (else the plan's population totals would give them back by "
               f"subtraction; a suppressed cohort that is exactly a metric_lapse_rate cell does not count, since "
               f"that tool prints it). Withheld cohorts read alike, small or not."]
    return data, caveats


TOOLS = {"cohort_summary": cohort_summary, "cohort_list": cohort_list}

__all__ = [
    "ALGORITHMS",
    "CAVEAT",
    "COHORTS_FILE",
    "COHORT_SPEC_VERSION",
    "MIN_CELL",
    "SEED",
    "TOOLS",
    "CohortsUnavailable",
    "assign",
    "assigned_cells",
    "build_cohorts",
    "cohort_list",
    "cohort_summary",
    "lapse_rate_cells",
    "published_sizes",
    "reference_graph",
    "suppress_cells",
    "wilson",
    "withheld_cohorts",
]
