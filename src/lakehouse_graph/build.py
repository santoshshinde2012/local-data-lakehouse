"""Build the renewal graph (renewal-graph/v1) as deterministic Parquet, then load Ladybug.

One source of truth: the graph is computed in process from the repo's own pandas gold
twin, ``scripts/build_churn_gold_local.py`` (loaded by file path, never modified, never
re-read from the audit CSV). ``silver()`` gives the events, ``gold(s, today)`` gives one
row per renewal with its T-7 features, outcome and route.

Pure functions:
  load_gold_twin(sample_dir)      importlib load of the gold script, pointed at sample_dir
  build_tables(silver, gold, today[, consts]) -> {label/type: DataFrame} (+ scaler, cut rows)
  similar_to(renewals) -> (edges, scaler)      SIMILAR_TO per spec similar_to/renewal-v1
  similar_to_full(renewals) -> (edges, scaler, cut, diagnostics); fit_scaler / zscore / knn
  write_parquet(df, path, schema) -> sha256   explicit arrow schema, no pandas metadata

Orchestration:
  build_profile(profile, sample_dir, export_dir, graph_root) -> (build_dir, manifest)
    lock -> identity -> tables -> Parquet (tmp dir) -> Ladybug (subprocess, capped pool)
    -> manifest -> atomic rename to builds/<business_build_id> -> <profile>/latest
    Same id + byte-identical Parquet: the build is kept and its manifest pins (exports,
    guarded files, seed status) are refreshed (repin), so regenerated exports never need a
    forced rebuild. A replacement (--rebuild, or a build that lost its graph.lbdb) is swapped
    in with one atomic exchange (store.replace_dir), so ``current`` / ``latest`` never dangle.
    When its Parquet is byte-identical it inherits what the carry-over registry names
    (CARRY_OVER; later phases add theirs with register_carry_over()): contract.json, the
    lineage tables (lineage/ + lineage.lbdb, only while fresh for the current code) and
    cohorts.parquet (only while its manifest record matches the file, the cohorts code and the
    NetworkX version), each with its manifest.json key. The log names anything dropped, and why.
  prepare_sample(profile, seed, n_users, graph_root)   what `make graph-sample` does: the user's
    generator and gold script, unchanged, writing into $GRAPH_ROOT/<profile>/{sample,export}
    (tiny: exports only; inject: prepare_inject_profile; default: refused)
  prepare_inject_profile(graph_root)   the `inject` profile's bronze: a scratch copy of the tiny
    fixture with exactly one poisoned user_name (spec.INJECT_*), plus its exports

Dates: renewal-graph/v1 stores every event as a DATE. Before the gold twin runs, every
date-grained bronze column (DATE_GRAINED_SOURCES) is read as text: a value that is not
YYYY-MM-DD (optionally followed by a midnight time with no offset, which is the same date) --
a time of day, a UTC offset or 'Z' (even at midnight: which day would it be?), another format,
an impossible calendar date such as 2026-02-30 -- fails the build with mixed_dates_message(),
which names the file, the column, the line and an example; never a traceback, never pandas'
own hint, never a silent truncation or conversion. limit_events.hit_at (TIMESTAMP_SOURCES) is
the one timestamp: the same pre-pass requires a naive YYYY-MM-DD[ HH:MM[:SS]] with a real day
and clock (no offset, no 'Z'), and the graph keeps the gold rule's to_date(hit_at).
check_date_grain() applies the time-of-day rule to an in-memory silver (e.g. from Iceberg) and
inside build_tables().

Besides the binding layout (parquet/, graph.lbdb, manifest.json, similar_to_scaler.parquet)
a build holds similar_to_cut.parquet (rank k+1 rows, tie diagnostics; not loaded) and
contract.json (written by scripts/check_graph_contract.py; promote reads it).

Run it through scripts/build_graph_local.py (make graph-build).
"""
from __future__ import annotations

import csv
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import spec, store

GOLD_SCRIPT = mf.GOLD_SCRIPT
CUT_FILE = "similar_to_cut.parquet"  # rank k+1 candidate per source (tie diagnostics; not loaded)


class CarryOver(NamedTuple):
    """What a phase adds to a build directory, handed to a byte-identical replacement (see CARRY_OVER)."""
    entries: tuple[str, ...]                                  # files or directories in the build dir
    manifest_key: str | None = None                           # its summary key in manifest.json, carried with it
    still_valid: Callable[[Path], str | None] | None = None   # (old build dir) -> None, or why it no longer holds


# The carry-over registry: what a replaced build hands to its replacement when the rebuilt Parquet
# is byte-identical (a --rebuild, or a build that lost its graph.lbdb). Each entry (with its
# database sidecars, ``<entry>.*``) is copied into the new directory before the atomic swap, with
# its manifest.json summary key, unless its ``still_valid`` check gives a reason it no longer holds.
# Anything else the old build held is dropped, and the builder names it (and the reason).
# The binding build layout is registered below; a later phase that adds an artefact to a build
# directory registers it too: ``build.register_carry_over(name, entries, manifest_key=...,
# still_valid=...)`` (a registration under an existing name replaces it, e.g. to add a check).
CARRY_OVER: dict[str, CarryOver] = {}


def register_carry_over(name: str, entries: tuple[str, ...] | list[str], *, manifest_key: str | None = None,
                        still_valid: Callable[[Path], str | None] | None = None) -> CarryOver:
    """Register (or replace) a phase's build-directory artefacts in CARRY_OVER; returns the entry."""
    reserved = {mf.MANIFEST_FILE, store.DB_FILE, store.PID_DIR, "parquet", spec.SCALER_FILE, CUT_FILE}  # the builder's
    entries = tuple(entries)
    bad = [e for e in entries if not e or "/" in e or e.startswith(".") or e in reserved]
    if bad or not entries:
        raise ValueError(f"carry-over {name!r}: entries must be plain names in the build directory that the builder "
                         f"does not write itself, got {entries!r}")
    CARRY_OVER[name] = item = CarryOver(entries, manifest_key, still_valid)
    return item


def _lineage_still_valid(old: Path) -> str | None:
    """The lineage tables still describe the repo: the files they were extracted from and the lineage
    code are unchanged (lineage.build.is_fresh, the lineage contract's own integrity test)."""
    try:
        from .lineage import build as lineage_build  # lazy: the lineage package imports this module
    except ImportError as e:
        return f"the lineage package cannot be imported to check it ({e})"
    try:
        man = lineage_build.read_manifest(old)
    except (OSError, ValueError) as e:
        return f"its lineage/manifest.json is unreadable ({type(e).__name__})"
    if not lineage_build.is_fresh(man):
        return "stale: a file it was extracted from, or the lineage code, changed since it was built"
    return None


COHORTS_CODE = "src/lakehouse_graph/cohorts.py"   # what made cohorts.parquet (its manifest record pins its sha256)


def _cohorts_still_valid(old: Path) -> str | None:
    """cohorts.parquet is the file its manifest record describes (sha256) and was made by the code and
    library in use now: the cohorts module unchanged (code_sha256) and the same NetworkX version
    (another version can move renewals between cohorts). Its input is the Parquet, which is
    byte-identical whenever anything is carried over. The seed and resolution are part of the
    record, so a non-default seed stays valid for what it says."""
    rec = mf.read_manifest(old).get("cohorts")
    if not isinstance(rec, dict):
        return "manifest.json has no cohorts record describing it"
    try:
        installed = importlib.metadata.version("networkx")
    except importlib.metadata.PackageNotFoundError:
        installed = None
    checks = [
        (mf.sha256_file(old / "cohorts.parquet") == rec.get("sha256"), "cohorts.parquet differs from its record"),
        (rec.get("code_sha256") == mf.sha256_file(spec.repo_root() / COHORTS_CODE),
         f"{COHORTS_CODE} changed since it was built"),
        (rec.get("library") == "networkx" and rec.get("library_version") == installed,
         f"built with {rec.get('library')} {rec.get('library_version')}, networkx {installed} is installed"),
        (isinstance(rec.get("seed"), int) and not isinstance(rec.get("seed"), bool), "its record has no integer seed"),
    ]
    return next((why for ok, why in checks if not ok), None)


# The binding build layout (IMPL §2): the contract record (store.is_strict_pass() then decides whether
# it still counts: it compares the id, the Parquet sha256 and the pinned exports), the lineage tables
# (PHASE 2b; carried only while fresh for the current code) and the cohorts table (PHASE 2d; carried
# only while its record matches the file, the cohorts code and the NetworkX version).
register_carry_over("contract", (store.CONTRACT_FILE,))
register_carry_over("lineage", ("lineage", "lineage.lbdb"), manifest_key="lineage", still_valid=_lineage_still_valid)
register_carry_over("cohorts", ("cohorts.parquet",), manifest_key="cohorts", still_valid=_cohorts_still_valid)


class _ByManifestKey(Mapping):
    """manifest.json key -> entries: a live, read-only view of the CARRY_OVER entries that have one."""

    def _items(self) -> dict[str, tuple[str, ...]]:
        return {c.manifest_key: c.entries for c in CARRY_OVER.values() if c.manifest_key}

    def __getitem__(self, key: str) -> tuple[str, ...]:
        return self._items()[key]

    def __iter__(self):
        return iter(self._items())

    def __len__(self) -> int:
        return len(self._items())


PHASE_ARTEFACTS = _ByManifestKey()  # the pre-registry name (other phases' docs cite it); register with CARRY_OVER
# Every date-grained source the graph reads: (silver table, column, bronze name). A time of day
# in any of them is a build error. limit_events.hit_at is absent on purpose: it is a timestamp
# and the graph stores silver's hit_date = to_date(hit_at), exactly as the gold rule does.
DATE_GRAINED_SOURCES = (
    ("sub_events", "event_date", "subscription_events.event_date"),
    ("invoices", "invoice_date", "invoices.invoice_date"),
    ("usage", "activity_date", "daily_usage.activity_date"),
    ("overage_settings", "changed_at", "overage_settings.changed_at"),
    ("overage_charges", "charged_at", "overage_charges.charged_at"),
    ("tickets", "created_date", "support_tickets.created_date"),
    ("incidents", "starts_on", "incidents.starts_on"),
    ("incidents", "ends_on", "incidents.ends_on"),
    ("pricing", "effective_date", "pricing_changes.effective_date"),
    ("snapshots", "snapshot_date", "subscription_snapshots.snapshot_date"),
    ("snapshots", "current_period_end", "subscription_snapshots.current_period_end"),
    ("snapshots", "started_at", "subscription_snapshots.started_at"),
)
# The one timestamp the graph reads: a naive YYYY-MM-DD[ HH:MM[:SS[.f]]] (no UTC offset, no 'Z':
# which day would it be?) whose date the gold rule to_date(hit_at) keeps. The bronze pre-pass
# checks it with the date-grained columns, so a tz-aware or impossible value is refused by name.
TIMESTAMP_SOURCES = (("limits", "hit_at", "limit_events.hit_at"),)
CUT_SCHEMA = pa.schema([pa.field("src", spec.STR, nullable=False), pa.field("dst", spec.STR, nullable=False),
                        pa.field("rank", spec.I64), pa.field("d2", spec.F64), pa.field("d2_q", spec.I64)])
PARQUET_OPTIONS = {"compression": "snappy", "use_dictionary": True, "write_statistics": True, "version": "2.6"}


class GraphBuildError(RuntimeError):
    """The inputs or the environment violate a build precondition."""


# --------------------------------------------------------------------------- gold twin
def load_gold_twin(sample_dir: str | os.PathLike):
    """Load scripts/build_churn_gold_local.py by path (the check_gold_parity.py pattern)."""
    path = spec.repo_root() / GOLD_SCRIPT
    mspec = importlib.util.spec_from_file_location("lakehouse_graph_gold_twin", path)
    if mspec is None or mspec.loader is None:
        raise GraphBuildError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    mod.SAMPLE = Path(sample_dir)  # silver() reads the module global at call time
    return mod


def gold_constants(mod) -> dict:
    return {"plans": list(mod.PLANS), "allowance": {k: int(v) for k, v in mod.ALLOWANCE.items()},
            "cap_cut": float(mod.CAP_CUT), "features": list(mod.FEATURES)}


def run_gold(mod) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.Timestamp]:
    s = mod.silver()
    check_date_grain(s)  # before gold(): a time of day would change its comparisons too
    today = s["snapshots"]["snapshot_date"].max()
    return s, mod.gold(s, today), today


# --------------------------------------------------------------------------- helpers
def _seq_ids(df: pd.DataFrame, prefix: str, order: list[str]) -> pd.Series:
    """Deterministic per-subscription ids: <prefix>:<subscription_id>:NNN in event order."""
    ordered = df.sort_values(["subscription_id", *order], kind="mergesort")
    n = ordered.groupby("subscription_id", sort=False).cumcount() + 1
    ids = prefix + ":" + ordered["subscription_id"].astype(str) + ":" + n.map("{:03d}".format)
    return ids.reindex(df.index)


def _edges(df: pd.DataFrame, rel: str, **cols) -> pd.DataFrame:
    es = spec.EDGE_SCHEMA[rel]
    out = pd.DataFrame({c: cols[c] for c, _ in es.all_columns})
    order = ["src", "rank"] if rel == "SIMILAR_TO" else ["src", "dst"] + (["event_date"] if "event_date" in out else [])
    return out.sort_values(order, kind="mergesort").reset_index(drop=True)


def _nodes(df: pd.DataFrame, label: str) -> pd.DataFrame:
    ns = spec.NODE_SCHEMA[label]
    out = df[[c for c, _ in ns.columns]].sort_values(ns.key, kind="mergesort").reset_index(drop=True)
    if out[ns.key].duplicated().any():
        raise GraphBuildError(f"{label}.{ns.key} is not unique")
    return out


# --------------------------------------------------------------------------- SIMILAR_TO
def fit_scaler(ref: pd.DataFrame, features: list[str] = spec.FEATURES) -> pd.DataFrame:
    """Population z-score (ddof=0) on the reference set, rows in renewal_id order."""
    x = ref[features].to_numpy(dtype=np.float64)
    return pd.DataFrame({"feature": features, "mean": x.mean(axis=0), "std": x.std(axis=0, ddof=0),
                         "n_ref": len(ref), "spec_version": spec.SIMILAR_TO_SPEC_VERSION})


def zscore(df: pd.DataFrame, scaler: pd.DataFrame) -> np.ndarray:
    x = df[list(scaler["feature"])].to_numpy(dtype=np.float64)
    mean, std = scaler["mean"].to_numpy(), scaler["std"].to_numpy()
    z = np.zeros_like(x)
    nz = std > 0  # std == 0 -> z = 0
    z[:, nz] = (x[:, nz] - mean[nz]) / std[nz]
    return z


def knn(src: pd.DataFrame, dst: pd.DataFrame, scaler: pd.DataFrame, k: int = spec.K, keep_extra: int = 0,
        chunk: int = 128) -> pd.DataFrame:
    """Blocked directed kNN: rank by (d2_q, dst renewal_id), self excluded.

    ``chunk`` sources are scored at a time; it only bounds memory (each row is computed
    independently, so the result is bit-identical for any chunk size: 128 keeps the seed-42
    builder near 300 MiB, 512 peaked near 440 MiB).

    Returns src, dst, rank, d2, d2_q, in_contract (rank <= k_eff) with k_eff =
    min(k, candidates_in_block - [src is a candidate]); ``keep_extra`` rows past k_eff
    are returned with in_contract=False (the rank-11 cut diagnostics).
    """
    parts = []
    for plan in sorted(src[spec.BLOCK].unique()):
        s = src[src[spec.BLOCK] == plan].sort_values("renewal_id", kind="mergesort")
        d = dst[dst[spec.BLOCK] == plan].sort_values("renewal_id", kind="mergesort")
        if d.empty:
            continue
        zs_all, zd = zscore(s, scaler), zscore(d, scaler)
        dst_ids = d["renewal_id"].to_numpy()
        pos = {v: i for i, v in enumerate(dst_ids)}
        src_ids = s["renewal_id"].to_numpy()
        n_dst = len(dst_ids)
        width = min(k + keep_extra, n_dst)
        for a in range(0, len(s), chunk):
            zs = zs_all[a:a + chunk]
            d2 = np.zeros((len(zs), n_dst), dtype=np.float64)
            for f in range(zd.shape[1]):  # explicit left-to-right sum in FEATURES order
                diff = zs[:, f][:, None] - zd[:, f][None, :]
                d2 = d2 + diff * diff
            key = np.floor(d2 * spec.QUANT + 0.5)
            ids = src_ids[a:a + chunk]
            self_col = np.array([pos.get(x, -1) for x in ids])
            has_self = self_col >= 0
            key[np.nonzero(has_self)[0], self_col[has_self]] = np.inf
            order = np.argsort(key, axis=1, kind="stable")[:, :width]  # dst sorted by id: ties -> id
            k_eff = np.minimum(k, n_dst - has_self.astype(int))
            take = np.minimum(k_eff + keep_extra, n_dst - has_self.astype(int))
            rank = np.arange(1, width + 1)[None, :].repeat(len(zs), axis=0)
            mask = rank <= take[:, None]
            r_idx, c_idx = np.nonzero(mask)
            j = order[r_idx, c_idx]
            dd = d2[r_idx, j]
            parts.append(pd.DataFrame({
                "src": ids[r_idx], "dst": dst_ids[j], "rank": rank[r_idx, c_idx].astype(np.int64), "d2": dd,
                "d2_q": spec.d2_quantise(dd), "in_contract": rank[r_idx, c_idx] <= k_eff[r_idx]}))
    cols = ["src", "dst", "rank", "d2", "d2_q", "in_contract"]
    if not parts:
        return pd.DataFrame(columns=cols)
    return pd.concat(parts, ignore_index=True)[cols].sort_values(["src", "rank"], kind="mergesort").reset_index(
        drop=True)


def similar_to(renewals: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(edges, scaler) of spec similar_to/renewal-v1 for a Renewal frame.

    Sources: every renewal. Candidates: route == 'model'. Block: plan_tier.
    ``similar_to_full`` also returns the rank k+1 cut rows and the tie diagnostics.
    """
    sim, scaler, _cut, _diagnostics = similar_to_full(renewals)
    return sim, scaler


def similar_to_full(renewals: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """SIMILAR_TO edges, persisted scaler, rank k+1 cut rows and tie diagnostics."""
    ren = renewals.sort_values("renewal_id", kind="mergesort").reset_index(drop=True)
    ref = ren[ren["route"] == spec.REFERENCE_ROUTE]
    scaler = fit_scaler(ref)
    e = knn(ren, ref, scaler, keep_extra=1)
    sim = e[e["in_contract"]].copy()
    cut = e[~e["in_contract"]][["src", "dst", "rank", "d2", "d2_q"]].reset_index(drop=True)
    pairs = set(zip(sim["src"], sim["dst"], strict=True))
    sim["mutual"] = [(b, a) in pairs for a, b in zip(sim["src"], sim["dst"], strict=True)]
    sim["dist"] = np.sqrt(sim["d2"].to_numpy())
    sim["spec_version"] = spec.SIMILAR_TO_SPEC_VERSION
    return sim, scaler, cut, similar_to_diagnostics(sim, cut)


def similar_to_diagnostics(sim: pd.DataFrame, cut: pd.DataFrame) -> dict:
    """Rank-k/k+1 cut: exact raw ties, quantised ties broken by dst, exact halves."""
    last = sim.loc[sim.groupby("src")["rank"].idxmax(), ["src", "d2", "d2_q"]].set_index("src")
    c = cut.set_index("src")
    gap = (c["d2"] - last["d2"].reindex(c.index)).dropna()
    gap_q = (c["d2_q"] - last["d2_q"].reindex(c.index)).dropna()
    ranked = np.concatenate([sim["d2"].to_numpy(), cut["d2"].to_numpy()]) * spec.QUANT
    dist_half = np.abs(ranked - np.floor(ranked) - 0.5)
    return {
        "sources_with_cut_candidate": len(gap),
        "cut_exact_raw_ties": int((gap == 0).sum()),
        "cut_gap_lt_1e-9": int((gap < 1e-9).sum()),
        "cut_quantised_ties_broken_by_dst": int((gap_q == 0).sum()),
        "cut_min_gap": float(gap.min()) if len(gap) else None,
        "cut_min_positive_gap": float(gap[gap > 0].min()) if (gap > 0).any() else None,
        "exact_halves": int((dist_half == 0).sum()),
        "nearest_half_quanta": float(dist_half.min()) if len(dist_half) else None,
    }


# --------------------------------------------------------------------------- tables
def _event_date(col: pd.Series, what: str) -> pd.Series:
    """The DATE the graph stores for an event (the way silver derives hit_date from hit_at).

    renewal-graph/v1 filters on whole days, and the gold rule compares the raw column with
    as_of (midnight): they agree only while the column carries no time of day, so a time of
    day is a build error here instead of a silent point-in-time difference later.
    """
    try:
        ts = pd.to_datetime(col)
    except (ValueError, TypeError) as e:  # e.g. rows with different UTC offsets
        raise GraphBuildError(f"{what} is not one date format ({str(e).splitlines()[0][:160]}): renewal-graph/v1 "
                              f"reads whole dates (YYYY-MM-DD). Fix the bronze column") from e
    if isinstance(ts.dtype, pd.DatetimeTZDtype):
        example = ts.dropna().iloc[0] if ts.notna().any() else None
        raise GraphBuildError(
            f"{what} carries a UTC offset ({ts.dtype.tz}, e.g. {example}): renewal-graph/v1 reads whole dates "
            f"(YYYY-MM-DD) and never converts or truncates a timestamp silently. Fix the bronze column (dates only), "
            f"or bump the graph spec first")
    day = ts.dt.normalize()
    timed = ts.notna() & (ts != day)
    if timed.any():
        raise GraphBuildError(
            f"{what} carries a time of day ({int(timed.sum()):,} of {len(ts):,} rows, e.g. {ts[timed].iloc[0]}): "
            f"renewal-graph/v1 stores event_date as a DATE, so its as_of filter would differ from the gold rule "
            f"and the time would be truncated silently. Fix the bronze column (dates only), or bump the graph "
            f"spec first. limit_events.hit_at is the one source with a time: it follows the gold rule "
            f"to_date(hit_at)")
    return day


def check_date_grain(silver: dict[str, pd.DataFrame]) -> None:
    """Fail when any date-grained source the graph reads carries a time of day.

    Covers every DATE_GRAINED_SOURCES column that is present (a missing table or column is
    reported by the code that needs it). limit_events.hit_at is not in the list: see above.
    """
    for table, col, what in DATE_GRAINED_SOURCES:
        df = silver.get(table)
        if df is not None and col in df.columns and len(df):
            _event_date(df[col], what)


_PLAIN_DATE = r"\d{4}-\d{2}-\d{2}"
_DATE_OR_MIDNIGHT = _PLAIN_DATE + r"(?:[T ]00:00(?::00(?:\.0+)?)?)?"  # the same date, written as a naive midnight
_CLOCK = r"(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d+)?)?"          # HH:MM[:SS[.f]] on a 24-hour clock
_NAIVE_TIMESTAMP = _PLAIN_DATE + rf"(?:[T ]{_CLOCK})?"                  # limit_events.hit_at: no offset, no 'Z'
_TIMESTAMPS = frozenset(what for _, _, what in TIMESTAMP_SOURCES)


def _date_columns(allow_midnight: bool) -> list[tuple[str, str, str]]:
    """(column, bronze name, pattern) of every bronze date / timestamp column the build reads."""
    day = _DATE_OR_MIDNIGHT if allow_midnight else _PLAIN_DATE
    return [(col, what, day) for _, col, what in DATE_GRAINED_SOURCES] + \
           [(col, what, _NAIVE_TIMESTAMP) for _, col, what in TIMESTAMP_SOURCES]


def _real_day(vals: pd.Series) -> pd.Series:
    """True where the leading YYYY-MM-DD of a value is a calendar day (2026-02-30 and 2026-13-01 are not)."""
    return pd.to_datetime(vals.str.slice(0, 10), format="%Y-%m-%d", errors="coerce").notna()


def not_plain_dates(sample_dir: str | os.PathLike, allow_midnight: bool = False) -> list[dict]:
    """The bronze date columns whose values are not all what the graph reads, read as text.

    Date-grained columns (DATE_GRAINED_SOURCES) must be plain YYYY-MM-DD; limit_events.hit_at
    (TIMESTAMP_SOURCES) a naive YYYY-MM-DD[ HH:MM[:SS]]; in both, the date must be a real calendar
    day. One entry per offending column: {what, file, column, rows, bad, row, example} (row =
    1-based line of the first offender, header = line 1). ``allow_midnight`` also accepts a naive
    midnight ('2026-07-01 00:00:00') in a date-grained column: the build's pre-pass, which refuses
    only what could change or hide a date. Without it: which column to name when pandas refuses a
    column with mixed formats.
    """
    out = []
    for col, what, pattern in _date_columns(allow_midnight):
        path = Path(sample_dir) / f"{what.split('.')[0]}.csv"
        try:
            vals = pd.read_csv(path, usecols=[col], dtype=str, keep_default_na=False)[col]
        except (OSError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError):
            continue  # a missing file or column is reported by the code that needs it
        filled = vals[vals.str.strip() != ""]
        wrong = ~filled.str.fullmatch(pattern)
        if (~wrong).any():
            wrong[~wrong] = ~_real_day(filled[~wrong])
        bad = filled[wrong]
        if len(bad):
            out.append({"what": what, "file": path.name, "column": col, "rows": len(vals), "bad": len(bad),
                        "row": int(bad.index[0]) + 2, "example": str(bad.iloc[0])})
    return out


def _not_a_date(example: str) -> str:
    """What a value the graph cannot read carries: 'a UTC offset', 'an impossible calendar date',
    'an impossible time of day', 'a time of day' or 'another date format'."""
    v = example.strip()
    if re.search(r"[T ]\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?\s*(Z|[+-]\d{2}(:?\d{2})?)$", v):
        return "a UTC offset"
    if not re.match(_PLAIN_DATE, v):
        return "another date format"
    if not _real_day(pd.Series([v])).iloc[0]:
        return "an impossible calendar date"
    if re.fullmatch(_PLAIN_DATE + r"[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?", v) and not re.fullmatch(_NAIVE_TIMESTAMP, v):
        return "an impossible time of day"
    return "a time of day" if re.search(r"[T ]\d{1,2}:\d{2}", v) else "another date format"


def mixed_dates_message(sample_dir: str | os.PathLike, pandas_error: str = "", found: list[dict] | None = None) -> str:
    """The build error for a bronze date column the graph cannot read (the pre-pass found it,
    pandas refused it, or it carries a UTC offset or an impossible date): names the file, the
    column, how many rows and an example, instead of pandas' own hint or a traceback. ``found``:
    not_plain_dates() output (default: computed, strict)."""
    found = not_plain_dates(sample_dir) if found is None else found
    tail = ("renewal-graph/v1 reads whole dates (YYYY-MM-DD) and never converts, truncates or guesses a date "
            "silently. Fix the bronze column; limit_events.hit_at is the one source with a time: a naive timestamp "
            "(no UTC offset or 'Z') that follows the gold rule to_date(hit_at)")
    if not found:
        detail = pandas_error.split(". You might want to try", 1)[0].strip().rstrip(".:")
        return (f"a date column of the bronze CSVs in {sample_dir} mixes formats ({detail}): some rows carry a time "
                f"of day or another format. {tail}")
    first = found[0]
    carries = _not_a_date(first["example"])
    shape = ("naive YYYY-MM-DD[ HH:MM:SS] timestamp" if first["what"] in _TIMESTAMPS else
             "plain YYYY-MM-DD date")
    where = f"e.g. {first['example']!r} in {first['file']} line {first['row']}"
    more = "".join(f"; also {f['what']} ({f['bad']:,} of {f['rows']:,} rows)" for f in found[1:])
    if first["bad"] == first["rows"]:
        head = f"{first['what']} is not a {shape} column: all {first['rows']:,} rows carry {carries}, {where}"
    else:
        head = (f"{first['what']} mixes formats: {first['bad']:,} of {first['rows']:,} rows are not a {shape}, "
                f"{where} (some rows carry {carries})")
    return f"{head}{more}. {tail}"


def build_tables(silver: dict[str, pd.DataFrame], gold: pd.DataFrame, today: pd.Timestamp,
                 consts: dict | None = None) -> dict[str, pd.DataFrame]:
    """All node and edge tables of renewal-graph/v1 (pure: no I/O when ``consts`` is given).

    Returns {Label: nodes, TYPE: edges} for every NODE_SCHEMA / EDGE_SCHEMA entry plus
    "similar_to_scaler" (persisted z-score scaler) and "similar_to_cut" (rank k+1 rows).
    ``consts`` comes from gold_constants(): plans, allowance, cap_cut of the gold twin
    (default: read from the repo's gold twin).
    """
    if consts is None:
        consts = gold_constants(load_gold_twin(spec.repo_root() / "data/sample/churn"))
    s = silver
    check_date_grain(s)
    snaps = s["snapshots"]
    g = gold.copy()
    missing = [f for f in spec.GOLD_FEATURES if f not in g.columns]
    if missing:
        raise GraphBuildError(f"gold is missing features {missing}: the renewal model changed; update spec.py")
    for f in spec.NUMERIC_FEATURES:
        kind = g[f].dtype.kind
        if (f in spec.INT_FEATURES) != (kind in "iu"):
            raise GraphBuildError(f"gold feature {f} has dtype {g[f].dtype}; spec says "
                                  f"{'int' if f in spec.INT_FEATURES else 'float'}")
    g["subscription_id"] = g["user_id"]
    g["renewal_date"] = pd.to_datetime(g["renewal_date"])
    g["as_of"] = pd.to_datetime(g["feature_as_of"])
    g["renewal_id"] = g["subscription_id"] + ":" + g["renewal_date"].dt.strftime("%Y-%m-%d")
    g = g.sort_values("renewal_id", kind="mergesort").reset_index(drop=True)
    if not snaps["subscription_id"].is_unique or set(snaps["subscription_id"]) != set(g["subscription_id"]):
        raise GraphBuildError("renewal-graph/v1 needs exactly one T-7 snapshot (renewal) per subscription")
    as_of = g.set_index("subscription_id")["as_of"]
    rdate = g.set_index("subscription_id")["renewal_date"]
    t: dict[str, pd.DataFrame] = {}

    # ---- nodes
    t["Subscription"] = _nodes(snaps, "Subscription")
    ev, inv = s["sub_events"], s["invoices"]
    canceled = ev[ev["event_type"] == "canceled"].groupby("subscription_id")["event_date"].max()
    # Outcome observed: the renewal date if renewed, the canceled date if lapsed, else null.
    lapsed = g["outcome"].isin(["voluntary_lapse", "involuntary_lapse"])
    observed = g["renewal_date"].where(g["outcome"] == "renewed")
    g["outcome_observed_on"] = pd.to_datetime(observed.where(~lapsed, g["subscription_id"].map(canceled)))
    changes = sorted(s["pricing"]["effective_date"]) if len(s["pricing"]) else []
    g["cuts_so_far"] = sum((g["as_of"] >= c).astype(np.int64) for c in changes) if changes else 0
    g["cuts_so_far"] = g["cuts_so_far"].astype(np.int64)
    allowance = g["plan_tier"].map(consts["allowance"]).astype(np.float64)
    g["allowance_at_as_of"] = (allowance * consts["cap_cut"] ** g["cuts_so_far"]).round(2)
    g["is_reference"] = g["route"] == spec.REFERENCE_ROUTE
    g["churned"] = g["churned"].astype(np.int64)
    t["Renewal"] = _nodes(g, "Renewal")
    t["Plan"] = _nodes(pd.DataFrame({
        "plan_tier": consts["plans"],
        "price_usd": [spec.PLAN_PRICE_USD[p] for p in consts["plans"]],
        "base_allowance_28d": [consts["allowance"][p] for p in consts["plans"]]}), "Plan")
    inc = s["incidents"].copy()
    inc["days"] = (inc["ends_on"] - inc["starts_on"]).dt.days + 1
    t["Incident"] = _nodes(inc, "Incident")
    pc = s["pricing"].copy()
    pc["cap_multiplier"] = consts["cap_cut"]
    t["PricingChange"] = _nodes(pc, "PricingChange")

    lim = s["limits"].copy()
    lim["event_id"] = _seq_ids(lim, "lh", ["hit_at", "limit_type"])
    lim["event_date"] = lim["hit_date"]
    t["LimitHit"] = _nodes(lim, "LimitHit")
    ovs = s["overage_settings"].copy()
    ovs["event_id"] = _seq_ids(ovs, "ovs", ["changed_at", "overage"])
    ovs["event_date"], ovs["state"] = _event_date(ovs["changed_at"], "overage_settings.changed_at"), ovs["overage"]
    t["OverageChange"] = _nodes(ovs, "OverageChange")
    ovc = s["overage_charges"].copy()
    ovc["event_id"] = _seq_ids(ovc, "ovc", ["charged_at", "amount_usd"])
    ovc["event_date"] = _event_date(ovc["charged_at"], "overage_charges.charged_at")
    t["OverageCharge"] = _nodes(ovc, "OverageCharge")
    tk = s["tickets"].copy()
    tk["event_date"] = _event_date(tk["created_date"], "support_tickets.created_date")
    t["Ticket"] = _nodes(tk, "Ticket")

    # Billing: cancel events + renewal-cycle invoices (paid at T, failed dunning attempts).
    # History invoices (< renewal_date) stay aggregated in Renewal.renewals_completed.
    inv = inv.assign(renewal_date=inv["subscription_id"].map(rdate))
    cyc = inv[inv["invoice_date"] >= inv["renewal_date"]]
    bill = pd.concat([
        pd.DataFrame({"subscription_id": ev["subscription_id"], "event_date": ev["event_date"],
                      "event_type": ev["event_type"], "amount_usd": np.nan,
                      "attempt": pd.array([pd.NA] * len(ev), dtype="Int64")}),
        pd.DataFrame({"subscription_id": cyc["subscription_id"], "event_date": cyc["invoice_date"],
                      "event_type": "invoice_" + cyc["status"], "amount_usd": cyc["amount_usd"].astype(np.float64),
                      "attempt": cyc["attempt"].astype("Int64")}),
    ], ignore_index=True)
    bill["event_id"] = _seq_ids(bill, "bill", ["event_date", "event_type"])
    bill["outcome_evidence"] = ~((bill["event_type"] == "cancel_scheduled")
                                 & (bill["event_date"] <= bill["subscription_id"].map(as_of)))
    t["BillingEvent"] = _nodes(bill, "BillingEvent")

    # ---- edges
    t["HAS_RENEWAL"] = _edges(g, "HAS_RENEWAL", src=g["subscription_id"], dst=g["renewal_id"], as_of=g["as_of"])
    t["ON_PLAN"] = _edges(g, "ON_PLAN", src=g["renewal_id"], dst=g["plan_tier"], as_of=g["as_of"])
    t["HIT_LIMIT"] = _edges(lim, "HIT_LIMIT", src=lim["subscription_id"], dst=lim["event_id"],
                            event_date=lim["event_date"])
    t["CHANGED_OVERAGE"] = _edges(ovs, "CHANGED_OVERAGE", src=ovs["subscription_id"], dst=ovs["event_id"],
                                  event_date=ovs["event_date"], state=ovs["state"])
    t["CHARGED_OVERAGE"] = _edges(ovc, "CHARGED_OVERAGE", src=ovc["subscription_id"], dst=ovc["event_id"],
                                  event_date=ovc["event_date"], amount_usd=ovc["amount_usd"])
    t["OPENED"] = _edges(tk, "OPENED", src=tk["subscription_id"], dst=tk["ticket_id"], event_date=tk["event_date"])
    t["BILLED"] = _edges(bill, "BILLED", src=bill["subscription_id"], dst=bill["event_id"],
                         event_date=bill["event_date"], event_type=bill["event_type"],
                         outcome_evidence=bill["outcome_evidence"])
    # EXPOSED_TO: one edge per active usage day inside a declared incident window.
    usage = s["usage"][["subscription_id", "activity_date"]]
    days = [usage[(usage["activity_date"] >= r.starts_on) & (usage["activity_date"] <= r.ends_on)]
            .assign(incident_id=r.incident_id) for r in s["incidents"].itertuples()]
    exp = pd.concat(days, ignore_index=True) if days else usage.iloc[:0].assign(incident_id=pd.Series(dtype=object))
    t["EXPOSED_TO"] = _edges(exp, "EXPOSED_TO", src=exp["subscription_id"], dst=exp["incident_id"],
                             event_date=exp["activity_date"])
    # FIRST_RENEWAL_AFTER: the gold rule (declared PIT exception), flagged known_by_as_of.
    started = snaps.set_index("subscription_id")["started_at"]
    fra = []
    for c in s["pricing"].itertuples():
        eff = c.effective_date
        m = g[(eff >= g["renewal_date"] - pd.Timedelta(days=spec.FIRST_RENEWAL_WINDOW_DAYS))
              & (eff < g["renewal_date"]) & (g["subscription_id"].map(started) < eff)]
        fra.append(pd.DataFrame({"src": m["renewal_id"], "dst": c.change_id, "event_date": eff,
                                 "known_by_as_of": eff <= m["as_of"]}))
    fra_df = pd.concat(fra, ignore_index=True) if fra else pd.DataFrame(columns=["src", "dst", "event_date",
                                                                                   "known_by_as_of"])
    t["FIRST_RENEWAL_AFTER"] = _edges(fra_df, "FIRST_RENEWAL_AFTER", **{c: fra_df[c] for c in fra_df.columns})
    cut_cap = pc.merge(pd.DataFrame({"plan_tier": consts["plans"]}), how="cross")
    t["CUT_CAP"] = _edges(cut_cap, "CUT_CAP", src=cut_cap["change_id"], dst=cut_cap["plan_tier"],
                          event_date=cut_cap["effective_date"], multiplier=cut_cap["cap_multiplier"])
    sim, scaler, cut, _ = similar_to_full(t["Renewal"])
    t["SIMILAR_TO"] = _edges(sim, "SIMILAR_TO", **{c: sim[c] for c in ["src", "dst", "rank", "d2", "d2_q", "dist",
                                                                         "mutual", "spec_version"]})
    t["similar_to_scaler"] = scaler
    t["similar_to_cut"] = cut
    return t


# --------------------------------------------------------------------------- Parquet
def _arrow_column(s: pd.Series, typ: pa.DataType) -> pa.Array:
    if pa.types.is_date32(typ):
        ts = pd.to_datetime(s)
        if len(ts) and (ts.notna() & (ts != ts.dt.normalize())).any():  # backstop for check_date_grain()
            raise GraphBuildError(f"column {s.name} carries a time of day but is stored as a DATE: refusing to "
                                  f"truncate it silently")
        vals = ts.to_numpy(dtype="datetime64[D]")
        return pa.array(vals, type=pa.date32(), from_pandas=True)
    if pa.types.is_string(typ):
        return pa.array(s.astype(object).where(s.notna(), None).tolist(), type=typ)
    if pa.types.is_boolean(typ):
        return pa.array(s.astype(bool).to_numpy(), type=typ)
    return pa.array(s, type=typ, from_pandas=True)  # int64 (nullable Int64 ok) / float64 (NaN -> null)


def to_arrow(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Explicit schema, fixed column order, no pandas metadata (engine-neutral, byte-stable)."""
    arrays = [_arrow_column(df[f.name], f.type) for f in schema]
    for f, a in zip(schema, arrays, strict=True):
        if not f.nullable and a.null_count:
            raise GraphBuildError(f"column {f.name} has {a.null_count} nulls but is not nullable")
    return pa.Table.from_arrays(arrays, schema=schema)


def write_parquet(df: pd.DataFrame, path: Path, schema: pa.Schema) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_arrow(df, schema), path, **PARQUET_OPTIONS)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table_files() -> list[tuple[str, str, pa.Schema]]:
    """(table name, relative path, schema) for every file the builder writes, in order."""
    out = [(n.label, f"parquet/{n.file}", n.schema) for n in spec.NODE_SCHEMA.values()]
    out += [(e.rel, f"parquet/{e.file}", e.schema) for e in spec.EDGE_SCHEMA.values()]
    out += [("similar_to_scaler", spec.SCALER_FILE, spec.SCALER_SCHEMA), ("similar_to_cut", CUT_FILE, CUT_SCHEMA)]
    return out


def write_tables(tables: dict[str, pd.DataFrame], out_dir: Path) -> dict[str, dict]:
    files = {}
    for name, rel, schema in table_files():
        files[rel] = {"table": name, "rows": len(tables[name]),
                      "sha256": write_parquet(tables[name], out_dir / rel, schema)}
    return files


def graph_counts(tables: dict[str, pd.DataFrame]) -> dict:
    """Node/edge counts of build_tables() output."""
    nodes = {label: len(tables[label]) for label in spec.NODE_SCHEMA}
    edges = {rel: len(tables[rel]) for rel in spec.EDGE_SCHEMA}
    return {"nodes": nodes, "edges": edges, "total_nodes": sum(nodes.values()), "total_edges": sum(edges.values())}


# --------------------------------------------------------------------------- orchestration
def _max_rss_bytes() -> int:
    import resource
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(r if sys.platform == "darwin" else r * 1024)  # macOS: bytes; Linux: KiB


def check_bronze(sample_dir: Path, profile: str) -> None:
    missing = [f for f in spec.BRONZE_FILES if not (sample_dir / f).is_file()]
    if missing:
        hint = ("Run: make churn-gold-local (regenerates data/sample/churn and data/export in place) or "
                "make graph-sample PROFILE=s42 SEED=42 N_USERS=8000 (scratch profile)")
        if profile == "tiny":
            hint = (f"The tiny profile reads the committed fixture {spec.TINY_FIXTURE}; make graph-sample "
                    f"PROFILE=tiny writes its exports only and never regenerates bronze. Restore the fixture from "
                    f"git (git checkout -- {spec.TINY_FIXTURE})")
        elif profile == "inject":
            hint = ("Run: make graph-sample PROFILE=inject (copies the tiny fixture to $GRAPH_ROOT/inject/sample and "
                    "plants one poisoned user_name), or scripts/build_graph_local.py build --profile inject")
        elif profile != "default":
            hint = f"Run: make graph-sample PROFILE={profile}"
        raise GraphBuildError(f"bronze CSVs missing in {sample_dir}: {', '.join(missing)}. {hint}")


def load_ladybug_subprocess(parquet_dir: Path, db_path: Path, buffer_pool_mb: int = store.LOAD_BUFFER_POOL_MB,
                            threads: int = store.THREADS) -> dict:
    """Run the Ladybug loader in a child process (isolates crashes, measures loader RSS)."""
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(filter(None, [str(spec.repo_root() / "src"),
                                                                    os.environ.get("PYTHONPATH")])))
    cmd = [sys.executable, "-m", "lakehouse_graph.store", "load", "--parquet-dir", str(parquet_dir),
           "--db", str(db_path), "--buffer-pool-mb", str(buffer_pool_mb), "--threads", str(threads)]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if p.returncode:
        raise GraphBuildError(f"Ladybug load failed (exit {p.returncode}): {p.stderr.strip()[-2000:]}")
    return json.loads(p.stdout)


def repin(bdir: Path, man: dict, *, profile: str, sample_dir: Path, export_dir: Path,
          graph_root: str | os.PathLike | None, guard: dict, verify_seed: bool = False, log=print) -> dict:
    """Refresh what an unchanged build's manifest says about the world around it.

    Building the same bronze with the same code gives the same business_build_id and the
    same Parquet, so the build is kept. The profile's exports (``make graph-sample`` or
    ``make churn-gold-local`` rewrite them with a new built_at), the user's guarded files
    and the seed / N_USERS status may have moved on since: pin them again, atomically, so
    the contract compares the build with what is on disk now. Returns the manifest in force.
    """
    new, changed = dict(man), []
    exports = mf.export_pin(export_dir)
    if man.get("exports") != exports:
        new["exports"] = exports
        changed.append("exports sha256")
    if man.get("guarded_sha256") != guard:
        new["guarded_sha256"] = guard
        changed.append("guarded files sha256")
    # The bronze bytes are the ones this build was made from (same id): a verification made
    # then still holds, and a plain declaration never replaces it.
    was_verified = man.get("seed_n_status") == "verified"
    seed = mf.seed_info(profile, sample_dir, graph_root, verify=False)
    if verify_seed and not was_verified and seed["status"] != "verified":
        seed = mf.seed_info(profile, sample_dir, graph_root, verify=True, scratch=spec.builds_dir(profile, graph_root))
    fields = mf.seed_fields(seed)
    if (seed["status"] == "verified" or not was_verified) and any(man.get(k) != v for k, v in fields.items()):
        new.update(fields)
        changed.append(f"seed / N_USERS ({seed['status']})")
    if changed:
        new["repinned_at"] = mf.utc_now()
        mf.write_manifest(bdir, new)
        log(f"    re-pinned in manifest.json: {', '.join(changed)}")
    return new


def build_profile(profile: str, sample_dir: str | os.PathLike | None = None,
                  export_dir: str | os.PathLike | None = None, graph_root: str | os.PathLike | None = None, *,
                  rebuild: bool = False, verify_seed: bool = False, lock_timeout: float = 600.0,
                  log=print) -> tuple[Path, dict]:
    """Build one profile into $GRAPH_ROOT/<profile>/builds/<business_build_id>/.

    An existing build with the same id is kept when the rebuilt Parquet is byte-identical
    (its manifest pins are refreshed, see ``repin``); ``rebuild=True`` replaces it. A
    replacement is built completely in a temp directory and swapped in atomically
    (store.replace_dir): links into the build stay valid throughout. When the Parquet is
    byte-identical the replacement inherits the CARRY_OVER entries (contract.json, which stops
    counting on its own if exports changed; the lineage tables while fresh; cohorts.parquet;
    whatever later phases registered), and the log names what was dropped and why.
    """
    t_start = time.perf_counter()
    spec.check_profile(profile)
    root = spec.graph_root(graph_root)
    sdir = Path(sample_dir).absolute() if sample_dir else spec.sample_dir(profile, root)
    edir = Path(export_dir).absolute() if export_dir else spec.export_dir(profile, root)
    check_bronze(sdir, profile)
    # every date-grained bronze column is a date (a naive midnight is the same date) and hit_at a naive
    # timestamp, each on a real calendar day: a time of day, a UTC offset, another format or 2026-02-30
    # is refused here, by name, before pandas or gold() read it
    undated = not_plain_dates(sdir, allow_midnight=True)
    if undated:
        raise GraphBuildError(mixed_dates_message(sdir, found=undated))
    guard_before = mf.guarded_hashes()
    with store.BuildLock(root, timeout=lock_timeout, log=log):
        ident = mf.build_identity(sdir)
        bid = ident["business_build_id"]
        bdir = spec.builds_dir(profile, root) / bid
        for name in store.restore_orphans(spec.builds_dir(profile, root)):
            log(f"    restored build {name}: an interrupted replacement had left it in {store.TRASH_PREFIX}*")
        tmp = spec.builds_dir(profile, root) / f".tmp-{bid}-{os.getpid()}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        try:
            log(f"==> graph build: profile {profile}, bronze {sdir}, business_build_id {bid}")
            mod = load_gold_twin(sdir)
            try:
                silver, gold, today = run_gold(mod)
                tables = build_tables(silver, gold, today, gold_constants(mod))
            except (KeyError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                raise GraphBuildError(f"the bronze CSVs in {sdir} are not what the gold twin reads "
                                      f"({type(e).__name__}: {e}): a file is empty, has no header row or lacks "
                                      f"a column") from e
            except (ValueError, TypeError) as e:
                # pandas refuses a date column whose rows do not share one format, or one with UTC
                # offsets ("Tz-aware ..."), or gold() cannot compare an offset column with as_of
                # (TypeError): when a date-grained bronze column is not plain YYYY-MM-DD, that is the
                # cause, and the error names it; anything else is not a date problem and propagates.
                first = str(e).splitlines()[0] if str(e) else ""
                dated = isinstance(e, ValueError) and ("time data" in first or "unconverted data remains" in first)
                if not dated and not not_plain_dates(sdir):
                    raise
                raise GraphBuildError(mixed_dates_message(sdir, first)) from e
            diagnostics = similar_to_diagnostics(tables["SIMILAR_TO"], tables["similar_to_cut"])
            files = write_tables(tables, tmp)
            counts = graph_counts(tables)
            build_s = time.perf_counter() - t_start
            builder_rss = _max_rss_bytes()
            del tables, silver, gold
            if bdir.exists() and not rebuild:
                try:
                    old = mf.read_manifest(bdir)
                except (OSError, ValueError) as e:
                    raise GraphBuildError(f"{bdir} exists but has no readable manifest.json ({e}); rerun with "
                                          f"--rebuild to replace it") from e
                same = {k: v["sha256"] for k, v in old.get("files", {}).items()} == \
                       {k: v["sha256"] for k, v in files.items()}
                if not same:
                    raise GraphBuildError(
                        f"determinism violation: {bdir} exists with the same business_build_id but different "
                        f"Parquet bytes (rerun with --rebuild to replace it)")
                if (bdir / store.DB_FILE).is_file():
                    shutil.rmtree(tmp)
                    log(f"    unchanged: rebuilt Parquet is byte-identical to the existing build {bid} "
                        f"({len(files)} files, sha256 equal); kept it")
                    man = repin(bdir, old, profile=profile, sample_dir=sdir, export_dir=edir, graph_root=root,
                                guard=guard_before, verify_seed=verify_seed, log=log)
                    store.update_link(spec.latest_link(profile, root), bdir)
                    log(f"    builder {build_s:.2f} s, max RSS {builder_rss / 2**20:.0f} MiB")
                    _check_guard(guard_before, log)
                    return bdir, man
                log(f"    {store.DB_FILE} is missing from the existing build {bid} (Parquet unchanged): "
                    f"replacing the build")
            ld = load_ladybug_subprocess(tmp / "parquet", tmp / store.DB_FILE)
            want = {**counts["nodes"], **counts["edges"]}
            bad = {k: (want[k], ld["counts"].get(k)) for k in want if ld["counts"].get(k) != want[k]}
            if bad:
                raise GraphBuildError(f"Ladybug counts differ from Parquet counts: {bad}")
            seed_info = mf.seed_info(profile, sdir, root, verify=verify_seed, scratch=spec.builds_dir(profile, root))
            man = mf.assemble_manifest(
                ident=ident, profile=profile, sample_dir=sdir, export_dir=edir, files=files, counts=counts,
                today=today, diagnostics=diagnostics, seed=seed_info, guard=guard_before,
                ladybug=ld, builder={"seconds": round(build_s, 2), "max_rss_bytes": builder_rss},
                built_at=mf.utc_now())
            mf.write_manifest(tmp, man)
            _check_guard(guard_before, log)
            if bdir.exists():
                held = store.live_pids(bdir)
                if held:
                    raise GraphBuildError(f"replacing build {bid} refused: it is held by live pid(s) {held} "
                                          f"(stop the server first)")
                kept, dropped, keys, reasons = carry_over(bdir, tmp, files)
                if keys:  # the other phases' summaries describe the carried artefacts
                    man = {**man, **keys}
                    mf.write_manifest(tmp, man)
                try:
                    how = store.replace_dir(tmp, bdir)
                except OSError as e:
                    raise GraphBuildError(f"could not swap the new build in ({e}); the existing build {bid} is "
                                          f"unchanged and links into it are still valid") from e
                said = [f"Parquet byte-identical, kept {', '.join(kept)}"] if kept else []
                if store.CONTRACT_FILE not in kept:
                    said.append("no contract record carried over (run make graph-check)")
                if dropped:
                    why = "".join(f"{r}; " for r in reasons)
                    said.append(f"not carried over: {', '.join(dropped)} ({why}rebuild them with their own targets)")
                log(f"    replaced the existing build {bid} atomically ({how}); {'; '.join(said)}")
            else:
                os.replace(tmp, bdir)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        store.update_link(spec.latest_link(profile, root), bdir)
    log(f"    {counts['total_nodes']:,} nodes / {counts['total_edges']:,} edges; Parquet + graph.lbdb "
        f"({ld['db_bytes'] / 1e6:.1f} MB, load {ld['load_s']} s) -> {bdir}")
    log(f"    builder {build_s:.2f} s, max RSS {builder_rss / 2**20:.0f} MiB; loader max RSS "
        f"{ld['max_rss_bytes'] / 2**20:.0f} MiB")
    return bdir, man


class CarriedOver(NamedTuple):
    kept: list[str]      # entries copied into the replacement ("lineage/" for a directory)
    dropped: list[str]   # what the old build held beyond the builder's output and the carried entries
    keys: dict           # manifest.json summary keys to write into the replacement's manifest
    reasons: list[str]   # "<registry name>: <why>" for each registered entry that no longer holds


def carry_over(old: Path, new: Path, files: dict) -> CarriedOver:
    """Copy the CARRY_OVER entries of the build being replaced into its replacement (``new``, the
    finished temp directory, before the swap; ``files`` = its manifest ``files``).

    Only when the old build's Parquet is byte-identical to the new one (manifest sha256 equal):
    then the artefacts made from that content (and their manifest.json summary keys) are exactly
    as valid as they were, unless an entry's ``still_valid`` check says otherwise (the lineage
    tables must still be fresh for the current code). ``dropped`` never lists the contract
    record (the caller reports it), so nothing is lost silently. Any builder that replaces a
    build directory (build_profile, iceberg_source) calls this; it holds the build lock.
    """
    return CarriedOver(*_carry_over_entries(old, new, files))


def _carry_over(old: Path, new: Path, files: dict) -> tuple[list[str], list[str], dict]:
    """(kept, dropped, keys) of carry_over(), the pre-registry signature (lakehouse_graph.iceberg_source
    calls it); the reasons are in carry_over().reasons."""
    got = carry_over(old, new, files)
    return got.kept, got.dropped, got.keys


def _carry_over_entries(old: Path, new: Path, files: dict) -> tuple[list[str], list[str], dict, list[str]]:
    """carry_over()'s work: (kept, dropped, keys, reasons)."""
    try:
        old_man = mf.read_manifest(old)
        old_files = {k: v["sha256"] for k, v in old_man.get("files", {}).items()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        old_man, old_files = {}, None
    identical = old_files == {k: v["sha256"] for k, v in files.items()}
    kept, keys, reasons = [], {}, []
    for name, item in CARRY_OVER.items() if identical else ():
        # an entry and its database sidecars (lineage.lbdb.wal, ...), never a name the builder wrote itself
        entries = sorted(p for p in old.iterdir() if (p.name in item.entries or p.name.startswith(
            tuple(f"{n}." for n in item.entries))) and not (new / p.name).exists())
        if not entries:
            continue
        why = None
        if item.still_valid is not None:
            try:
                why = item.still_valid(old)
            except (OSError, ValueError, KeyError, TypeError, ImportError) as e:
                why = f"its check failed ({type(e).__name__}: {e})"
        if why:
            reasons.append(f"{name}: {why}")
            continue
        for src in entries:
            if src.is_dir():
                shutil.copytree(src, new / src.name, symlinks=True)
            else:
                shutil.copy2(src, new / src.name)
            kept.append(src.name + ("/" if src.is_dir() else ""))
        if item.manifest_key and item.manifest_key in old_man:
            keys[item.manifest_key] = old_man[item.manifest_key]
    own = {p.name for p in new.iterdir()} | {store.PID_DIR, store.CONTRACT_FILE}
    dropped = sorted(p.name for p in old.iterdir() if p.name not in own and not p.name.startswith(store.DB_FILE))
    return kept, dropped, keys, reasons


def _check_guard(before: dict, log, during: str = "the build") -> None:
    after = mf.guarded_hashes()
    if after != before:
        changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
        raise GraphBuildError(f"non-interference violated: guarded files changed during {during}: {changed}")


# --------------------------------------------------------------------------- samples (graph-sample)
def run_user_script(rel: str, env: dict[str, str]) -> str:
    """Run one of the user's scripts, unchanged, with this interpreter and its directories redirected."""
    p = subprocess.run([sys.executable, str(spec.repo_root() / rel)], env={**os.environ, **env},
                       capture_output=True, text=True, check=False)
    if p.returncode:
        raise GraphBuildError(f"{rel} failed (exit {p.returncode}): {(p.stderr or p.stdout).strip()[-2000:]}")
    return p.stdout


def _write_exports(sdir: Path, edir: Path, log) -> None:
    out = run_user_script(GOLD_SCRIPT, {"CHURN_SAMPLE_DIR": str(sdir), "CHURN_EXPORT_DIR": str(edir)})
    for line in out.strip().splitlines():
        log(f"    {line}")


def poison_snapshots(text: str, subscription_id: str = spec.INJECT_SUBSCRIPTION,
                     user_name: str = spec.INJECT_USER_NAME) -> tuple[str, int]:
    """subscription_snapshots.csv text with ``subscription_id``'s user_name replaced.

    Only the matching rows are rewritten (csv-quoted if needed); every other byte is kept.
    Returns (text, rows changed). The generator writes one record per line, which this relies on.
    """
    lines = text.splitlines(keepends=True)
    header = next(csv.reader([lines[0]]))
    i_sub, i_name = header.index("subscription_id"), header.index("user_name")
    changed = 0
    for i, line in enumerate(lines[1:], start=1):
        if subscription_id not in line:
            continue
        row = next(csv.reader([line]))
        if len(row) != len(header) or row[i_sub] != subscription_id:
            continue
        row[i_name] = user_name
        buf = io.StringIO(newline="")
        csv.writer(buf, lineterminator=line[len(line.rstrip("\r\n")):]).writerow(row)
        lines[i] = buf.getvalue()
        changed += 1
    return "".join(lines), changed


def prepare_inject_profile(graph_root: str | os.PathLike | None = None, *, lock_timeout: float = 600.0,
                           log=print) -> dict:
    """The `inject` profile's inputs: the tiny bronze with exactly ONE poisoned user_name.

    Copies the committed fixture (read only, never modified) into $GRAPH_ROOT/inject/sample,
    replaces the user_name of spec.INJECT_SUBSCRIPTION in subscription_snapshots.csv with
    spec.INJECT_USER_NAME (an instruction aimed at an agent: the eval injection case and the
    output-hygiene tests need one row of hostile text in otherwise known data), writes the
    exports with the user's gold script into $GRAPH_ROOT/inject/export and records
    sample_meta.json. Idempotent: bronze and exports that are already in place are left alone,
    so repeated builds keep their export pins. Takes the BuildLock (call it before, never
    inside, build_profile).
    """
    root = spec.graph_root(graph_root)
    with store.BuildLock(root, timeout=lock_timeout, log=log):
        return _prepare_inject(root, log)


def _prepare_inject(root: Path, log) -> dict:
    fixture = spec.repo_root() / spec.TINY_FIXTURE
    sdir, edir = spec.sample_dir("inject", root), spec.export_dir("inject", root)
    guard = mf.guarded_hashes()
    missing = [f for f in spec.BRONZE_FILES if not (fixture / f).is_file()]
    if missing:
        raise GraphBuildError(f"the tiny fixture {fixture} is incomplete ({', '.join(missing)}): restore it from git")
    poisoned, rows = poison_snapshots((fixture / "subscription_snapshots.csv").read_text(encoding="utf-8"))
    if rows != 1:
        raise GraphBuildError(f"the inject profile poisons exactly one snapshot row; the tiny fixture has {rows} "
                              f"rows for {spec.INJECT_SUBSCRIPTION}")
    want = {f: (fixture / f).read_bytes() for f in spec.BRONZE_FILES}
    want["subscription_snapshots.csv"] = poisoned.encode("utf-8")
    sdir.mkdir(parents=True, exist_ok=True)
    wrote = []
    for name, data in want.items():
        path = sdir / name
        if not path.is_file() or path.read_bytes() != data:
            tmp = path.with_name(f".{name}.tmp-{os.getpid()}")
            tmp.write_bytes(data)
            os.replace(tmp, path)
            wrote.append(name)
    meta = mf.read_sample_meta("inject", root)
    sample_now = mf.input_hashes(sdir)
    stale = (bool(wrote) or not meta or meta.get("sample_sha256") != sample_now
             or meta.get("export_sha256") != mf.export_hashes(edir)
             or any(not (edir / f).is_file() for f in spec.EXPORT_FILES))
    if stale:
        log(f"==> graph-sample inject: tiny fixture -> {sdir} with user_name of {spec.INJECT_SUBSCRIPTION} replaced "
            f"(1 row); exports -> {edir}")
        _write_exports(sdir, edir, log)
        if not mf.verify_seed(fixture, spec.TINY_SEED, spec.TINY_N_USERS, spec.profile_dir("inject", root)):
            raise GraphBuildError("the tiny fixture is no longer generator output for seed 42, N_USERS 120")
        mf.write_sample_meta(
            "inject", root, seed=spec.TINY_SEED, n_users=spec.TINY_N_USERS,
            method=("tiny fixture (regenerated with scripts/generate_churn_sample.py, seed 42, N_USERS 120: sha256 "
                    f"match) + user_name of {spec.INJECT_SUBSCRIPTION} replaced by construction (1 row)"),
            extra={"inject": {"subscription_id": spec.INJECT_SUBSCRIPTION, "user_name": spec.INJECT_USER_NAME,
                              "rows_changed": rows, "source": mf.display_path(fixture)}})
    _check_guard(guard, log, "graph-sample")
    return {"profile": "inject", "sample_dir": sdir, "export_dir": edir, "changed": stale,
            "subscription_id": spec.INJECT_SUBSCRIPTION, "user_name": spec.INJECT_USER_NAME}


def sample_params(profile: str, seed: int | None = None, n_users: int | None = None) -> tuple[int, int]:
    """(seed, n_users) of a profile's sample; ValueError when the request cannot be honoured.

    The seed is derived from the profile name, so an explicit SEED / N_USERS may only repeat
    what the name already says: s<digits> -> the digits (N_USERS free, default 8000); tiny and
    inject -> the committed fixture's seed 42 / N_USERS 120, which nothing regenerates.
    """
    spec.check_profile(profile)
    if profile == "default":
        raise ValueError("PROFILE=default is refused. The default profile reads data/sample/churn and data/export, "
                         "which only `make churn-sample` / `make churn-gold-local` may write. Use PROFILE=s42 "
                         "(SEED=42 N_USERS=8000), PROFILE=tiny or PROFILE=inject")
    if profile in ("tiny", "inject"):
        fixed = (spec.TINY_SEED, spec.TINY_N_USERS)
        if (seed, n_users) not in ((None, None), fixed, (fixed[0], None), (None, fixed[1])):
            given = " ".join(f"{k}={v}" for k, v in (("SEED", seed), ("N_USERS", n_users)) if v is not None)
            does = ("writes EXPORTS ONLY ($GRAPH_ROOT/tiny/export) and never regenerates bronze" if profile == "tiny"
                    else "copies that fixture and plants one poisoned user_name")
            raise ValueError(f"PROFILE={profile} is bound to the committed tiny fixture (seed {fixed[0]}, N_USERS "
                             f"{fixed[1]}): {given} cannot apply. graph-sample PROFILE={profile} {does}. For a "
                             f"generated sample use PROFILE=s<seed>, e.g. PROFILE=s7 N_USERS=500")
        return fixed
    want = int(spec.SEED_PROFILE_RE.match(profile).group(1))
    if seed is not None and seed != want:
        raise ValueError(f"PROFILE={profile} means seed {want}, but SEED={seed}")
    n_users = spec.DEFAULT_N_USERS if n_users is None else n_users
    if n_users < 1:
        raise ValueError(f"N_USERS must be a positive integer (got {n_users})")
    return want, n_users


def prepare_sample(profile: str, seed: int | None = None, n_users: int | None = None,
                   graph_root: str | os.PathLike | None = None, *, lock_timeout: float = 600.0, log=print) -> dict:
    """What `make graph-sample` does for one profile; never writes data/sample or data/export.

      s<seed>  the user's generator (CHURN_SEED=<seed>, N_USERS) then the user's gold script,
               writing $GRAPH_ROOT/<profile>/{sample,export}
      tiny     EXPORTS ONLY ($GRAPH_ROOT/tiny/export): the bronze is the committed fixture
               (seed 42, N_USERS 120), which is verified against the generator, never rewritten
      inject   prepare_inject_profile()
      default  refused: data/sample/churn and data/export belong to `make churn-*`
    Records sample_meta.json (seed / N_USERS verified) and asserts non-interference.
    Raises ValueError for a request that cannot be honoured (see sample_params). Takes the
    BuildLock, so a build never reads a half-written sample (call it before, never inside,
    build_profile).
    """
    seed, n_users = sample_params(profile, seed, n_users)
    root = spec.graph_root(graph_root)
    with store.BuildLock(root, timeout=lock_timeout, log=log):
        if profile == "inject":
            return {**_prepare_inject(root, log), "seed": seed, "n_users": n_users}
        return _prepare_sample(profile, seed, n_users, root, log)


def _prepare_sample(profile: str, seed: int, n_users: int, root: Path, log) -> dict:
    sdir, edir = spec.sample_dir(profile, root), spec.export_dir(profile, root)
    pdir = spec.profile_dir(profile, root)
    guard = mf.guarded_hashes()
    if profile == "tiny":
        check_bronze(sdir, profile)
        log(f"==> graph-sample tiny: EXPORTS ONLY -> {edir}; bronze = the committed fixture {spec.TINY_FIXTURE} "
            f"(seed {seed}, N_USERS {n_users}), read only, never regenerated")
        _write_exports(sdir, edir, log)
        if not mf.verify_seed(sdir, seed, n_users, pdir):
            raise GraphBuildError("the tiny fixture is no longer generator output for seed 42, N_USERS 120")
        method = "regenerated with scripts/generate_churn_sample.py (seed 42, N_USERS 120): sha256 match"
    else:
        log(f"==> graph-sample {profile}: seed {seed}, N_USERS {n_users} -> {pdir}/{{sample,export}}")
        out = run_user_script(mf.GENERATOR, {"CHURN_SAMPLE_DIR": str(sdir), "CHURN_SEED": str(seed),
                                             "N_USERS": str(n_users)})
        for line in out.strip().splitlines():
            log(f"    {line}")
        _write_exports(sdir, edir, log)
        method = "generated by make graph-sample with these values (verified by construction)"
    missing = [f for f in spec.BRONZE_FILES if not (sdir / f).is_file()] + \
              [f for f in spec.EXPORT_FILES if not (edir / f).is_file()]
    if missing:
        raise GraphBuildError(f"missing after generation: {missing} (bronze {sdir}, exports {edir})")
    _check_guard(guard, log, "graph-sample")
    mf.write_sample_meta(profile, root, seed=seed, n_users=n_users, method=method)
    return {"profile": profile, "sample_dir": sdir, "export_dir": edir, "changed": True, "seed": seed,
            "n_users": n_users}
