"""pandas oracle: golden answers and invariants computed from a build's Parquet only.

The oracle is the reference the contract compares three things against:
  1. structural invariants (PIT parity must be 0, naive traversals must be wrong, ...);
  2. the same questions asked in Cypher on graph.lbdb (scripts/check_graph_contract.py);
  3. the committed goldens in ``goldens/<name>.json`` (selected by the bronze sha256).

Goldens are never typed by hand. Regenerate them when the renewal model changes:

  make graph-golden             fresh tiny + s42 builds in a scratch GRAPH_ROOT, diff against the
                                committed files, nothing written
  make graph-golden CONFIRM=1   the same, and the files that differ are rewritten
  (refresh_golden() below; one build by hand:
   PYTHONPATH=src python -m lakehouse_graph.oracle --build <build_dir> --print-golden)

It reads Parquet + similar_to_scaler.parquet + similar_to_cut.parquet + manifest.json and
never opens Ladybug, the bronze CSVs or the gold twin. SIMILAR_TO is re-derived for a
sample of sources with an independent implementation of similar_to/renewal-v1.

Gold content (the source-aware contract for Iceberg-sourced builds): ``gold_content()`` is the
gold row of every renewal as a build carries it on Renewal (key, labels, dates and the 21 numeric
features), ``twin_gold_content()`` the same columns from a gold frame (the pandas twin's, or
Iceberg's), ``content_sha256()`` their canonical hash (floats bit for bit) and ``gold_drift()``
the exact cells in which two of them differ. ``gold_frame()`` turns a build's gold content back
into the frame the builder reads, so the contract can rebuild "the bronze's events + this build's
own gold" and prove a drift explains every difference from a golden.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import io
import json
import os
import sys
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa

from . import manifest as mf
from . import queries, spec

HERO_SUBSCRIPTION = "sub_santosh"
GOLDEN_DIR = Path(__file__).resolve().parent / "goldens"
GOLDEN_VERSION = 1
# Committed golden file -> the (profile, seed, N_USERS) it is generated from (make graph-golden).
GOLDEN_PROFILES = {"tiny": ("tiny", spec.TINY_SEED, spec.TINY_N_USERS),
                   "s42": ("s42", spec.DEFAULT_SEED, spec.DEFAULT_N_USERS)}
SPOT_CHECK_SOURCES = 200
TIE_AWARE_KEYS = {"d2_q": 1}  # compared within +-1 quantum across platforms


# --------------------------------------------------------------------------- loading
def _read(path: Path, schema: pa.Schema) -> pd.DataFrame:
    df = pd.read_parquet(path)
    for f in schema:
        if pa.types.is_date32(f.type):
            df[f.name] = pd.to_datetime(df[f.name])
    return df


def load_tables(build_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Every node/edge table (dates as datetime64) plus the scaler and the rank k+1 rows."""
    from .build import table_files  # file layout only; no builder logic

    d = Path(build_dir)
    return {name: _read(d / rel, schema) for name, rel, schema in table_files()}


def _events(t: dict, rel: str) -> pd.DataFrame:
    """A Subscription->event edge table joined to its renewal(s): + renewal_id, as_of."""
    hr = t["HAS_RENEWAL"][["src", "dst"]].rename(columns={"dst": "renewal_id"})
    r = t["Renewal"][["renewal_id", "as_of", "renewal_date"]]
    return t[rel].merge(hr, on="src").merge(r, on="renewal_id")


def _in_window(e: pd.DataFrame, days: int) -> pd.Series:
    return (e["event_date"] > e["as_of"] - pd.Timedelta(days=days)) & (e["event_date"] <= e["as_of"])


def _naive_window(e: pd.DataFrame, days: int) -> pd.Series:
    return e["event_date"] > e["as_of"] - pd.Timedelta(days=days)


# --------------------------------------------------------------------------- counts
def counts(t: dict) -> dict:
    nodes = {label: len(t[label]) for label in spec.NODE_SCHEMA}
    edges = {rel: len(t[rel]) for rel in spec.EDGE_SCHEMA}
    return {"nodes": nodes, "edges": edges, "total_nodes": sum(nodes.values()), "total_edges": sum(edges.values())}


def routes(t: dict) -> dict:
    r = t["Renewal"]
    model = r[r["route"] == "model"]
    by_plan = model.groupby("plan_tier")["churned"].agg(["size", "sum"])
    return {"routes": {k: int(v) for k, v in sorted(r["route"].value_counts().items())},
            "outcomes": {k: int(v) for k, v in sorted(r["outcome"].value_counts().items())},
            "model_lapses": int(model["churned"].sum()),
            "model_lapses_by_plan": {p: {"n": int(x["size"]), "lapses": int(x["sum"])} for p, x in by_plan.iterrows()}}


# --------------------------------------------------------------------------- point in time
def pit_values(t: dict) -> pd.DataFrame:
    """Per renewal: each graph-verified feature recomputed from edges (pit_*), and naive_*."""
    r = t["Renewal"].set_index("renewal_id")
    out = pd.DataFrame(index=r.index)

    def per_renewal(e: pd.DataFrame, mask: pd.Series) -> pd.Series:
        return e[mask].groupby("renewal_id").size().reindex(r.index).fillna(0).astype(int)

    h = _events(t, "HIT_LIMIT")
    out["pit_limit_hits_14d"] = per_renewal(h, _in_window(h, 14))
    out["naive_limit_hits_14d"] = per_renewal(h, _naive_window(h, 14))
    k = _events(t, "OPENED")
    out["pit_support_tickets_90d"] = per_renewal(k, _in_window(k, 90))
    out["naive_support_tickets_90d"] = per_renewal(k, _naive_window(k, 90))
    x = _events(t, "EXPOSED_TO")
    out["pit_incident_exposed_28d"] = (per_renewal(x, _in_window(x, 28)) > 0).astype(int)
    out["naive_incident_exposed_28d"] = (per_renewal(x, _naive_window(x, 28)) > 0).astype(int)
    c = _events(t, "CHARGED_OVERAGE")
    out["pit_overage_usd_28d"] = (c[_in_window(c, 28)].groupby("renewal_id")["amount_usd"].sum().round(2)
                                  .reindex(r.index).fillna(0.0))
    # Latest setting on/before as_of is 'disabled' and an 'enabled' exists on/before as_of.
    o = _events(t, "CHANGED_OVERAGE")
    o = o[o["event_date"] <= o["as_of"]].sort_values(["renewal_id", "event_date", "dst"], kind="mergesort")
    last_state = o.groupby("renewal_id")["state"].last().reindex(r.index)
    ever_on = o[o["state"] == "enabled"].groupby("renewal_id").size().reindex(r.index).fillna(0)
    out["pit_overage_toggled_off"] = ((last_state == "disabled") & (ever_on > 0)).astype(int)
    # Declared exception: the gold rule is "an edge exists"; the as_of filtered variant differs.
    f = t["FIRST_RENEWAL_AFTER"].merge(t["Renewal"][["renewal_id", "as_of"]], left_on="src", right_on="renewal_id")
    out["pit_first_renewal_after_pricing_change"] = r.index.isin(set(f["src"])).astype(int)
    out["asof_first_renewal_after_pricing_change"] = r.index.isin(
        set(f.loc[f["event_date"] <= f["as_of"], "src"])).astype(int)
    return out


def pit_parity(t: dict) -> dict:
    r = t["Renewal"].set_index("renewal_id")
    v = pit_values(t)
    feats = ["limit_hits_14d", "support_tickets_90d", "overage_usd_28d", "overage_toggled_off",
             "incident_exposed_28d", "first_renewal_after_pricing_change"]
    mism = {}
    for f in feats:
        if f == "overage_usd_28d":
            mism[f] = int((~np.isclose(v[f"pit_{f}"], r[f], rtol=0, atol=1e-9)).sum())
        else:
            mism[f] = int((v[f"pit_{f}"] != r[f]).sum())
    naive = {f: int((v[f"naive_{f}"] != r[f]).sum()) for f in
             ["limit_hits_14d", "incident_exposed_28d", "support_tickets_90d"]}
    return {"parity_mismatches": mism, "naive_mismatches": naive,
            "first_renewal_after_as_of_filtered_mismatches": int(
                (v["asof_first_renewal_after_pricing_change"] != r["first_renewal_after_pricing_change"]).sum())}


def billed(t: dict) -> dict:
    b = _events(t, "BILLED")
    r = t["Renewal"]
    pre = b[b["event_date"] <= b["as_of"]]
    has_sched = r["renewal_id"].isin(set(pre.loc[pre["event_type"] == "cancel_scheduled", "renewal_id"]))
    expected_flag = ~((b["event_type"] == "cancel_scheduled") & (b["event_date"] <= b["as_of"]))
    return {"on_or_before_as_of": len(pre),
            "on_or_before_all_cancel_scheduled": bool((pre["event_type"] == "cancel_scheduled").all()),
            "cancel_flow_iff_mismatches": int((has_sched != (r["route"] == "cancel_flow")).sum()),
            "cancel_flow_renewals": int((r["route"] == "cancel_flow").sum()),
            "outcome_evidence_flag_mismatches": int((b["outcome_evidence"] != expected_flag).sum()),
            "not_outcome_evidence_edges": int((~b["outcome_evidence"]).sum())}


def leak_surface(t: dict) -> dict:
    """Event edges dated after their renewal's as_of (the graph keeps the leak surface on purpose)."""
    per = {}
    for rel in spec.EVENT_RELATIONS:
        e = _events(t, rel)
        post = e["event_date"] > e["as_of"]
        per[rel] = {"edges": len(t[rel]), "post_as_of": int(post.sum()),
                    "renewals": int(e.loc[post, "renewal_id"].nunique())}
    f = t["FIRST_RENEWAL_AFTER"]
    r = t["Renewal"].set_index("renewal_id")
    unknown = ~f["known_by_as_of"]
    flag_bad = int((f["known_by_as_of"] != (f["event_date"] <= f["src"].map(r["as_of"]))).sum())
    per["FIRST_RENEWAL_AFTER"] = {"edges": len(f), "post_as_of": int(unknown.sum()),
                                  "renewals": int(f.loc[unknown, "src"].nunique())}
    return {"by_type": per, "event_edges": sum(v["edges"] for v in per.values()),
            "post_as_of_total": sum(v["post_as_of"] for v in per.values()),
            "known_by_as_of_flag_mismatches": flag_bad,
            "declared_exception_by_change": {k: int(v) for k, v in sorted(
                f.loc[unknown].groupby("dst").size().items())},
            "declared_exception_on_model_rows": int(f.loc[unknown, "src"].map(r["route"]).eq("model").sum())}


# --------------------------------------------------------------------------- SIMILAR_TO
def _independent_top_k(src_row: pd.Series, cand: pd.DataFrame, scaler: pd.DataFrame, k: int) -> list[tuple]:
    """similar_to/renewal-v1 for one source, written without the builder's code path."""
    feats = list(scaler["feature"])
    mean = dict(zip(scaler["feature"], scaler["mean"], strict=True))
    std = dict(zip(scaler["feature"], scaler["std"], strict=True))
    d2 = np.zeros(len(cand), dtype=np.float64)
    for f in feats:  # left to right in FEATURES order
        if std[f] > 0:
            zs = (np.float64(src_row[f]) - mean[f]) / std[f]
            zd = (cand[f].to_numpy(dtype=np.float64) - mean[f]) / std[f]
        else:
            zs, zd = np.float64(0.0), np.zeros(len(cand))
        diff = zs - zd
        d2 = d2 + diff * diff
    q = np.floor(d2 * spec.QUANT + 0.5).astype(np.int64)
    ranked = sorted(zip(q.tolist(), cand["renewal_id"].tolist(), d2.tolist(), strict=True))[:k]
    return [(dst, i + 1, int(qq), dd) for i, (qq, dst, dd) in enumerate(ranked)]


def similar_to_invariants(t: dict) -> dict:
    sim, r = t["SIMILAR_TO"], t["Renewal"].set_index("renewal_id")
    cut, scaler = t["similar_to_cut"], t["similar_to_scaler"]
    ref = r[r["route"] == spec.REFERENCE_ROUTE]
    plan, route = r["plan_tier"], r["route"]
    deg = sim.groupby("src").size().reindex(r.index).fillna(0).astype(int)
    cand_per_plan = ref.groupby("plan_tier").size()
    k_eff = np.minimum(spec.K, plan.map(cand_per_plan).fillna(0).astype(int) - (route == spec.REFERENCE_ROUTE))
    pairs = set(zip(sim["src"], sim["dst"], strict=True))
    mutual = np.fromiter(((b, a) in pairs for a, b in zip(sim["src"], sim["dst"], strict=True)), dtype=bool,
                         count=len(sim))
    indeg = sim.groupby("dst").size()
    top = min(indeg.items(), key=lambda kv: (-kv[1], kv[0])) if len(indeg) else (None, 0)
    g_all = nx.DiGraph()
    g_all.add_nodes_from(r.index)
    g_all.add_edges_from(zip(sim["src"], sim["dst"], strict=True))
    g_ref = nx.Graph()
    g_ref.add_nodes_from(ref.index)
    ref_ids = set(ref.index)
    g_ref.add_edges_from((a, b) for a, b in zip(sim["src"], sim["dst"], strict=True) if a in ref_ids)
    # order and key checks
    s = sim.sort_values(["src", "rank"], kind="mergesort")
    same_src = s["src"].eq(s["src"].shift())
    prev_q, prev_dst = s["d2_q"].shift(), s["dst"].shift()
    unordered = same_src & ((s["d2_q"] < prev_q) | ((s["d2_q"] == prev_q) & (s["dst"] <= prev_dst)))
    rank_bad = (s.groupby("src").cumcount() + 1 != s["rank"]).sum()
    last = s.loc[s.groupby("src")["rank"].idxmax()].set_index("src")
    c = cut.set_index("src")
    lq, ld = last["d2_q"].reindex(c.index), last["dst"].reindex(c.index)
    cut_unordered = (c["d2_q"] < lq) | ((c["d2_q"] == lq) & (c["dst"] <= ld))
    ranked_d2 = np.concatenate([sim["d2"].to_numpy(), cut["d2"].to_numpy()]) * spec.QUANT
    halves = int((np.abs(ranked_d2 - np.floor(ranked_d2) - 0.5) == 0).sum())
    # independent re-derivation for a deterministic sample of sources (+ the hero)
    ids = sorted(r.index)
    step = max(1, len(ids) // SPOT_CHECK_SOURCES)
    sample = sorted(set(ids[::step]) | {i for i in ids if i.startswith(HERO_SUBSCRIPTION + ":")})
    ref_sorted = ref.reset_index().sort_values("renewal_id", kind="mergesort")
    by_src = {k: v for k, v in s[s["src"].isin(sample)].groupby("src")}
    spot_bad = 0
    for sid in sample:
        cand = ref_sorted[(ref_sorted["plan_tier"] == plan[sid]) & (ref_sorted["renewal_id"] != sid)]
        want = _independent_top_k(r.loc[sid], cand, scaler, spec.K)
        got = by_src.get(sid)
        have = [] if got is None else list(zip(got["dst"], got["rank"], got["d2_q"], got["d2"], strict=True))
        # the d2 comparison only runs when the (dst, rank, d2_q) lists are equal, so lengths match
        if [(a, b, c_) for a, b, c_, _ in want] != [(a, int(b), int(c_)) for a, b, c_, _ in have] or \
                any(w[3] != h[3] for w, h in zip(want, have, strict=True)):
            spot_bad += 1
    return {
        "edges": len(sim),
        "sources": int(sim["src"].nunique()),
        "out_degree_histogram": {str(k): int(v) for k, v in sorted(deg.value_counts().items())},
        "out_degree_not_k_eff": int((deg != k_eff).sum()),
        "cross_plan_edges": int((sim["src"].map(plan) != sim["dst"].map(plan)).sum()),
        "dst_not_model": int((sim["dst"].map(route) != spec.REFERENCE_ROUTE).sum()),
        "self_loops": int((sim["src"] == sim["dst"]).sum()),
        "distinct_dst": int(sim["dst"].nunique()),
        "reference_rows": len(ref),
        "reference_never_chosen": int(len(ref) - sim["dst"].nunique()),
        "mutual_pairs": int(mutual.sum() // 2),
        "mutual_flag_mismatches": int((mutual != sim["mutual"].to_numpy()).sum()),
        "undirected_edges": len({(a, b) if a < b else (b, a) for a, b in pairs}),
        "weak_components_all_nodes": int(nx.number_weakly_connected_components(g_all)),
        "reference_components": sorted((len(x) for x in nx.connected_components(g_ref)), reverse=True),
        "max_in_degree": int(top[1]),
        "max_in_degree_renewal": top[0],
        "rank_not_contiguous": int(rank_bad),
        "rank_order_violations": int(unordered.sum()),
        "d2_q_key_mismatches": int((spec.d2_quantise(sim["d2"].to_numpy()) != sim["d2_q"].to_numpy()).sum()),
        "dist_mismatches": int((np.sqrt(sim["d2"].to_numpy()) != sim["dist"].to_numpy()).sum()),
        "spec_version_mismatches": int((sim["spec_version"] != spec.SIMILAR_TO_SPEC_VERSION).sum()),
        "cut_rows": len(cut),
        "cut_order_violations": int(cut_unordered.sum()),
        "cut_quantised_ties_broken_by_dst": int((c["d2_q"] == lq).sum()),
        "exact_halves": halves,
        "spot_check_sources": len(sample),
        "spot_check_mismatches": int(spot_bad),
        "scaler_features": list(scaler["feature"]),
        "scaler_n_ref": int(scaler["n_ref"].iloc[0]) if len(scaler) else 0,
    }


# --------------------------------------------------------------------------- goldens
def hero_renewal(t: dict) -> str | None:
    r = t["Renewal"]
    m = r.loc[r["subscription_id"] == HERO_SUBSCRIPTION, "renewal_id"]
    return m.iloc[0] if len(m) else None


def evidence(t: dict, renewal_id: str) -> list[dict]:
    """PIT evidence rows for one renewal, same shape and order as queries.evidence()."""
    r = t["Renewal"].set_index("renewal_id").loc[renewal_id]
    as_of, rdate = r["as_of"].date(), r["renewal_date"].date()
    sub = t["HAS_RENEWAL"].loc[t["HAS_RENEWAL"]["dst"] == renewal_id, "src"].iloc[0]
    detail_col = {"CHANGED_OVERAGE": "state", "CHARGED_OVERAGE": "amount_usd", "BILLED": "event_type"}
    limit_type = t["LimitHit"].set_index("event_id")["limit_type"]
    out = []
    for rel in spec.EVENT_RELATIONS:
        e = t[rel]
        e = e[(e["src"] == sub) & (e["event_date"] <= r["as_of"])]
        if rel == "BILLED":
            e = e[~e["outcome_evidence"]]
        for x in e.itertuples():
            kw = {}
            if rel == "HIT_LIMIT":
                kw["limit_type"] = limit_type[x.dst]
            elif rel in detail_col:
                kw[detail_col[rel]] = getattr(x, detail_col[rel])
            out.append(queries.evidence_row(rel, x.event_date.date(), x.dst, as_of, rdate,
                                            detail=queries.event_detail(rel, **kw)))
    cc = t["CUT_CAP"]
    for x in cc[(cc["dst"] == r["plan_tier"]) & (cc["event_date"] <= r["as_of"])].itertuples():
        out.append(queries.evidence_row("CUT_CAP", x.event_date.date(), x.src, as_of, rdate,
                                        detail=f"via plan {r['plan_tier']}"))
    f = t["FIRST_RENEWAL_AFTER"]
    for x in f[f["src"] == renewal_id].itertuples():
        out.append(queries.evidence_row("FIRST_RENEWAL_AFTER", x.event_date.date(), x.dst, as_of, rdate,
                                        known_by_as_of=bool(x.known_by_as_of)))
    return sorted(out, key=queries.evidence_sort_key)


def hidden_after_as_of(t: dict, renewal_id: str) -> int:
    n = 0
    for rel in spec.EVENT_RELATIONS:
        e = _events(t, rel)
        n += int(((e["renewal_id"] == renewal_id) & (e["event_date"] > e["as_of"])).sum())
    return n


def top_k(t: dict, renewal_id: str, k: int = spec.K) -> list[dict]:
    r = t["Renewal"].set_index("renewal_id")
    s = t["SIMILAR_TO"]
    s = s[s["src"] == renewal_id].sort_values("rank").head(k)
    return [{"rank": int(x.rank), "renewal_id": x.dst, "d2_q": int(x.d2_q), "outcome": r.at[x.dst, "outcome"],
             "route": r.at[x.dst, "route"]} for x in s.itertuples()]


def nearest_lapses(t: dict, renewal_id: str, n: int = 3) -> list[dict]:
    """Nearest voluntary lapses by weighted shortest path on dist over directed SIMILAR_TO."""
    s = t["SIMILAR_TO"]
    g = nx.DiGraph()
    g.add_weighted_edges_from(zip(s["src"], s["dst"], s["dist"], strict=True), weight="dist")
    if renewal_id not in g:
        return []
    lapsed = set(t["Renewal"].loc[t["Renewal"]["outcome"] == "voluntary_lapse", "renewal_id"])
    dist = nx.single_source_dijkstra_path_length(g, renewal_id, weight="dist")
    hits = sorted((round(float(d), 4), rid) for rid, d in dist.items() if rid in lapsed and rid != renewal_id)
    return [{"renewal_id": rid, "path_dist": d} for d, rid in hits[:n]]


def sharing_neighbours(t: dict, renewal_id: str) -> dict:
    s = t["SIMILAR_TO"]
    mine = set(s.loc[s["src"] == renewal_id, "dst"])
    others = set(s.loc[s["dst"].isin(mine) & (s["src"] != renewal_id), "src"])
    route = t["Renewal"].set_index("renewal_id")["route"]
    return {k: int(v) for k, v in sorted(route.loc[sorted(others)].value_counts().items())}


def exposure_incident(t: dict, incident_id: str) -> dict:
    x = _events(t, "EXPOSED_TO")
    x = x[x["dst"] == incident_id]
    r = t["Renewal"].set_index("renewal_id")
    exposed = r.loc[sorted(set(x.loc[_in_window(x, 28), "renewal_id"]))]
    by_plan = {}
    for plan_tier, g in exposed.groupby("plan_tier"):
        model = g[g["route"] == "model"]
        by_plan[plan_tier] = {"exposed": len(g), "model": len(model),
                              "voluntary_lapses": int(model["churned"].sum()),
                              "cancel_flow": int((g["route"] == "cancel_flow").sum()),
                              "dunning": int((g["route"] == "dunning").sum())}
    first_seen = x.groupby("renewal_id")["event_date"].min()
    naive = int((first_seen > r["as_of"].reindex(first_seen.index)).sum())
    return {"by_plan": by_plan, "total": len(exposed), "naive_additional": naive}


def exposure_pricing_change(t: dict, change_id: str) -> dict:
    f = t["FIRST_RENEWAL_AFTER"]
    f = f[f["dst"] == change_id]
    return {"total": len(f), "known_by_as_of_false": int((~f["known_by_as_of"]).sum())}


def motif_limit_hit_then_overage_off(t: dict) -> dict:
    """Model renewals with a cap hit on or before an overage switch-off, both on/before as_of."""
    h, o = _events(t, "HIT_LIMIT"), _events(t, "CHANGED_OVERAGE")
    first_hit = h[h["event_date"] <= h["as_of"]].groupby("renewal_id")["event_date"].min()
    o = o[(o["event_date"] <= o["as_of"]) & (o["state"] == "disabled")]
    last_off = o.groupby("renewal_id")["event_date"].max()
    j = pd.concat([first_hit.rename("hit"), last_off.rename("off")], axis=1).dropna()
    ids = set(j.index[j["hit"] <= j["off"]])
    r = t["Renewal"]
    m = r[(r["route"] == "model") & r["renewal_id"].isin(ids)]
    n, k = len(m), int(m["churned"].sum())
    return {"renewals": n, "lapses": k, "rate": round(k / n, 4) if n else None}


def first_renewal_after_by_plan(t: dict) -> dict:
    r = t["Renewal"]
    m = r[r["route"] == "model"].copy()
    m["first_after"] = m["renewal_id"].isin(set(t["FIRST_RENEWAL_AFTER"]["src"]))
    out: dict[str, dict] = {}
    for (plan_tier, flag), g in m.groupby(["plan_tier", "first_after"]):
        n, k = len(g), int(g["churned"].sum())
        out.setdefault(plan_tier, {})["with" if flag else "without"] = {"n": n, "lapses": k, "rate": round(k / n, 4)}
    return out


def goldens(t: dict) -> dict:
    hero = hero_renewal(t)
    out: dict = {"hero": None}
    if hero:
        r = t["Renewal"].set_index("renewal_id").loc[hero]
        city = t["Subscription"].set_index("subscription_id").at[r["subscription_id"], "city"]
        top = top_k(t, hero)
        out["hero"] = {
            "renewal_id": hero, "as_of": str(r["as_of"].date()), "renewal_date": str(r["renewal_date"].date()),
            "route": r["route"], "plan_tier": r["plan_tier"], "city": city,
            "evidence": evidence(t, hero),
            "evidence_hidden_after_as_of": hidden_after_as_of(t, hero),
            "top10": top,
            "top10_voluntary_lapses": sum(x["outcome"] == "voluntary_lapse" for x in top),
            "nearest_lapses": nearest_lapses(t, hero),
            "sharing_neighbours_by_route": sharing_neighbours(t, hero),
        }
    out["exposure_incident"] = {i: exposure_incident(t, i) for i in sorted(t["Incident"]["incident_id"])}
    out["exposure_pricing_change"] = {c: exposure_pricing_change(t, c)
                                      for c in sorted(t["PricingChange"]["change_id"])}
    out["motif_limit_hit_then_overage_off"] = motif_limit_hit_then_overage_off(t)
    out["first_renewal_after_by_plan"] = first_renewal_after_by_plan(t)
    return out


def compute(build_dir: str | Path, tables: dict | None = None) -> dict:
    """Everything the contract pins: counts, routes, invariants and golden answers."""
    t = tables or load_tables(build_dir)
    hr = t["HAS_RENEWAL"]
    per_sub = hr.groupby("src").size().reindex(t["Subscription"]["subscription_id"]).fillna(0)
    return {
        "counts": counts(t),
        **routes(t),
        "invariants": {
            "subscriptions_without_exactly_one_renewal": int((per_sub != 1).sum()),
            "has_renewal_as_of_mismatches": int(
                (hr["as_of"] != hr["dst"].map(t["Renewal"].set_index("renewal_id")["as_of"])).sum()),
            "renewal_id_format_mismatches": int((t["Renewal"]["renewal_id"] != t["Renewal"]["subscription_id"] + ":" +
                                                 t["Renewal"]["renewal_date"].dt.strftime("%Y-%m-%d")).sum()),
            "as_of_not_t_minus_7": int(((t["Renewal"]["renewal_date"] - t["Renewal"]["as_of"]).dt.days != 7).sum()),
            **pit_parity(t),
            "billed": billed(t),
            "leak_surface": leak_surface(t),
            "similar_to": similar_to_invariants(t),
        },
        "goldens": goldens(t),
    }


# --------------------------------------------------------------------------- gold content
GOLD_KEY = "subscription_id"
GOLD_DATES = ("as_of", "renewal_date")
GOLD_LABELS = ("plan_tier", "outcome", "route", "churned")
# The gold row of a renewal as Renewal carries it: what build_tables() reads from gold(), in Renewal's order.
GOLD_CONTENT = (GOLD_KEY, "plan_tier", *GOLD_DATES, *spec.NUMERIC_FEATURES, "churned", "outcome", "route")
# Gold features the PIT parity invariant (#2) recomputes from edges: a drift in one breaks parity.
PIT_FEATURES = ("limit_hits_14d", "support_tickets_90d", "overage_usd_28d", "overage_toggled_off",
                "incident_exposed_28d", "first_renewal_after_pricing_change")
GOLD_DRIFT_CELLS_SHOWN = 200   # cells listed in a drift record (all are counted)
_NAN_BITS = np.int64(0x7FF8000000000001)
# The decimals the gold twin rounds each float feature to (scripts/build_churn_gold_local.gold(): round(4),
# overage_usd_28d round(2)). A drift of at most one unit in that last decimal is what two engines rounding
# the same value at a half differently give ("rounding"); anything else is a different value, and so is
# any drift in a PIT feature (drift_kind: invariant #2 recomputes those exactly).
GOLD_FLOAT_DECIMALS = {f: 2 if f == "overage_usd_28d" else 4 for f in spec.NUMERIC_FEATURES
                       if f not in spec.INT_FEATURES}
ROUNDING_SLACK = 1e-9   # relative: 0.7139 + 0.0001 is 1.0000000000287557e-04 away in binary floats


def rounding_unit(column: str) -> float | None:
    """One unit in the last decimal the twin rounds ``column`` to; None for a column that is not a
    rounded float (labels, dates, counts, flags: any difference there is a different value)."""
    d = GOLD_FLOAT_DECIMALS.get(column)
    return None if d is None else 10.0 ** -d


def drift_kind(column: str, build_value, twin_value) -> str:
    """'rounding' when both are numbers of a rounded float column at most one unit in its last decimal
    apart, else 'beyond rounding' (a label, route, churned, date or count that differs, a NaN on one
    side, or a larger float difference).

    A point-in-time feature (PIT_FEATURES: overage_usd_28d is the one rounded float among them) is
    never 'rounding': invariant #2 recomputes it from the build's own edges and compares it exactly
    (atol 1e-9, in pandas and in Cypher), so a cent of drift there fails the contract whatever the
    reason, and the drift record says so instead of calling it harmless. (The alternative, letting
    #2 accept that cent at the recorded cells, would make the PIT invariant depend on a twin the
    build was not made from.)"""
    unit = rounding_unit(column)
    if unit is None or column in PIT_FEATURES or build_value is None or twin_value is None:
        return "beyond rounding"
    a, b = float(build_value), float(twin_value)
    if np.isnan(a) or np.isnan(b):
        return "beyond rounding"
    return "rounding" if abs(a - b) <= unit * (1 + ROUNDING_SLACK) else "beyond rounding"


def _canonical_gold(df: pd.DataFrame) -> pd.DataFrame:
    """GOLD_CONTENT columns with one dtype each (dates as YYYY-MM-DD text, int features and churned
    as int64, the other features as float64, labels as text), sorted by subscription_id."""
    out = {}
    for c in GOLD_CONTENT:
        s = df[c]
        if c in GOLD_DATES:
            out[c] = pd.to_datetime(s).dt.strftime("%Y-%m-%d").astype(object)
        elif c in spec.INT_FEATURES or c == "churned":
            out[c] = s.astype(np.int64)
        elif c in spec.NUMERIC_FEATURES:
            out[c] = pd.Series([np.nan if v is None else float(v) for v in s], index=s.index, dtype=np.float64)
        else:
            out[c] = s.astype(str).astype(object)
    return pd.DataFrame(out).sort_values(GOLD_KEY, kind="mergesort").reset_index(drop=True)


def gold_content(renewal: pd.DataFrame) -> pd.DataFrame:
    """The gold row of every renewal as a build carries it on its Renewal table (GOLD_CONTENT)."""
    return _canonical_gold(renewal)


def twin_gold_content(gold: pd.DataFrame) -> pd.DataFrame:
    """GOLD_CONTENT of a gold frame (scripts/build_churn_gold_local.gold(), or Iceberg's gold table):
    user_id is the subscription_id and feature_as_of the as_of, as build_tables() reads them."""
    cols = {GOLD_KEY: gold["user_id"], "as_of": gold["feature_as_of"]}
    return _canonical_gold(pd.DataFrame({c: cols[c] if c in cols else gold[c] for c in GOLD_CONTENT}))


def gold_frame(content: pd.DataFrame) -> pd.DataFrame:
    """The frame build_tables() reads (user_id, feature_as_of, ...) holding exactly ``content``."""
    g = content.rename(columns={GOLD_KEY: "user_id", "as_of": "feature_as_of"}).copy()
    for c in ("feature_as_of", "renewal_date"):
        g[c] = pd.to_datetime(g[c])
    return g


def _bits(s: pd.Series) -> np.ndarray:
    x = s.to_numpy(dtype=np.float64)
    return np.where(np.isnan(x), _NAN_BITS, x.view(np.int64))


def content_sha256(content: pd.DataFrame) -> str:
    """Canonical sha256 of a GOLD_CONTENT frame: column by column, floats bit for bit (one NaN)."""
    h = hashlib.sha256()
    for c in GOLD_CONTENT:
        s = content[c]
        h.update(f"{c}:{len(s)}\n".encode())
        if s.dtype == np.float64:
            h.update(_bits(s).astype("<i8").tobytes())
        elif s.dtype == np.int64:
            h.update(s.to_numpy().astype("<i8").tobytes())
        else:
            h.update("\x1f".join(map(str, s)).encode())
    return h.hexdigest()


def _plain(v):
    """A cell as JSON: float / int / text."""
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) else float(v)
    if isinstance(v, (np.integer, int)):
        return int(v)
    return str(v)


def gold_drift(build: pd.DataFrame, twin: pd.DataFrame) -> dict:
    """Exactly where a build's gold content differs from a twin's (both GOLD_CONTENT frames).

    Returns {sha256: [build, twin], rows: [build, twin], only_build / only_twin (subscription ids),
    columns: {column: cells}, cells (count) and cell_list: the first GOLD_DRIFT_CELLS_SHOWN cells as
    {subscription_id, column, build, twin, kind}, floats compared bit for bit}. Equal content: cells
    0, no rows on one side only and equal sha256. Each cell's kind (drift_kind()) is 'rounding' or
    'beyond rounding' (a PIT feature is always the latter: invariant #2 recomputes it exactly);
    beyond_rounding counts the latter (by column in beyond_rounding_columns, the
    first GOLD_DRIFT_CELLS_SHOWN of them in beyond_rounding_list), max_abs_delta is per float column.
    """
    a, b = build.set_index(GOLD_KEY), twin.set_index(GOLD_KEY)
    common = a.index.intersection(b.index).sort_values()
    x, y = a.loc[common], b.loc[common]
    cells, columns, deltas = [], {}, {}
    for c in GOLD_CONTENT[1:]:
        bad = (_bits(x[c]) != _bits(y[c])) if x[c].dtype == np.float64 else (x[c].to_numpy() != y[c].to_numpy())
        if bad.any():
            columns[c] = int(bad.sum())
            got = [{GOLD_KEY: sid, "column": c, "build": _plain(x.at[sid, c]), "twin": _plain(y.at[sid, c])}
                   for sid in common[bad]]
            for cell in got:
                cell["kind"] = drift_kind(c, cell["build"], cell["twin"])
            cells += got
            if x[c].dtype == np.float64:
                diff = np.abs(x[c].to_numpy()[bad] - y[c].to_numpy()[bad])
                deltas[c] = None if np.isnan(diff).all() else float(np.nanmax(diff))
    cells.sort(key=lambda d: (d[GOLD_KEY], GOLD_CONTENT.index(d["column"])))
    beyond = [cell for cell in cells if cell["kind"] != "rounding"]
    by_column: dict[str, int] = {}
    for cell in beyond:
        by_column[cell["column"]] = by_column.get(cell["column"], 0) + 1
    return {"sha256": [content_sha256(build), content_sha256(twin)], "rows": [len(a), len(b)],
            "only_build": sorted(set(a.index) - set(b.index)), "only_twin": sorted(set(b.index) - set(a.index)),
            "columns": columns, "cells": len(cells), "cell_list": cells[:GOLD_DRIFT_CELLS_SHOWN],
            "max_abs_delta": deltas, "beyond_rounding": len(beyond), "beyond_rounding_columns": by_column,
            "beyond_rounding_list": beyond[:GOLD_DRIFT_CELLS_SHOWN]}


# --------------------------------------------------------------------------- export cross-check
EXPORT_CELLS_LISTED = 10_000   # mismatching (user_id, column) cells an export cross-check lists


def export_crosscheck(t: dict, export_dir: str | Path) -> dict:
    """Renewal features vs the profile's churn_renewals_audit.csv (built_at excluded).

    The graph frame is written with the export's own pandas CSV writer and compared cell by cell:
    text as written; a numeric feature (or churned) also equal when both cells parse to the same
    number (the Spark export writes decimal(31,4) as '0.8000' where pandas writes '0.8'). The
    result lists the mismatching cells as [user_id, column] (``cells``; up to EXPORT_CELLS_LISTED,
    ``cells_complete`` says whether that is all of them), so a caller can tell a recorded gold
    drift from any other difference.
    """
    d = Path(export_dir)
    audit_path = d / "churn_renewals_audit.csv"
    if not audit_path.is_file():
        return {"status": "missing", "path": str(audit_path)}
    audit = pd.read_csv(audit_path, dtype=str, keep_default_na=False)
    r, s = t["Renewal"], t["Subscription"].set_index("subscription_id")
    g = pd.DataFrame({"user_id": r["subscription_id"], "user_name": r["subscription_id"].map(s["user_name"]),
                      "plan_tier": r["plan_tier"], **{f: r[f] for f in spec.NUMERIC_FEATURES},
                      "churned": r["churned"], "outcome": r["outcome"], "route": r["route"],
                      "feature_as_of": r["as_of"].dt.strftime("%Y-%m-%d"),
                      "renewal_date": r["renewal_date"].dt.strftime("%Y-%m-%d"),
                      "city": r["subscription_id"].map(s["city"])})
    cols = [c for c in audit.columns if c != "built_at"]
    res: dict = {"status": "ok", "path": str(audit_path), "rows": len(audit), "columns": len(cols),
                 "mismatches": {}, "cells": [], "cells_complete": True}
    if set(cols) != set(g.columns):
        res.update(status="mismatch", mismatches={"columns": sorted(set(cols) ^ set(g.columns))}, cells_complete=False)
        return res
    if len(audit) != len(g):
        res.update(status="mismatch", mismatches={"rows": [len(audit), len(g)]}, cells_complete=False)
        return res
    text = g[cols].sort_values("user_id", kind="mergesort").to_csv(index=False)
    mine = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
    theirs = audit[cols].sort_values("user_id", kind="mergesort").reset_index(drop=True)
    if (mine["user_id"].to_numpy() != theirs["user_id"].to_numpy()).any():
        moved = int((mine["user_id"] != theirs["user_id"]).sum())
        res.update(status="mismatch", mismatches={"user_id": {"rows": moved}}, cells_complete=False)
        return res
    numeric = {*spec.NUMERIC_FEATURES, "churned"}
    for c in cols:
        bad = mine[c].to_numpy() != theirs[c].to_numpy()
        if bad.any() and c in numeric:   # the same number written another way ('0.8000' = '0.8') is equal
            x = pd.to_numeric(mine[c].where(bad), errors="coerce").to_numpy(dtype=np.float64)
            y = pd.to_numeric(theirs[c].where(bad), errors="coerce").to_numpy(dtype=np.float64)
            bad &= ~(x == y)
        if bad.any():
            i = int(np.argmax(bad))
            res["mismatches"][c] = {"rows": int(bad.sum()), "example": [theirs.at[i, "user_id"], theirs.at[i, c],
                                                                        mine.at[i, c]]}
            res["cells"] += [[uid, c] for uid in theirs["user_id"].to_numpy()[bad]]
    # the two radar files: model rows and the hero record
    train = d / "churn_user_features.csv"
    if train.is_file():
        ids = set(pd.read_csv(train, usecols=["user_id"], dtype=str)["user_id"])
        want = set(r.loc[r["route"] == "model", "subscription_id"])
        res["train_rows"] = len(ids)
        if ids != want:
            res["mismatches"]["churn_user_features.user_id"] = {"rows": len(ids ^ want)}
            res["cells_complete"] = False
    hero = d / "hero_inference_record.json"
    if hero.is_file():
        rec = json.loads(hero.read_text())
        row = g[g["user_id"] == rec.get("user_id")]
        bad_keys = sorted(k for k, v in rec.items() if len(row) != 1 or k not in row or row.iloc[0][k] != v)
        res["hero_record_keys"] = len(rec)
        if bad_keys:
            res["mismatches"]["hero_inference_record"] = {"keys": bad_keys}
            if len(row) == 1 and all(k in row for k in bad_keys):
                res["cells"] += [[rec["user_id"], k] for k in bad_keys]
            else:
                res["cells_complete"] = False
    if len(res["cells"]) > EXPORT_CELLS_LISTED:
        res["cells"], res["cells_complete"] = res["cells"][:EXPORT_CELLS_LISTED], False
    if res["mismatches"]:
        res["status"] = "mismatch"
    return res


# --------------------------------------------------------------------------- golden files
def golden_document(build_dir: str | Path) -> dict:
    man = mf.read_manifest(build_dir)
    return {
        "golden_version": GOLDEN_VERSION,
        "generated_by": "python -m lakehouse_graph.oracle --build <build_dir> --print-golden (never edit by hand)",
        "spec": man["spec"],
        "key": {"inputs_combined_sha256": man["inputs"]["combined_sha256"], "inputs_sha256": man["inputs"]["sha256"],
                "seed": man["seed"], "n_users": man["n_users"], "seed_n_status": man["seed_n_status"]},
        "measured_on": {"platform": man["platform"], "versions": man["versions"]},
        "values": compute(build_dir),
    }


def golden_text(build_dir: str | Path) -> str:
    """The exact bytes of a golden file for this build (what --print-golden prints)."""
    return json.dumps(golden_document(build_dir), indent=1, sort_keys=True) + "\n"


def refresh_golden(name: str, build_dir: str | Path, *, write: bool = False, golden_dir: Path | None = None,
                   log=print, max_diff_lines: int = 60) -> str:
    """Compare goldens/<name>.json with the oracle's answer for ``build_dir``; write only if asked.

    Prints what differs (golden values, tie-aware for d2_q; key / platform / versions; then a
    unified diff of the file). Returns "unchanged", "differs" (nothing written: ``write`` is
    False) or "written". A golden is never overwritten silently: `make graph-golden` passes
    ``write`` only with CONFIRM=1.
    """
    path = Path(golden_dir or GOLDEN_DIR) / f"{name}.json"
    new_text = golden_text(build_dir)
    new = json.loads(new_text)
    key = new["key"]
    log(f"== golden {name}.json <- build {Path(build_dir).name} (seed {key['seed']}, N_USERS {key['n_users']}, "
        f"{key['seed_n_status']})")
    old_text = path.read_text() if path.is_file() else None
    if old_text == new_text:
        log(f"   unchanged: {path} is byte-identical to the oracle's output for a fresh build")
        return "unchanged"
    if old_text is None:
        log(f"   {path} does not exist yet")
    else:
        try:
            old = json.loads(old_text)
        except ValueError:
            old = {}
        values = diff(old.get("values", {}), new["values"])
        header = diff({k: v for k, v in old.items() if k != "values"}, {k: v for k, v in new.items() if k != "values"})
        log(f"   DIFFERS: {len(values)} golden value(s) and {len(header)} key / platform / version field(s)")
        for line in (values + header)[:max_diff_lines]:
            log(f"     {line}")
        text_diff = list(difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), f"{name}.json (committed)",
                                              f"{name}.json (fresh build)", lineterm="", n=1))
        for line in text_diff[:max_diff_lines]:
            log(f"     {line}")
        if len(text_diff) > max_diff_lines:
            log(f"     ... {len(text_diff) - max_diff_lines} more diff lines")
    if not write:
        log(f"   NOT written: rerun with CONFIRM=1 to overwrite {path}")
        return "differs"
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(new_text)
    os.replace(tmp, path)
    log(f"   WRITTEN: {path} (review the diff, update the plan-number tests in tests/graph/test_contract.py if the "
        f"numbers moved, then run make graph-local)")
    return "written"


def load_goldens(golden_dir: Path | None = None) -> dict[str, dict]:
    d = golden_dir or GOLDEN_DIR
    return {p.stem: json.loads(p.read_text()) for p in sorted(d.glob("*.json"))}


def find_golden(man: dict, golden_dir: Path | None = None) -> tuple[str | None, dict | None, str, bool]:
    """(name, golden, note, exact) for a build.

    exact=True: a committed golden has the same bronze sha256 as this build's inputs.
    exact=False: no golden has these bytes, but one has the same seed / N_USERS (another
    platform, or the generator / renewal model changed): the caller compares it and warns
    when values differ. (None, None, note, False): nothing to compare against.
    """
    all_g = load_goldens(golden_dir)
    want = man["inputs"]["combined_sha256"]
    for name, g in all_g.items():
        if g.get("key", {}).get("inputs_combined_sha256") == want:
            return name, g, f"golden {name}.json (bronze sha256 match)", True
    for name, g in all_g.items():
        if (g.get("key", {}).get("seed"), g.get("key", {}).get("n_users")) == (man.get("seed"), man.get("n_users")):
            return name, g, (f"golden {name}.json (same seed {man.get('seed')} / N_USERS {man.get('n_users')}, "
                             f"but the bronze sha256 differs)"), False
    return None, None, (f"no committed golden for these inputs (seed {man.get('seed')}, N_USERS "
                        f"{man.get('n_users')}); goldens exist for: {', '.join(all_g) or 'none'}"), False


def diff(expected, actual, path: str = "") -> list[str]:
    """Deep comparison; d2_q values are tie-aware (+-1 quantum). Returns human-readable diffs."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        out = []
        for k in sorted(set(expected) | set(actual)):
            p = f"{path}.{k}" if path else str(k)
            if k not in expected:
                out.append(f"{p}: unexpected (got {actual[k]!r})")
            elif k not in actual:
                out.append(f"{p}: missing (golden {expected[k]!r})")
            elif k in TIE_AWARE_KEYS and isinstance(expected[k], int) and isinstance(actual[k], int):
                if abs(expected[k] - actual[k]) > TIE_AWARE_KEYS[k]:
                    out.append(f"{p}: golden {expected[k]!r} != {actual[k]!r}")
            else:
                out.extend(diff(expected[k], actual[k], p))
        return out
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            return [f"{path}: length golden {len(expected)} != {len(actual)}"]
        out = []
        for i, (e, a) in enumerate(zip(expected, actual, strict=True)):
            out.extend(diff(e, a, f"{path}[{i}]"))
        return out
    return [] if expected == actual else [f"{path}: golden {expected!r} != {actual!r}"]


# --------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="pandas oracle for a renewal graph build")
    ap.add_argument("--build", default=None, help="build directory (default: the profile's latest build)")
    ap.add_argument("--profile", default="default")
    ap.add_argument("--graph-root", default=None)
    ap.add_argument("--print-golden", action="store_true", help="print the golden JSON document for this build")
    a = ap.parse_args(argv)
    try:
        bdir = Path(a.build) if a.build else spec.latest_link(a.profile, a.graph_root)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    if not (bdir / mf.MANIFEST_FILE).is_file():
        print(f"no build at {bdir}: run make graph-build PROFILE={a.profile}", file=sys.stderr)
        return 1
    text = golden_text(bdir) if a.print_golden else json.dumps(compute(bdir), indent=1, sort_keys=True) + "\n"
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
