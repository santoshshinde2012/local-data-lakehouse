"""The physically pruned, label-free evidence graph (PLAN Later A): <build>/evidence.lbdb, the only database the
guarded raw-Cypher tool (graph_cypher, cypher_guard.py) may open.

    python scripts/build_evidence_graph.py --profile s42 [--graph-root <dir>]     (make target: see the docs)

What it holds: what the radar model could see at T-7, for every renewal at once, so that even a careless or hostile
query cannot read past a renewal's as_of or read its label:
  * every Subscription->event edge dated AFTER its renewal's as_of is DROPPED (HIT_LIMIT, CHANGED_OVERAGE,
    CHARGED_OVERAGE, OPENED, BILLED, EXPOSED_TO; v1 has one renewal per subscription, which is asserted), and so is
    every event node left without an edge;
  * BILLED outcome evidence is DROPPED (outcome_evidence = true: invoices at renewal, cancellations); what stays is
    the cancel_scheduled events on or before as_of, which the radar itself routes on (point-in-time facts, not the
    label), and the outcome_evidence column goes with the rest;
  * FIRST_RENEWAL_AFTER follows the gold rule and is the single declared exception: the edges whose change took
    effect after as_of (s42: 495, tiny: 5) are KEPT, flagged known_by_as_of = false and declared_exception = true;
  * Renewal loses every label property: churned, outcome, route, is_reference, outcome_observed_on;
    Subscription loses user_name and city (identity, never evidence: resolve names with graph_find);
  * SIMILAR_TO is DROPPED: its candidates are route = 'model' renewals only, so whether a renewal has an incoming
    edge reveals its route, and with it much of the label (evidence.json records the measured share: on s42 a
    renewal with no incoming edge lapsed far more often than the base rate). Neighbours stay with the template tool
    graph_similar_renewals and its visibility rule.
Hubs (Plan, Incident, PricingChange) and CUT_CAP are global calendar facts as of the build's data_end; a query about
one renewal should still compare them with that renewal's as_of.

Proof, recorded in evidence.json and re-checked by the builder on the loaded database: node / edge counts
(Parquet = Ladybug), 0 Subscription->event edges after as_of (pandas and Cypher), exactly the oracle's number of
declared-exception FIRST_RENEWAL_AFTER edges, all flagged, BILLED only cancel_scheduled on or before as_of, no table
or property that names a label (Cypher: show_tables / table_info), and 6-feature point-in-time parity 0 (pandas and
Cypher) computed WITHOUT the as_of upper bound: on the pruned graph a naive traversal is already point-in-time.

Files: <build>/evidence/ (deterministic Parquet, the database's input), <build>/evidence.lbdb and
<build>/evidence.json (the record; also manifest.json["evidence"]). Registered in build.CARRY_OVER at import, so a
byte-identical rebuild of the build keeps them while the record still matches (this module, ladybug, the inputs).
"""
from __future__ import annotations

import contextlib
import importlib.metadata
import json
import os
import shutil
import time
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import build, spec, store
from . import manifest as mf

EVIDENCE_SPEC_VERSION = "evidence-graph/v1"
EVIDENCE_DB = "evidence.lbdb"
EVIDENCE_META = "evidence.json"
EVIDENCE_DIR = "evidence"
MANIFEST_KEY = "evidence"
LABEL_PROPERTIES = ("churned", "outcome", "route", "is_reference", "outcome_observed_on")
IDENTITY_PROPERTIES = ("user_name", "city")
DROPPED_RELATIONS = ("SIMILAR_TO",)
DROPPED_EDGE_PROPERTIES = {"BILLED": ("outcome_evidence",)}
# Words no table or property of the evidence graph may contain (checked on the loaded database).
LABEL_WORDS = ("churn", "outcome", "route", "is_reference", "observed", "lapse", "renewed")
PARITY_FEATURES = ("limit_hits_14d", "support_tickets_90d", "incident_exposed_28d", "overage_usd_28d",
                   "overage_toggled_off", "first_renewal_after_pricing_change")
BUFFER_POOL_MB = 128
_CODE = "src/lakehouse_graph/pruned.py"
_MB = 1024 * 1024


class EvidenceError(RuntimeError):
    """The evidence graph cannot be built or does not hold what it must."""


# --------------------------------------------------------------------------- schema
def node_schema() -> dict[str, spec.NodeSpec]:
    """spec.NODE_SCHEMA without the label and identity properties."""
    drop = set(LABEL_PROPERTIES) | set(IDENTITY_PROPERTIES)
    return {label: replace(n, columns=tuple((c, t) for c, t in n.columns if c not in drop))
            for label, n in spec.NODE_SCHEMA.items()}


def edge_schema() -> dict[str, spec.EdgeSpec]:
    """spec.EDGE_SCHEMA without SIMILAR_TO and BILLED's outcome_evidence; FIRST_RENEWAL_AFTER also carries
    declared_exception (= not known_by_as_of), so the exception is visible to any query that touches it."""
    out = {}
    for rel, e in spec.EDGE_SCHEMA.items():
        if rel in DROPPED_RELATIONS:
            continue
        cols = tuple((c, t) for c, t in e.columns if c not in DROPPED_EDGE_PROPERTIES.get(rel, ()))
        if rel == "FIRST_RENEWAL_AFTER":
            cols = (*cols, ("declared_exception", spec.BOOL))
        out[rel] = replace(e, columns=cols)
    return out


def ddl_statements() -> list[str]:
    out = []
    for n in node_schema().values():
        cols = ", ".join(f"{c} {store._lb_type(t)}" for c, t in n.columns)
        out.append(f"CREATE NODE TABLE {n.label}({cols}, PRIMARY KEY({n.key}))")
    for e in edge_schema().values():
        props = "".join(f", {c} {store._lb_type(t)}" for c, t in e.columns)
        out.append(f"CREATE REL TABLE {e.rel}(FROM {e.src} TO {e.dst}{props})")
    return out


def table_files() -> list[tuple[str, str, object]]:
    """(table, path relative to the build, arrow schema) of the evidence Parquet, in load order."""
    out = [(n.label, f"{EVIDENCE_DIR}/{n.file}", n.schema) for n in node_schema().values()]
    out += [(e.rel, f"{EVIDENCE_DIR}/{e.file}", e.schema) for e in edge_schema().values()]
    return out


# --------------------------------------------------------------------------- pruning (pure)
def _as_of_by_subscription(t: dict) -> pd.Series:
    hr = t["HAS_RENEWAL"]
    if not hr["src"].is_unique:
        raise EvidenceError("renewal-graph/v1 has one renewal per subscription; this build has more, so a "
                            "subscription's events cannot be cut at one as_of (an earlier renewal would see later "
                            "events): refusing to build the evidence graph")
    ren = t["Renewal"].set_index("renewal_id")
    return pd.Series(ren.loc[hr["dst"], "as_of"].to_numpy(), index=hr["src"].to_numpy())


def prune(t: dict) -> tuple[dict[str, pd.DataFrame], dict]:
    """(evidence tables, what was removed) from the build's tables (oracle.load_tables: dates as datetime64)."""
    as_of = _as_of_by_subscription(t)
    out: dict[str, pd.DataFrame] = {}
    removed_edges: dict[str, int] = {}
    kept_targets: dict[str, set] = {}
    for rel in spec.EVENT_RELATIONS:
        e = t[rel]
        when = e["src"].map(as_of)
        if when.isna().any():
            raise EvidenceError(f"{rel}: {int(when.isna().sum())} edges from a subscription without a renewal")
        keep = e["event_date"] <= when
        if rel == "BILLED":
            evidence = keep & ~e["outcome_evidence"].astype(bool)
            if bool((keep & e["outcome_evidence"].astype(bool)).any()):
                raise EvidenceError("BILLED outcome evidence on or before as_of: the build's outcome_evidence flag "
                                    "disagrees with the PIT rule")
            keep = evidence
        out[rel] = e[keep].reset_index(drop=True)
        removed_edges[rel] = int((~keep).sum())
        kept_targets[spec.EDGE_SCHEMA[rel].dst] = kept_targets.get(spec.EDGE_SCHEMA[rel].dst, set()) | set(
            out[rel]["dst"])
    f = t["FIRST_RENEWAL_AFTER"].copy()
    ren_as_of = t["Renewal"].set_index("renewal_id")["as_of"]
    flag = f["event_date"] <= f["src"].map(ren_as_of)
    if bool((flag != f["known_by_as_of"].astype(bool)).any()):
        raise EvidenceError("FIRST_RENEWAL_AFTER: known_by_as_of disagrees with event_date <= as_of")
    f["declared_exception"] = ~f["known_by_as_of"].astype(bool)
    out["FIRST_RENEWAL_AFTER"] = f
    for rel in ("HAS_RENEWAL", "ON_PLAN", "CUT_CAP"):
        out[rel] = t[rel]
    removed_edges["SIMILAR_TO"] = len(t["SIMILAR_TO"])
    removed_nodes: dict[str, int] = {}
    for label, n in node_schema().items():
        df = t[label]
        if label in kept_targets and label not in ("Incident",):      # event nodes: only those still reached
            keep = df[n.key].isin(kept_targets[label])
            removed_nodes[label] = int((~keep).sum())
            df = df[keep]
        out[label] = df[[c for c, _ in n.columns]].reset_index(drop=True)
    report = {"removed_edges": {k: v for k, v in removed_edges.items() if v},
              "removed_nodes": {k: v for k, v in removed_nodes.items() if v},
              "declared_exception_edges": int(f["declared_exception"].sum()),
              "first_renewal_after_edges": len(f)}
    return out, report


def similar_to_route_leak(t: dict) -> dict:
    """Why SIMILAR_TO is dropped, measured: its destinations are route='model' renewals only, so a renewal with no
    incoming edge is far more often a lapse (cancel_flow / dunning) than the base rate."""
    ren = t["Renewal"]
    has_in = ren["renewal_id"].isin(set(t["SIMILAR_TO"]["dst"]))
    labelled = ren[ren["outcome"] != "pending"]
    lapsed = labelled["outcome"].isin(["voluntary_lapse", "involuntary_lapse"])
    no_in = ~labelled["renewal_id"].isin(set(t["SIMILAR_TO"]["dst"]))
    return {"renewals_without_incoming_edge": int((~has_in).sum()),
            "lapse_share_without_incoming_edge": round(float(lapsed[no_in].mean()), 4) if no_in.any() else None,
            "lapse_share_with_incoming_edge": round(float(lapsed[~no_in].mean()), 4) if (~no_in).any() else None,
            "lapse_share_all": round(float(lapsed.mean()), 4) if len(labelled) else None,
            "destinations_route": sorted(set(ren.set_index("renewal_id").loc[sorted(set(t["SIMILAR_TO"]["dst"])),
                                                                          "route"]))}


def counts(tables: dict[str, pd.DataFrame]) -> dict:
    nodes = {label: len(tables[label]) for label in node_schema()}
    edges = {rel: len(tables[rel]) for rel in edge_schema()}
    return {"nodes": nodes, "edges": edges, "total_nodes": sum(nodes.values()), "total_edges": sum(edges.values())}


# --------------------------------------------------------------------------- checks (pandas)
def _per_renewal(t: dict, rel: str) -> pd.DataFrame:
    hr = t["HAS_RENEWAL"][["src", "dst"]].rename(columns={"dst": "renewal_id"})
    return t[rel].merge(hr, on="src").merge(t["Renewal"][["renewal_id", "as_of"]], on="renewal_id")


def pit_values(t: dict, upper_bound: bool) -> pd.DataFrame:
    """The 6 graph-verified features recomputed from the evidence edges. upper_bound=False is the naive traversal
    (no ``event_date <= as_of``): on a physically pruned graph it must already equal gold."""
    r = t["Renewal"].set_index("renewal_id")
    out = pd.DataFrame(index=r.index)

    def window(e: pd.DataFrame, days: int) -> pd.Series:
        lo = e["event_date"] > e["as_of"] - pd.Timedelta(days=days)
        return lo & (e["event_date"] <= e["as_of"]) if upper_bound else lo

    def per(e: pd.DataFrame, mask: pd.Series) -> pd.Series:
        return e[mask].groupby("renewal_id").size().reindex(r.index).fillna(0).astype(int)

    h = _per_renewal(t, "HIT_LIMIT")
    out["limit_hits_14d"] = per(h, window(h, 14))
    k = _per_renewal(t, "OPENED")
    out["support_tickets_90d"] = per(k, window(k, 90))
    x = _per_renewal(t, "EXPOSED_TO")
    out["incident_exposed_28d"] = (per(x, window(x, 28)) > 0).astype(int)
    c = _per_renewal(t, "CHARGED_OVERAGE")
    out["overage_usd_28d"] = (c[window(c, 28)].groupby("renewal_id")["amount_usd"].sum().round(2)
                              .reindex(r.index).fillna(0.0))
    o = _per_renewal(t, "CHANGED_OVERAGE")
    if upper_bound:
        o = o[o["event_date"] <= o["as_of"]]
    o = o.sort_values(["renewal_id", "event_date", "dst"], kind="mergesort")
    last = o.groupby("renewal_id")["state"].last().reindex(r.index)
    ever_on = o[o["state"] == "enabled"].groupby("renewal_id").size().reindex(r.index).fillna(0)
    out["overage_toggled_off"] = ((last == "disabled") & (ever_on > 0)).astype(int)
    f = t["FIRST_RENEWAL_AFTER"]
    out["first_renewal_after_pricing_change"] = r.index.isin(set(f["src"])).astype(int)
    return out


def parity_pandas(t: dict) -> dict:
    r = t["Renewal"].set_index("renewal_id")
    res = {}
    for upper in (False, True):
        v = pit_values(t, upper)
        res["pit_window" if upper else "naive"] = {
            f: int((~np.isclose(v[f], r[f], rtol=0, atol=1e-9)).sum()) if f == "overage_usd_28d"
            else int((v[f] != r[f]).sum()) for f in PARITY_FEATURES}
    return res


def post_as_of_pandas(t: dict) -> dict[str, int]:
    return {rel: int((_per_renewal(t, rel)["event_date"] > _per_renewal(t, rel)["as_of"]).sum())
            for rel in spec.EVENT_RELATIONS}


# --------------------------------------------------------------------------- Ladybug
def load_db(parquet_root: Path, db_path: Path) -> dict:
    """COPY the evidence Parquet into a fresh database (capped pool and threads), CHECKPOINT, close."""
    import ladybug as lb

    if db_path.exists():
        raise FileExistsError(f"{db_path} exists: the evidence loader only writes a fresh database")
    t0 = time.perf_counter()
    db = lb.Database(str(db_path), buffer_pool_size=store.LOAD_BUFFER_POOL_MB * _MB, max_num_threads=store.THREADS)
    conn = lb.Connection(db, num_threads=store.THREADS)
    try:
        for stmt in ddl_statements():
            conn.execute(stmt)
        for name, rel, _ in table_files():
            conn.execute(f"COPY {name} FROM {store._quoted(parquet_root / rel)}")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
        db.close()
    return {"load_s": round(time.perf_counter() - t0, 2), "bytes": db_path.stat().st_size}


def open_db(db_path: Path, buffer_pool_mb: int = BUFFER_POOL_MB):
    """(db, conn): read only, capped pool and threads, the serving query timeout, pybind backend only."""
    import ladybug as lb

    db = lb.Database(str(db_path), read_only=True, buffer_pool_size=buffer_pool_mb * _MB,
                     max_num_threads=store.THREADS, backend="pybind")
    try:
        conn = store.connect(db, store.THREADS, store.QUERY_TIMEOUT_MS)
    except BaseException:
        db.close()
        raise
    return db, conn


def _q(conn, query: str) -> list[list]:
    return store.rows(conn.execute(query))


EVENT_WINDOW_DAYS = {"HIT_LIMIT": 14, "OPENED": 90, "EXPOSED_TO": 28, "CHARGED_OVERAGE": 28}


def cypher_checks(conn) -> dict:
    """The proof, re-run as Cypher on the loaded evidence database."""
    tables = [(row[1], row[2]) for row in _q(conn, "CALL show_tables() RETURN *")]
    props = {name: [row[1] for row in _q(conn, f"CALL table_info('{name}') RETURN *")] for name, _ in tables}
    named = sorted(f"{t}.{p}" for t, ps in props.items() for p in [t, *ps]
                   if any(w in p.lower() for w in LABEL_WORDS))
    n_counts = {label: int(_q(conn, f"MATCH (n:{label}) RETURN count(n)")[0][0]) for label in node_schema()}
    e_counts = {rel: int(_q(conn, f"MATCH ()-[e:{rel}]->() RETURN count(e)")[0][0]) for rel in edge_schema()}
    post = {rel: int(_q(conn, f"MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal) MATCH (s)-[e:{rel}]->() "
                              f"WHERE e.event_date > r.as_of RETURN count(e)")[0][0])
            for rel in spec.EVENT_RELATIONS}
    fra = _q(conn, "MATCH (r:Renewal)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange) RETURN "
                   "sum(CASE WHEN f.event_date > r.as_of THEN 1 ELSE 0 END), "
                   "sum(CASE WHEN f.event_date > r.as_of AND f.declared_exception AND NOT f.known_by_as_of "
                   "THEN 1 ELSE 0 END), "
                   "sum(CASE WHEN f.declared_exception = f.known_by_as_of THEN 1 ELSE 0 END)")[0]
    billed = {str(k): int(n) for k, n in _q(conn, "MATCH (:Subscription)-[:BILLED]->(b:BillingEvent) "
                                                  "RETURN b.event_type, count(*) ORDER BY b.event_type")}
    return {"tables": sorted(name for name, _ in tables), "label_named": named,
            "similar_to_present": "SIMILAR_TO" in props, "counts": {"nodes": n_counts, "edges": e_counts},
            "post_as_of_subscription_event_edges": post,
            "first_renewal_after_post_as_of": int(fra[0] or 0), "first_renewal_after_flagged": int(fra[1] or 0),
            "first_renewal_after_flag_inconsistent": int(fra[2] or 0), "billed_event_types": billed,
            "pit_parity": cypher_parity(conn)}


def cypher_parity(conn) -> dict:
    """6-feature parity on the evidence database: naive (no as_of upper bound) and windowed, vs the gold values."""
    res = {}
    for upper in (False, True):
        def win(rel: str, upper=upper) -> str:
            lo = f"e.event_date > r.as_of - INTERVAL('{EVENT_WINDOW_DAYS[rel]} DAYS')"
            return f"{lo} AND e.event_date <= r.as_of" if upper else lo
        bound = " AND e.event_date <= r.as_of" if upper else ""
        q = {
            "limit_hits_14d": ("HIT_LIMIT", "LimitHit", f"sum(CASE WHEN {win('HIT_LIMIT')} THEN 1 ELSE 0 END)"),
            "support_tickets_90d": ("OPENED", "Ticket", f"sum(CASE WHEN {win('OPENED')} THEN 1 ELSE 0 END)"),
            "incident_exposed_28d": ("EXPOSED_TO", "Incident", f"CASE WHEN sum(CASE WHEN {win('EXPOSED_TO')} THEN 1 "
                                                               f"ELSE 0 END) > 0 THEN 1 ELSE 0 END"),
            "overage_usd_28d": ("CHARGED_OVERAGE", "OverageCharge",
                                f"round(sum(CASE WHEN {win('CHARGED_OVERAGE')} THEN e.amount_usd ELSE 0.0 END), 2)"),
        }
        out = {}
        for feat, (rel, label, expr) in q.items():
            rows = _q(conn, f"MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal) "
                            f"OPTIONAL MATCH (s)-[e:{rel}]->(:{label}) WITH r, {expr} AS v RETURN r.{feat}, v")
            out[feat] = sum(1 for gold, v in rows if not _same(gold, v))
        rows = _q(conn, "MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal) "
                        "OPTIONAL MATCH (s)-[e:CHANGED_OVERAGE]->(:OverageChange) "
                        f"WITH r, max(CASE WHEN e.state = 'disabled'{bound} THEN e.event_date END) AS off, "
                        f"max(CASE WHEN e.state = 'enabled'{bound} THEN e.event_date END) AS on_ "
                        "RETURN r.overage_toggled_off, CASE WHEN off IS NOT NULL AND on_ IS NOT NULL AND off > on_ "
                        "THEN 1 ELSE 0 END")
        out["overage_toggled_off"] = sum(1 for gold, v in rows if not _same(gold, v))
        rows = _q(conn, "MATCH (r:Renewal) OPTIONAL MATCH (r)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange) "
                        "WITH r, count(f) AS n RETURN r.first_renewal_after_pricing_change, "
                        "CASE WHEN n > 0 THEN 1 ELSE 0 END")
        out["first_renewal_after_pricing_change"] = sum(1 for gold, v in rows if not _same(gold, v))
        res["pit_window" if upper else "naive"] = {f: out[f] for f in PARITY_FEATURES}
    return res


def _same(gold, value) -> bool:
    if gold is None or value is None:
        return gold == value
    return abs(float(gold) - float(value)) <= 1e-9


# --------------------------------------------------------------------------- the record
def code_sha256() -> str:
    return mf.sha256_file(spec.repo_root() / _CODE)


def _ladybug_version() -> str | None:
    try:
        return importlib.metadata.version("ladybug")
    except importlib.metadata.PackageNotFoundError:
        return None


def parent_provenance(man: dict) -> dict:
    """The parent build's provenance, without anything the evidence graph does not hold."""
    return {"build_id": man.get("business_build_id"), "profile": man.get("profile"),
            "spec": dict(man.get("spec") or spec.SPEC_VERSIONS), "evidence_spec": EVIDENCE_SPEC_VERSION,
            "inputs_sha256": (man.get("inputs") or {}).get("combined_sha256"),
            "code_sha256": mf.sha256_json(man.get("code_sha256") or {}), "seed": man.get("seed"),
            "n_users": man.get("n_users"), "commit": man.get("commit"), "dirty": man.get("dirty"),
            "data_end": man.get("data_end"), "synthetic": bool(man.get("synthetic", True))}


def read_record(build_dir: str | os.PathLike) -> dict:
    return json.loads((Path(build_dir) / EVIDENCE_META).read_text(encoding="utf-8"))


def verify_record(build_dir: Path, rec: dict, *, check_inputs: bool = True) -> str | None:
    """None when <build>/evidence.lbdb is what its record says and was made from this build, by this code and this
    ladybug; otherwise why not (the carry-over check and the cypher server's start check)."""
    db = build_dir / EVIDENCE_DB
    if rec.get("spec") != EVIDENCE_SPEC_VERSION:
        return f"its record is spec {rec.get('spec')!r}, this code makes {EVIDENCE_SPEC_VERSION}"
    if not db.is_file():
        return f"{EVIDENCE_DB} is missing"
    if mf.sha256_file(db) != (rec.get("db") or {}).get("sha256"):
        return f"{EVIDENCE_DB} differs from the sha256 in {EVIDENCE_META}"
    if rec.get("ladybug") != _ladybug_version():
        return f"made by ladybug {rec.get('ladybug')}, {_ladybug_version()} is installed (rebuild, never migrate)"
    if check_inputs:
        if rec.get("code_sha256") != code_sha256():
            return f"{_CODE} changed since it was built"
        try:
            man = mf.read_manifest(build_dir)
        except (OSError, ValueError) as e:
            return f"the build's manifest.json is unreadable ({type(e).__name__})"
        pinned = {rel: (man.get("files") or {}).get(rel, {}).get("sha256") for rel in rec.get("inputs_sha256", {})}
        if rec.get("business_build_id") != man.get("business_build_id") or pinned != rec.get("inputs_sha256"):
            return "it was made from other Parquet than this build pins"
    return None


def still_valid(old: Path) -> str | None:
    """build.CARRY_OVER check: the evidence files of a replaced build hold for its byte-identical replacement."""
    try:
        rec = read_record(old)
    except (OSError, ValueError) as e:
        return f"its {EVIDENCE_META} is unreadable ({type(e).__name__})"
    return verify_record(old, rec)


build.register_carry_over("evidence", (EVIDENCE_DIR, EVIDENCE_DB, EVIDENCE_META), manifest_key=MANIFEST_KEY,
                          still_valid=still_valid)


# --------------------------------------------------------------------------- build
def _graph_root_of(build_dir: Path) -> Path | None:
    build_dir = build_dir.resolve()
    if build_dir.parent.name == "builds" and spec.is_profile(build_dir.parent.parent.name):
        return build_dir.parents[2]
    return None


def build_evidence(build_dir: str | os.PathLike, *, lock_timeout: float = 600.0, log=print) -> dict:
    """Write <build>/evidence/ + evidence.lbdb + evidence.json (+ manifest.json["evidence"]) and return the record.

    Under the graph root's build lock: the inputs are the build's own Parquet, checked against the sha256 its
    manifest pins; the result is checked (pandas and Cypher) before it replaces anything. An unchanged evidence
    Parquet with a valid database is kept as it is."""
    from . import oracle

    t0 = time.perf_counter()
    bdir = Path(build_dir).absolute()
    root = _graph_root_of(bdir)
    lock = store.BuildLock(root, timeout=lock_timeout, log=log) if root else contextlib.nullcontext()
    with lock:
        try:
            man = mf.read_manifest(bdir)
        except (OSError, ValueError) as e:
            raise EvidenceError(f"{bdir} is not a graph build (no readable manifest.json: {e}); run make "
                                f"graph-build first") from e
        files = man.get("files") or {}
        inputs = {rel: mf.sha256_file(bdir / rel) for rel in sorted(files) if rel.startswith("parquet/")}
        differ = sorted(rel for rel, h in inputs.items() if h != files[rel].get("sha256"))
        if differ:
            raise EvidenceError(f"{bdir}: {', '.join(differ)} differ from the sha256 its manifest.json pins; "
                                f"rebuild it (make graph-build) first")
        t = oracle.load_tables(bdir)
        tables, removed = prune(t)
        leak = similar_to_route_leak(t)
        want_declared = int((~t["FIRST_RENEWAL_AFTER"]["known_by_as_of"].astype(bool)).sum())
        full = {"total_nodes": sum(len(t[label]) for label in spec.NODE_SCHEMA),
                "total_edges": sum(len(t[rel]) for rel in spec.EDGE_SCHEMA)}
        del t
        tmp = bdir / f".evidence.tmp-{os.getpid()}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir()
        try:
            parquet = {rel: {"table": name, "rows": len(tables[name]),
                             "sha256": build.write_parquet(tables[name], tmp / rel, schema)}
                       for name, rel, schema in table_files()}
            written = _read_tables(tmp)
            pandas_checks = {"post_as_of_subscription_event_edges": post_as_of_pandas(written),
                             "pit_parity": parity_pandas(written)}
            cnt = counts(tables)
            old = None
            with contextlib.suppress(OSError, ValueError):
                old = read_record(bdir)
            same = (old is not None and {k: v["sha256"] for k, v in old.get("parquet", {}).items()} ==
                    {k: v["sha256"] for k, v in parquet.items()} and verify_record(bdir, old, check_inputs=False)
                    is None and old.get("code_sha256") == code_sha256())
            if same:
                db_path, loaded = bdir / EVIDENCE_DB, {"load_s": 0.0, "bytes": (bdir / EVIDENCE_DB).stat().st_size}
            else:
                db_path = tmp / EVIDENCE_DB
                loaded = load_db(tmp, db_path)
            db, conn = open_db(db_path)
            try:
                cy = cypher_checks(conn)
            finally:
                conn.close()
                db.close()
            record = {
                "spec": EVIDENCE_SPEC_VERSION, "in_contract": False, "business_build_id": man.get("business_build_id"),
                "provenance": parent_provenance(man), "inputs_sha256": inputs, "code_sha256": code_sha256(),
                "ladybug": _ladybug_version(),
                "db": {"file": EVIDENCE_DB, "sha256": mf.sha256_file(db_path), "bytes": loaded["bytes"]},
                "parquet": parquet, "counts": cnt,
                "full_graph": full,
                "with_similar_to_for_comparison": {"total_nodes": cnt["total_nodes"],
                                                   "total_edges": cnt["total_edges"] + removed["removed_edges"].get(
                                                       "SIMILAR_TO", 0)},
                "removed": removed, "similar_to_dropped": leak,
                "dropped_properties": {"Renewal": list(LABEL_PROPERTIES), "Subscription": list(IDENTITY_PROPERTIES),
                                       **{rel: list(v) for rel, v in DROPPED_EDGE_PROPERTIES.items()}},
                "checks": {"pandas": pandas_checks, "cypher": cy},
                "built_at": mf.utc_now(),
            }
            problems = check_record(record, want_declared)
            if problems:
                raise EvidenceError("the evidence graph does not hold what it must: " + "; ".join(problems))
            if not same:
                _install(tmp, bdir)
            write_json_atomic(bdir / EVIDENCE_META, record)
            mf.write_manifest(bdir, {**mf.read_manifest(bdir), MANIFEST_KEY: summary(record)})
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    took = time.perf_counter() - t0
    log(f"    {'kept the unchanged' if same else 'wrote'} {bdir / EVIDENCE_DB}: {cnt['total_nodes']:,} nodes / "
        f"{cnt['total_edges']:,} edges ({took:.1f} s); {removed['declared_exception_edges']} declared-exception "
        f"FIRST_RENEWAL_AFTER edges kept, flagged")
    return record


def _read_tables(root: Path) -> dict[str, pd.DataFrame]:
    out = {}
    for name, rel, schema in table_files():
        df = pq.read_table(root / rel).to_pandas()
        for f in schema:
            if f.type == spec.DATE:
                df[f.name] = pd.to_datetime(df[f.name])
        out[name] = df
    return out


def _install(tmp: Path, bdir: Path) -> None:
    """Move the new evidence Parquet and database into the build (the record is written after)."""
    final_dir = bdir / EVIDENCE_DIR
    if final_dir.exists():
        shutil.rmtree(final_dir)
    os.replace(tmp / EVIDENCE_DIR, final_dir)
    for p in sorted(bdir.glob(EVIDENCE_DB + ".*")):        # stale sidecars of an older database
        p.unlink()
    os.replace(tmp / EVIDENCE_DB, bdir / EVIDENCE_DB)


def write_json_atomic(path: Path, obj: dict) -> None:
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def summary(record: dict) -> dict:
    """The manifest.json["evidence"] entry: what a reader of the build needs, the full record is evidence.json."""
    c = record["checks"]
    return {"spec": record["spec"], "file": EVIDENCE_DB, "db_sha256": record["db"]["sha256"],
            "total_nodes": record["counts"]["total_nodes"], "total_edges": record["counts"]["total_edges"],
            "declared_exception_edges": record["removed"]["declared_exception_edges"],
            "post_as_of_subscription_event_edges": sum(c["cypher"]["post_as_of_subscription_event_edges"].values()),
            "pit_parity_mismatches": {k: sum(v.values()) for k, v in c["cypher"]["pit_parity"].items()},
            "label_free": not c["cypher"]["label_named"] and not c["cypher"]["similar_to_present"],
            "code_sha256": record["code_sha256"], "built_at": record["built_at"]}


def check_record(record: dict, want_declared: int) -> list[str]:
    """Every invariant the evidence graph must hold (empty list: it holds)."""
    out = []
    c = record["checks"]
    cy, pd_ = c["cypher"], c["pandas"]
    if cy["counts"] != {k: record["counts"][k] for k in ("nodes", "edges")}:
        out.append(f"Ladybug counts {cy['counts']} differ from the Parquet counts")
    for side, post in (("pandas", pd_["post_as_of_subscription_event_edges"]),
                       ("cypher", cy["post_as_of_subscription_event_edges"])):
        if any(post.values()):
            out.append(f"{side}: Subscription->event edges after as_of remain: {post}")
    for side, par in (("pandas", pd_["pit_parity"]), ("cypher", cy["pit_parity"])):
        for mode, mism in par.items():
            if any(mism.values()):
                out.append(f"{side} {mode} PIT parity mismatches: {mism}")
    if record["removed"]["declared_exception_edges"] != want_declared or \
            cy["first_renewal_after_post_as_of"] != want_declared or cy["first_renewal_after_flagged"] != want_declared:
        out.append(f"declared-exception FIRST_RENEWAL_AFTER edges: kept {cy['first_renewal_after_post_as_of']}, "
                   f"flagged {cy['first_renewal_after_flagged']}, the build has {want_declared}")
    if cy["first_renewal_after_flag_inconsistent"]:
        out.append("FIRST_RENEWAL_AFTER: declared_exception is not the negation of known_by_as_of")
    if set(cy["billed_event_types"]) - {"cancel_scheduled"}:
        out.append(f"BILLED holds outcome evidence: {cy['billed_event_types']}")
    if cy["label_named"] or cy["similar_to_present"]:
        out.append(f"a table or property names a label: {cy['label_named']} (SIMILAR_TO: {cy['similar_to_present']})")
    return out


def describe(record: dict) -> str:
    """One line for logs and the cypher tool's description."""
    c = record["counts"]
    return (f"{EVIDENCE_SPEC_VERSION}: {c['total_nodes']:,} nodes / {c['total_edges']:,} edges, label-free, "
            f"cut at each renewal's as_of; {record['removed']['declared_exception_edges']} FIRST_RENEWAL_AFTER edges "
            f"after as_of kept flagged (declared exception)")
