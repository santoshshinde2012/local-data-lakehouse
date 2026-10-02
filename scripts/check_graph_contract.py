#!/usr/bin/env python3
"""Validate a renewal graph build against the graph contract (renewal-graph/v1).

Checks one build (default: the profile's latest) the way check_churn_export.py checks the
export: structural problems always fail; warnings print; ``--strict`` makes warnings fail.

  1. integrity      Parquet sha256 = manifest; build id still matches its sources + code
  2. counts         nodes/edges per label/type; Ladybug counts = Parquet counts
  3. invariants     PLAN 6.6 #1-#6 in pandas (oracle) AND in Cypher (graph.lbdb):
                    point-in-time parity is 0 for six gold features; the naive traversal
                    (no as_of bound) MUST be wrong; FIRST_RENEWAL_AFTER is the one declared
                    exception; BILLED on/before as_of = cancel_flow; SIMILAR_TO spec
  4. goldens        oracle answers = committed goldens/<name>.json, selected by the bronze
                    sha256 (warning when no golden exists for these inputs)
  5. export         Renewal features = the profile's churn_renewals_audit.csv (required
                    under --strict)
  6. determinism    rebuilding from the same inputs + code gives the same business_build_id
                    and byte-identical Parquet
  7. interference   data/sample/churn/* and data/export/* sha256 unchanged
  8. lint           every Cypher template has ORDER BY and LIMIT; tool templates obey the leak
                    rule (queries.lint(): as_of bound on source events, no BILLED outcome
                    evidence, neighbour outcomes only under the visibility rule and about
                    the source only, named properties only, one meaning per name, the
                    calendar cut at a named renewal's as_of, population outcomes as cohort
                    aggregates; queries.py docstring) and are vetted shapes

Source-aware (Iceberg-sourced builds, manifest["iceberg"], lakehouse_graph.iceberg_source):
  * identity    the recorded provenance is complete and consistent (tag, every input and twin
                table with uuid / snapshot / rows, the identity pins = the input snapshots, the
                twin check passed); freshness is recomputed over the Iceberg pins when the build
                has its own identity (manifest.current_identity), else over the bronze;
  * gold        its gold content (oracle.gold_content: Renewal's gold columns) vs the pandas
                twin's gold of its bronze: equal (sha256) -> the golden applies exactly; else
                the drift is recorded with the exact cells: INFO when every cell is rounding
                (a rounded float feature at most one unit in its last decimal from the twin's,
                oracle.drift_kind), a warning naming the cells for anything beyond (a label,
                route, churned, date or count, a larger float difference, a renewal on one
                side only: the lakehouse computed another value), and the golden values are
                DERIVED: the builder on the bronze's events + this build's own gold must give
                this build byte for byte (else a warning), golden values may move only where the
                drifted columns can move them (SIMILAR_TO-derived values for a feature drift,
                anything for a label / date / PIT-feature drift; anything else is an error), and
                every invariant (#1-#6, Cypher = oracle) is computed from the build's own data
                and must hold exactly as for any build;
  * export      a mismatch that is exactly the recorded drift cells is INFO (the exports hold the
                pandas twin's gold); numbers compare by value ('0.8000' = '0.8');
  * determinism re-read the inputs at the recorded tag + snapshot ids and rebuild
                (iceberg_source.verify_build): byte-identical Parquet. Pins that no longer hold
                (tag moved, snapshot expired, rows changed) are an error; no reachable catalog
                (no pyiceberg, nothing configured) a warning. A build that kept the bronze
                identity must also rebuild byte-identically from its CSVs.
The catalog: --catalog-uri / --warehouse, else PYICEBERG_CATALOG__LAKEHOUSE__* (or
.pyiceberg.yaml), else the SQLite catalog the manifest recorded (it holds no credentials).

A strict pass is recorded in <build>/contract.json; `make graph-promote` needs it. A later
non-strict pass does not replace that record (a failure does), and the record stops counting
when the build's Parquet or pinned exports change. The record is written under the build
lock ($GRAPH_ROOT/.lock) after re-checking that the directory still holds the build that was
checked, so a concurrent `--rebuild` can neither drop it nor receive a record of other bytes.

Usage:
  python scripts/check_graph_contract.py [--profile default] [--strict]
  python scripts/check_graph_contract.py --build <build_dir> --strict --json out.json
  python scripts/check_graph_contract.py --build <iceberg build> --strict [--catalog-uri URI --warehouse URI]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import build, iceberg_source, oracle, queries, spec, store  # noqa: E402 (after sys.path)
from lakehouse_graph import manifest as mf  # noqa: E402

EVIDENCE_SAMPLE = 200
DRIFT_CELLS_PRINTED = 20
# Golden values a drift in the SIMILAR_TO features can move (oracle.diff paths): the kNN invariants
# and the hero's neighbourhood. Everything else reads events, labels, dates or PIT features.
SIMILAR_TO_PATHS = ("invariants.similar_to.", "goldens.hero.top10", "goldens.hero.nearest_lapses",
                    "goldens.hero.sharing_neighbours_by_route")


class Report:
    """Collects errors / warnings / info and prints a readable, sectioned summary.

    info is what a reader must see and nothing has to be done about (an Iceberg build's rounding-size
    gold drift vs the pandas twin, with its exact cells): it never fails a check, --strict included."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.infos: list[str] = []
        self.results: dict = {}

    def section(self, title: str) -> None:
        print(f"\n== {title}")

    def ok(self, msg: str) -> None:
        print(f"  ok    {msg}")

    def note(self, msg: str) -> None:
        print(f"  note  {msg}")

    def info(self, msg: str) -> None:
        self.infos.append(msg)
        print(f"  info  {msg}")

    def error(self, msg: str) -> None:
        self.errors.append(msg)
        print(f"  FAIL  {msg}")

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)
        print(f"  WARN  {msg}")

    def check(self, cond: bool, msg: str, detail: str = "") -> bool:
        if cond:
            self.ok(msg)
        else:
            self.error(f"{msg}{': ' + detail if detail else ''}")
        return bool(cond)

    def equal(self, what: str, got, want, source: str = "") -> bool:
        return self.check(got == want, f"{what} = {_fmt(want)}{' (' + source + ')' if source else ''}",
                          f"got {_fmt(got)}, expected {_fmt(want)}")


def _fmt(v) -> str:
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    s = json.dumps(v, sort_keys=True, default=str) if isinstance(v, (dict, list)) else str(v)
    return s if len(s) <= 120 else s[:117] + "..."


# --------------------------------------------------------------------------- 1. integrity
def check_integrity(rep: Report, bdir: Path, man: dict) -> bool:
    rep.section("Integrity and identity")
    bad = [rel for rel, meta in man["files"].items()
           if not (bdir / rel).is_file() or mf.sha256_file(bdir / rel) != meta["sha256"]]
    rep.check(not bad, f"{len(man['files'])} Parquet files match the manifest sha256", f"changed or missing: {bad}")
    rep.check(man.get("synthetic") is True, "manifest: synthetic = true")
    for key in ("business_build_id", "spec", "inputs", "code_sha256", "versions", "platform", "seed", "n_users",
                "seed_n_status", "commit", "dirty", "data_end", "exports", "builder", "ladybug", "pit_rule"):
        if key not in man:
            rep.error(f"manifest is missing '{key}'")
    rep.equal("spec versions", {k: man["spec"].get(k) for k in spec.SPEC_VERSIONS}, dict(spec.SPEC_VERSIONS))
    if man.get("iceberg") is not None:
        check_iceberg_provenance(rep, man)
    try:
        ident = mf.current_identity(man)
    except (KeyError, TypeError, ValueError) as e:
        rep.error(f"the build's identity cannot be recomputed from its manifest ({type(e).__name__}: {e})")
        return False
    fresh = ident["business_build_id"] == man["business_build_id"]
    over = "Iceberg input pins, bronze" if mf.iceberg_pins(man) is not None else "inputs"
    if fresh:
        rep.ok(f"business_build_id {man['business_build_id']} = sha256({over}, code, spec, versions, platform)")
    else:
        was = _recorded_identity(man)
        changed = _changed(ident["payload"], was)
        how = "make graph-build" if mf.iceberg_pins(man) is None else "build_graph_local.py build --source iceberg"
        if mf.iceberg_pins(man) is not None and mf.sha256_json(was)[:12] != man["business_build_id"]:
            rep.error(f"the recorded identity payload (iceberg.identity_payload) does not hash to business_build_id "
                      f"{man['business_build_id']}: the manifest was edited after the build")
        rep.warn(f"stale build: business_build_id would now be {ident['business_build_id']} "
                 f"(changed: {', '.join(changed) or 'nothing the manifest records'}); run {how}")
    how = man.get("seed_n_verification") or man.get("seed_n_source")
    rep.note(f"seed {man['seed']}, N_USERS {man['n_users']} ({man['seed_n_status']}: {how}); "
             f"commit {man['commit']}{' +dirty' if man['dirty'] else ''}; data_end {man['data_end']}; "
             f"platform {man['platform']}; {', '.join(f'{k} {v}' for k, v in sorted(man['versions'].items()))}")
    return fresh


def _recorded_identity(man: dict) -> dict:
    """The identity payload the build was given: the Iceberg one it recorded, or the bronze one rebuilt
    from the manifest's own fields (inputs, code, spec, params, versions, platform)."""
    payload = (man.get("iceberg") or {}).get("identity_payload")
    if payload is not None:
        return payload
    return {"inputs": man["inputs"]["sha256"], "code": man["code_sha256"],
            "spec": {k: man["spec"].get(k) for k in spec.SPEC_VERSIONS}, "params": man.get("params"),
            "versions": man["versions"], "platform": man["platform"]}


def _changed(now: dict, was: dict) -> list[str]:
    """Identity payload parts that differ (versions compared on the identity's packages only)."""
    out = []
    for key, value in now.items():
        old = was.get(key)
        if key == "versions" and isinstance(old, dict):
            old = {name: old.get(name) for name in value}
        if value != old:
            out.append(key)
    return out


def check_iceberg_provenance(rep: Report, man: dict) -> None:
    """An Iceberg-sourced build's recorded provenance is complete and consistent with its identity."""
    ice, ib = man["iceberg"], iceberg_source
    tag, tables = ice.get("tag"), ice.get("tables") or {}
    inputs = {t: p for t, p in tables.items() if p.get("role") == "input"}
    outputs = {t: p for t, p in tables.items() if p.get("role") == "output"}
    want_in = {f"{ib.CATALOG}.{t}" for t, _ in ib.SILVER.values()} | {f"{ib.CATALOG}.{ib.GOLD_TABLE}"}
    want_out = {f"{ib.CATALOG}.{t}" for t in (ib.NODES_TABLE, ib.EDGES_TABLE, ib.SCALER_TABLE)}
    bad = []
    if not ib.TAG_RE.match(str(tag or "")):
        bad.append(f"tag {tag!r} is not graph_<12 hex>")
    if set(inputs) != want_in or set(outputs) != want_out:
        odd = sorted((set(inputs) ^ want_in) | (set(outputs) ^ want_out))
        bad.append(f"tables read differ from the publish's: {odd}")
    loose = sorted(t for t, p in tables.items()
                   if p.get("tag") != tag or not p.get("table_uuid") or type(p.get("snapshot_id")) is not int
                   or type(p.get("rows")) is not int)
    if loose:
        bad.append(f"tables without their tag / table uuid / snapshot id / row count: {loose}")
    pins = mf.iceberg_pins(man)
    own = str(ice.get("identity", "")).startswith("iceberg")
    if own != (pins is not None):
        bad.append(f"identity {ice.get('identity')!r} with{'' if pins is not None else 'out'} identity pins")
    if pins is not None and pins != {t: p.get("snapshot_id") for t, p in inputs.items()}:
        bad.append("the identity pins are not the snapshot ids of the input tables read")
    if not (ice.get("twin") or {}).get("ok"):
        bad.append("the twin check (gold.graph_* = this build on the same inputs) did not pass")
    rep.check(not bad, f"source: Iceberg {tag} (lakehouse build {ice.get('lakehouse_build_id')}, published "
                       f"{ice.get('published_at')}): {len(inputs)} inputs + {len(outputs)} twin tables read by tag + "
                       f"snapshot id (table uuid, snapshot, rows recorded); twin = this build (SIMILAR_TO tie-only); "
                       f"identity: {ice.get('identity')}", "; ".join(bad))
    code = ice.get("code") or {}
    if code and not code.get("matches_checkout"):
        rep.info(f"{tag} was published by other job / SQL bytes than this checkout's (iceberg.code records both); "
                 f"the twin check above compared its output with this builder's")


# --------------------------------------------------------------------------- 2-3. pandas
def check_oracle(rep: Report, man: dict, v: dict) -> None:
    c, inv = v["counts"], v["invariants"]
    rep.section("Counts (Parquet)")
    rep.equal("manifest counts", man["counts"], c, "oracle recount")
    n, e = c["nodes"], c["edges"]
    rep.ok(f"{c['total_nodes']:,} nodes: " + ", ".join(f"{k} {x:,}" for k, x in n.items()))
    rep.ok(f"{c['total_edges']:,} edges: " + ", ".join(f"{k} {x:,}" for k, x in e.items()))
    rep.check(n["Subscription"] == n["Renewal"] == e["HAS_RENEWAL"] == e["ON_PLAN"],
              "count(Subscription) = count(Renewal) = count(HAS_RENEWAL) = count(ON_PLAN)", _fmt(c))
    for node, rel in (("LimitHit", "HIT_LIMIT"), ("OverageChange", "CHANGED_OVERAGE"),
                      ("OverageCharge", "CHARGED_OVERAGE"), ("Ticket", "OPENED"), ("BillingEvent", "BILLED")):
        rep.check(n[node] == e[rel], f"count({node}) = count({rel})", f"{n[node]} vs {e[rel]}")
    rep.check(e["CUT_CAP"] == n["PricingChange"] * n["Plan"], "count(CUT_CAP) = PricingChange x Plan")
    rep.ok("routes " + ", ".join(f"{k} {x:,}" for k, x in v["routes"].items())
           + f"; voluntary lapses on model rows {v['model_lapses']:,}")

    rep.section("Invariants (pandas oracle)")
    rep.equal("#1 subscriptions without exactly one renewal", inv["subscriptions_without_exactly_one_renewal"], 0)
    rep.equal("#1 HAS_RENEWAL.as_of != Renewal.as_of", inv["has_renewal_as_of_mismatches"], 0)
    rep.equal("#1 renewal_id != subscription_id:renewal_date", inv["renewal_id_format_mismatches"], 0)
    rep.equal("#1 renewals where as_of != renewal_date - 7", inv["as_of_not_t_minus_7"], 0)
    for f, m in inv["parity_mismatches"].items():
        rule = "gold rule, declared exception" if f == "first_renewal_after_pricing_change" else \
            "event_date <= as_of + window"
        rep.equal(f"#2 PIT parity mismatches {f} ({rule})", m, 0)
    ls = inv["leak_surface"]
    by = ls["by_type"]
    fra_post = by["FIRST_RENEWAL_AFTER"]["post_as_of"]
    rep.equal("#2 first_renewal_after mismatches IF as_of-filtered (= declared-exception edges)",
              inv["first_renewal_after_as_of_filtered_mismatches"], fra_post)
    rep.equal("#2 known_by_as_of flag != (effective_date <= as_of)", ls["known_by_as_of_flag_mismatches"], 0)
    nv = inv["naive_mismatches"]
    rep.equal("#3 naive limit_hits_14d wrong (= renewals with a post-as_of cap hit)", nv["limit_hits_14d"],
              by["HIT_LIMIT"]["renewals"])
    rep.equal("#3 naive support_tickets_90d wrong (= renewals with a post-as_of ticket)", nv["support_tickets_90d"],
              by["OPENED"]["renewals"])
    rep.check(0 < nv["incident_exposed_28d"] <= by["EXPOSED_TO"]["renewals"],
              f"#3 naive incident_exposed_28d wrong in {nv['incident_exposed_28d']:,} renewals "
              f"(0 < n <= {by['EXPOSED_TO']['renewals']:,} with post-as_of exposure)")
    for f, m in nv.items():
        if m == 0:
            rep.error(f"#3 the naive traversal for {f} is no longer wrong: the leak fixture is gone")
    b = inv["billed"]
    rep.check(b["on_or_before_all_cancel_scheduled"],
              f"#4 BILLED on/before as_of: {b['on_or_before_as_of']:,} edges, all cancel_scheduled")
    rep.equal("#4 route = cancel_flow iff a cancel_scheduled on/before as_of (mismatches)",
              b["cancel_flow_iff_mismatches"], 0)
    rep.equal("#4 outcome_evidence flag mismatches", b["outcome_evidence_flag_mismatches"], 0)
    rep.equal("#4 BILLED edges that are not outcome evidence", b["not_outcome_evidence_edges"],
              b["on_or_before_as_of"])
    rep.ok(f"#5 post-as_of edges {ls['post_as_of_total']:,} of {ls['event_edges']:,}: "
           + ", ".join(f"{k} {x['post_as_of']:,} ({x['renewals']:,} renewals)" for k, x in by.items()))
    s = inv["similar_to"]
    rep.ok(f"#6 SIMILAR_TO {s['edges']:,} edges, out-degree {s['out_degree_histogram']}, "
           f"{s['distinct_dst']:,} distinct dst ({s['reference_never_chosen']:,} of {s['reference_rows']:,} "
           f"reference never chosen), {s['mutual_pairs']:,} mutual pairs, {s['undirected_edges']:,} undirected, "
           f"{s['weak_components_all_nodes']} weak components, reference components {s['reference_components']}, "
           f"max in-degree {s['max_in_degree']} ({s['max_in_degree_renewal']}), "
           f"{s['cut_quantised_ties_broken_by_dst']} quantised ties at the cut")
    for key, label in (("out_degree_not_k_eff", "sources whose out-degree != k_eff"),
                       ("cross_plan_edges", "cross-plan edges"), ("dst_not_model", "dst with route != model"),
                       ("self_loops", "self loops"), ("mutual_flag_mismatches", "mutual flag mismatches"),
                       ("rank_not_contiguous", "non-contiguous ranks"),
                       ("rank_order_violations", "rank order violations of (d2_q, dst)"),
                       ("d2_q_key_mismatches", "d2_q != floor(d2*1e9 + 0.5)"),
                       ("dist_mismatches", "dist != sqrt(d2)"), ("spec_version_mismatches", "spec_version mismatches"),
                       ("cut_order_violations", "rank k+1 candidates ordered before rank k"),
                       ("spot_check_mismatches",
                        f"independent re-derivation mismatches ({s['spot_check_sources']} sources)")):
        rep.equal(f"#6 {label}", s[key], 0)
    rep.equal("#6 scaler features", s["scaler_features"], list(spec.FEATURES))
    rep.equal("#6 scaler fitted on the reference set (n_ref)", s["scaler_n_ref"], s["reference_rows"])
    if s["exact_halves"]:
        rep.warn(f"#6 {s['exact_halves']} ranked candidates sit exactly on a quantisation half "
                 f"(frac(d2*1e9) == 0.5): engines may round them differently")
    else:
        rep.ok("#6 ranked candidates with frac(d2*1e9) == 0.5 = 0")


# --------------------------------------------------------------------------- 3. Cypher
def _mism(rows: list[dict], col: str, float_tol: float | None = None) -> int:
    if float_tol is not None:
        return int(sum(abs(float(r[col]) - float(r["gold"])) > float_tol for r in rows))
    return int(sum(int(r[col]) != int(r["gold"]) for r in rows))


def check_cypher(rep: Report, bdir: Path, v: dict, tables: dict) -> None:
    rep.section("Ladybug (Cypher on graph.lbdb, read only) = pandas oracle")
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        def q(name, params=None, **ids):  # the contract is the one caller of the contract-only templates
            return queries.fetch(conn, name, params, contract=True, **ids)

        c, inv = v["counts"], v["invariants"]
        got = {label: q("count_nodes", label=label)[0]["n"] for label in spec.NODE_SCHEMA}
        rep.equal("Ladybug node counts", got, c["nodes"], "Parquet")
        got = {rel: q("count_rels", rel=rel)[0]["n"] for rel in spec.EDGE_SCHEMA}
        rep.equal("Ladybug edge counts", got, c["edges"], "Parquet")
        rr = q("routes")
        rep.equal("routes", {r["route"]: int(r["n"]) for r in rr}, v["routes"])
        rep.equal("voluntary lapses on model rows", sum(int(r["lapses"]) for r in rr if r["route"] == "model"),
                  v["model_lapses"])
        per = {int(r["renewals"]): int(r["subscriptions"]) for r in q("renewals_per_subscription")}
        rep.equal("#1 renewals per subscription", per, {1: c["nodes"]["Subscription"]})

        pv = oracle.pit_values(tables)
        nv = inv["naive_mismatches"]
        for feat, tol in (("limit_hits_14d", None), ("support_tickets_90d", None), ("incident_exposed_28d", None),
                          ("overage_usd_28d", 1e-9), ("overage_toggled_off", None)):
            rows = q(f"contract_pit_{feat}")
            rep.equal(f"#2 PIT parity mismatches {feat} over {len(rows):,} renewals", _mism(rows, "pit", tol), 0)
            mine = np.array([float(r["pit"]) for r in rows])
            theirs = pv[f"pit_{feat}"].reindex([r["renewal_id"] for r in rows]).to_numpy(dtype=float)
            rep.equal(f"   per-renewal Cypher value != pandas value ({feat})",
                      int((np.abs(mine - theirs) > 1e-9).sum()), 0)
            if feat in nv:
                rep.equal(f"#3 naive {feat} wrong", _mism(rows, "naive"), nv[feat], "oracle")
        rows = q("contract_pit_first_renewal_after")
        rep.equal("#2 PIT parity mismatches first_renewal_after_pricing_change (gold rule)", _mism(rows, "pit"), 0)
        rep.equal("#2 first_renewal_after mismatches IF as_of-filtered", _mism(rows, "as_of_filtered"),
                  inv["first_renewal_after_as_of_filtered_mismatches"], "oracle")
        rep.equal("#2 known_by_as_of flag inconsistencies", sum(int(r["flag_inconsistent"]) for r in rows), 0)
        rows = q("contract_billed_by_renewal")
        b = inv["billed"]
        rep.equal("#4 BILLED on/before as_of", sum(int(r["on_or_before"]) for r in rows), b["on_or_before_as_of"],
                  "oracle")
        rep.equal("#4 ... that are cancel_scheduled", sum(int(r["scheduled"]) for r in rows),
                  b["on_or_before_as_of"])
        rep.equal("#4 route = cancel_flow iff such an edge (mismatches)",
                  sum((int(r["scheduled"]) > 0) != (r["route"] == "cancel_flow") for r in rows), 0)
        rep.equal("#4 BILLED edges that are not outcome evidence", sum(int(r["not_outcome_evidence"]) for r in rows),
                  b["not_outcome_evidence_edges"], "oracle")
        by = inv["leak_surface"]["by_type"]
        got = {}
        for rel in spec.EVENT_RELATIONS:
            r0 = (q("contract_post_as_of_edges", rel=rel) or [{}])[0]
            got[rel] = {"post_as_of": int(r0.get("edges") or 0), "renewals": int(r0.get("renewals") or 0)}
        rep.equal("#5 post-as_of edges by type", got,
                  {k: {"post_as_of": x["post_as_of"], "renewals": x["renewals"]} for k, x in by.items()
                   if k in spec.EVENT_RELATIONS}, "oracle")
        rep.equal("#5 declared-exception FIRST_RENEWAL_AFTER edges by change",
                  {r["change_id"]: int(r["edges"]) for r in q("contract_first_renewal_after_unknown")},
                  inv["leak_surface"]["declared_exception_by_change"], "oracle")

        s = inv["similar_to"]
        rep.equal("#6 out-degree histogram", {str(r["out_degree"]): int(r["sources"]) for r in q("similar_out_degree")},
                  s["out_degree_histogram"], "oracle")
        e = q("similar_edge_checks")[0]
        rep.equal("#6 edges / cross-plan / dst not model / self loops / distinct dst",
                  [int(e[k]) for k in ("edges", "cross_plan", "dst_not_model", "self_loops", "distinct_dst")],
                  [s["edges"], 0, 0, 0, s["distinct_dst"]], "oracle")
        rep.equal("#6 mutual pairs (pattern match)", int(q("similar_mutual_edges")[0]["mutual_edges"]) // 2,
                  s["mutual_pairs"], "oracle")
        rep.equal("#6 mutual pairs (stored flag)", int(e["mutual_flagged"]) // 2, s["mutual_pairs"], "oracle")
        top = (q("similar_max_in_degree") or [{"renewal_id": None, "in_degree": 0}])[0]
        rep.equal("#6 max in-degree", [top["renewal_id"], int(top["in_degree"])],
                  [s["max_in_degree_renewal"], s["max_in_degree"]], "oracle")

        g = v["goldens"]
        ids = sorted(tables["Renewal"]["renewal_id"])
        step = max(1, len(ids) // EVIDENCE_SAMPLE)
        hero = (g.get("hero") or {}).get("renewal_id")
        sample = sorted(set(ids[::step]) | ({hero} if hero else set()))
        bad = [rid for rid in sample if queries.evidence(conn, rid) != oracle.evidence(tables, rid)]
        rep.check(not bad, f"evidence rows identical for {len(sample)} sampled renewals (Cypher templates vs pandas)",
                  f"differs for {bad[:5]}")
        if hero:
            h = g["hero"]
            p = {"renewal_id": hero}
            rep.equal("hero evidence (ordered rows)", queries.evidence(conn, hero), h["evidence"], "oracle")
            rep.equal("hero events hidden after as_of", int(q("contract_evidence_hidden_after_as_of", p)[0]["hidden"]),
                      h["evidence_hidden_after_as_of"], "oracle")
            top10 = [{"rank": int(r["rank"]), "renewal_id": r["renewal_id"], "d2_q": int(r["d2_q"]),
                      "outcome": r["outcome"], "route": r["route"]} for r in q("similar_top_k", {**p, "k": spec.K})]
            rep.equal("hero top-10 (rank, renewal, d2_q, outcome)", top10, h["top10"], "oracle")
            near = [{"renewal_id": r["renewal_id"], "path_dist": round(float(r["path_dist"]), 4)}
                    for r in q("similar_nearest_lapses", {**p, "k": 3})]
            rep.equal("hero nearest lapses (weighted shortest path on dist)", near, h["nearest_lapses"], "oracle")
            rep.equal("renewals sharing a hero neighbour, by route",
                      {r["route"]: int(r["renewals"]) for r in q("similar_sharing_neighbours", p)},
                      h["sharing_neighbours_by_route"], "oracle")
        for inc, want in g["exposure_incident"].items():
            rows = q("exposure_incident_by_plan", {"incident_id": inc})
            got = {r["plan_tier"]: {k: int(r[k]) for k in ("exposed", "model", "voluntary_lapses", "cancel_flow",
                                                            "dunning")} for r in rows}
            nrow = (q("contract_exposure_incident_naive", {"incident_id": inc}) or [{"naive_additional": 0}])[0]
            rep.equal(f"{inc} exposure by plan + naive extra", [got, int(nrow["naive_additional"])],
                      [want["by_plan"], want["naive_additional"]], "oracle")
        for ch, want in g["exposure_pricing_change"].items():
            rows = q("exposure_pricing_change", {"change_id": ch})
            got = {"total": sum(int(r["renewals"]) for r in rows),
                   "known_by_as_of_false": sum(int(r["renewals"]) for r in rows if not r["known_by_as_of"])}
            rep.equal(f"{ch} first-renewal-after totals", got, want, "oracle")
        m = q("motif_limit_hit_then_overage_off")[0]
        want = g["motif_limit_hit_then_overage_off"]
        rep.equal("motif cap hit then overage off (renewals, lapses)", [int(m["renewals"]), int(m["lapses"] or 0)],
                  [want["renewals"], want["lapses"]], "oracle")
        got = {}
        for r in q("first_renewal_after_by_plan"):
            n_, k_ = int(r["n"]), int(r["lapses"])
            got.setdefault(r["plan_tier"], {})["with" if r["first_after"] else "without"] = {
                "n": n_, "lapses": k_, "rate": round(k_ / n_, 4)}
        rep.equal("first renewal after a cap cut, by plan", got, g["first_renewal_after_by_plan"], "oracle")
    finally:
        conn.close()
        db.close()


# --------------------------------------------------------------------------- 4-8
def check_gold_source(rep: Report, man: dict, tables: dict) -> dict | None:
    """An Iceberg-sourced build: its gold content vs the pandas twin's gold of its bronze.

    Equal (sha256) means a golden for these bronze bytes applies exactly. A difference of rounding
    size (a rounded float feature at most one unit in its last decimal from the twin's: Spark and
    pandas rounding the same value at a half differently, oracle.drift_kind) is not a defect of the
    build: it is recorded as INFO with the exact cells. Any other difference (a label, route,
    churned, date or count, a NaN on one side, a larger float difference, a renewal on one side
    only) is a value the lakehouse computed differently from the gold the graph is specified on: a
    warning naming the cells (strict fails), recorded with them. Returns {drift, silver, today,
    consts, sdir} for the golden and export checks, or None when there is no bronze to run the
    pandas twin on (a warning)."""
    rep.section("Gold content (Iceberg source vs the pandas twin)")
    sdir = mf.resolve_path(man["inputs"]["sample_dir"])
    where = mf.display_path(sdir)
    if not all((sdir / f).is_file() for f in spec.BRONZE_FILES):
        rep.warn(f"gold drift not measured: no bronze CSVs in {where} to run the pandas twin on, so no golden can be "
                 f"applied or derived")
        return None
    mod = build.load_gold_twin(sdir)
    try:
        silver, gold, today = build.run_gold(mod)
    except (build.GraphBuildError, KeyError, ValueError, TypeError) as e:
        rep.warn(f"gold drift not measured: the pandas twin cannot run on {where} ({type(e).__name__}: "
                 f"{str(e)[:200]}), so no golden can be applied or derived")
        return None
    drift = oracle.gold_drift(oracle.gold_content(tables["Renewal"]), oracle.twin_gold_content(gold))
    rep.results["gold_drift"] = drift
    mine, twin = drift["sha256"]
    rows = drift["rows"][0]
    if not (drift["cells"] or drift["only_build"] or drift["only_twin"]):
        rep.ok(f"gold content sha256 {mine[:16]} = the pandas twin's on {where} ({rows:,} renewals x "
               f"{len(oracle.GOLD_CONTENT) - 1} gold columns, floats bit for bit)")
    else:
        cols = ", ".join(f"{c} {n}" for c, n in drift["columns"].items()) or "none"
        beyond = drift["beyond_rounding"]
        rep.info(f"gold drift vs the pandas twin on {where}: {drift['cells']:,} cell(s) differ ({cols}); "
                 f"{drift['cells'] - beyond:,} within rounding, {beyond:,} beyond; renewals only "
                 f"in this build {drift['only_build'][:5] or 'none'}, only in the twin "
                 f"{drift['only_twin'][:5] or 'none'}; "
                 f"gold sha256 {mine[:16]} here, {twin[:16]} in the twin")
        for c in drift["cell_list"][:DRIFT_CELLS_PRINTED]:
            rep.info(f"   {c[oracle.GOLD_KEY]} {c['column']}: {c['build']!r} in this build, {c['twin']!r} in the "
                     f"pandas twin ({c['kind']})")
        if drift["cells"] > DRIFT_CELLS_PRINTED:
            rep.info(f"   ... {drift['cells'] - DRIFT_CELLS_PRINTED:,} more cell(s) in the --json document "
                     f"(gold_drift.cell_list, first {oracle.GOLD_DRIFT_CELLS_SHOWN})")
        if beyond or drift["only_build"] or drift["only_twin"]:
            units = ", ".join(sorted({f"{u:g}" for u in map(oracle.rounding_unit, oracle.GOLD_FLOAT_DECIMALS)}))
            shown = "; ".join(f"{c[oracle.GOLD_KEY]} {c['column']}: {c['build']!r} here, {c['twin']!r} in the twin"
                              for c in drift["beyond_rounding_list"][:8])
            more = f" (+{beyond - 8:,} more in gold_drift.beyond_rounding_list)" if beyond > 8 else ""
            rows = (f"; renewals only in this build {len(drift['only_build']):,}, only in the twin "
                    f"{len(drift['only_twin']):,}") if drift["only_build"] or drift["only_twin"] else ""
            by_col = ", ".join(f"{c} {n}" for c, n in drift["beyond_rounding_columns"].items()) or "none"
            rep.warn(f"gold drift beyond rounding vs the pandas twin on {where}: {beyond:,} cell(s) ({by_col}) are not "
                     f"a rounded float feature within one unit of its last decimal ({units}) of the twin's{rows}. A "
                     f"label, route, churned, date or count that differs, or a larger float difference, is a value "
                     f"the lakehouse computed differently from the gold the graph is specified on, not rounding"
                     + (f": {shown}{more}" if shown else ""))
    return {"drift": drift, "silver": silver, "today": today, "consts": build.gold_constants(mod), "sdir": sdir}


def _drifted(twin: dict | None) -> bool:
    d = (twin or {}).get("drift")
    return bool(d and (d["cells"] or d["only_build"] or d["only_twin"]))


def _drift_scope(drift: dict) -> str:
    """What a gold drift can move: 'all' (rows, labels, dates, plan or a PIT feature differ),
    'similar_to' (only SIMILAR_TO features) or 'none' (features nothing in the goldens reads)."""
    cols = set(drift["columns"])
    if drift["only_build"] or drift["only_twin"] or \
            cols & {*oracle.GOLD_LABELS, *oracle.GOLD_DATES, *oracle.PIT_FEATURES}:
        return "all"
    return "similar_to" if cols & set(spec.FEATURES) else "none"


def _similar_summary(golden: dict, v: dict) -> str:
    """Tie-aware view of the hero's neighbourhood: the golden's renewals and ranks vs this build's."""
    g, h = (golden.get("goldens") or {}).get("hero") or {}, (v.get("goldens") or {}).get("hero") or {}
    if not g or not h:
        return "no hero"
    a, b = g.get("top10", []), h.get("top10", [])
    ranks = [x["rank"] for x, y in zip(a, b, strict=False) if x["renewal_id"] != y["renewal_id"]]
    moved = [abs(int(x["d2_q"]) - int(y["d2_q"])) for x, y in zip(a, b, strict=False)
             if x["renewal_id"] == y["renewal_id"]]
    near = [x["renewal_id"] for x in g.get("nearest_lapses", [])] == \
        [x["renewal_id"] for x in h.get("nearest_lapses", [])]
    same = "the golden's renewals in the golden's ranks" if not ranks and len(a) == len(b) else \
        f"other renewals at ranks {ranks} than the golden" + ("" if len(a) == len(b) else f" ({len(a)} vs {len(b)})")
    return (f"hero top-10: {same}; d2_q of the shared ones moved by up to {max(moved, default=0):,} quanta; nearest "
            f"lapses {'the same renewals' if near else 'other renewals'}")


def check_derived(rep: Report, man: dict, bdir: Path, tables: dict, twin: dict) -> bool:
    """The builder on the bronze's events (the pandas twin's silver) + this build's own gold gives
    this build byte for byte: then every difference from the golden follows from the gold drift."""
    where = mf.display_path(twin["sdir"])
    gold = oracle.gold_frame(oracle.gold_content(tables["Renewal"]))
    try:
        derived = build.build_tables(twin["silver"], gold, twin["today"], twin["consts"])
    except (build.GraphBuildError, KeyError, ValueError) as e:
        rep.warn(f"the build's own gold cannot be rebuilt on {where}'s events ({type(e).__name__}: {str(e)[:200]}): "
                 f"its renewals are not the bronze's, so no golden value can be derived from the gold drift")
        return False
    parent = bdir.parent.parent if bdir.parent.parent.is_dir() else None
    with tempfile.TemporaryDirectory(prefix=".contract-derived-", dir=parent) as td:
        files = build.write_tables(derived, Path(td))
    differ = sorted(k for k in set(files) | set(man["files"])
                    if files.get(k, {}).get("sha256") != man["files"].get(k, {}).get("sha256"))
    if differ:
        rep.warn(f"the build is not the builder's graph of {where}'s events with its own gold: {len(differ)} file(s) "
                 f"differ ({', '.join(differ[:5])}{'...' if len(differ) > 5 else ''}). The lakehouse's events (silver) "
                 f"are not the bronze's, so no golden value can be derived from the gold drift")
        return False
    rep.ok(f"derived: the builder on {where}'s events (the pandas twin's silver) + this build's own gold rebuilds this "
           f"build byte for byte ({len(files)} files): every difference from the golden follows from the gold drift")
    return True


def check_goldens(rep: Report, man: dict, v: dict, tables: dict | None = None, bdir: Path | None = None,
                  twin: dict | None = None) -> str | None:
    rep.section("Goldens")
    name, golden, note, exact = oracle.find_golden(man)
    if golden is None:
        rep.warn(f"goldens skipped: {note}")
        return None
    diffs = oracle.diff(golden["values"], v)
    if exact and _drifted(twin):
        return derived_goldens(rep, man, v, tables, bdir, twin, name, golden, note, diffs)
    if exact and twin is not None:
        rep.ok(f"{note} applies: this build's gold content is the pandas twin's on the golden's bronze (sha256 "
               f"{twin['drift']['sha256'][0][:16]})")
    shown = "; ".join(diffs[:8]) + (f" (+{len(diffs) - 8} more)" if len(diffs) > 8 else "")
    what = "counts, routes, invariants, hero evidence + top-10, exposure, motif, first-after-cut"
    if exact:
        rep.check(not diffs, f"oracle = {note}: {what}", shown)
    elif diffs:
        rep.warn(f"inputs differ from {note} and {len(diffs)} golden value(s) differ: {shown}. If the renewal model "
                 f"or generator changed, regenerate the golden with the oracle (--print-golden)")
    else:
        rep.ok(f"oracle = {note}: {what}")
        why = ("the inject profile's one poisoned user_name" if man["profile"] == "inject" else
               "other platform or CSV formatting")
        rep.note(f"the bronze bytes differ from the golden's ({why}) but every golden value matches")
    here, there = man["platform"], golden.get("measured_on", {}).get("platform")
    if there and here != there:
        rep.note(f"golden measured on {there}, this build on {here}: d2_q compared tie-aware (+-1)")
    return name


def derived_goldens(rep: Report, man: dict, v: dict, tables: dict, bdir: Path, twin: dict, name: str, golden: dict,
                    note: str, diffs: list[str]) -> str:
    """Goldens for a build whose gold drifted from the pandas twin of the golden's bronze.

    The golden pins the pandas twin's gold, so it does not apply value for value. Instead: (1) the
    build must be the builder's graph of the bronze's events with its own gold (check_derived),
    (2) golden values may differ only where the drifted columns can move them (_drift_scope:
    SIMILAR_TO-derived values for a feature drift; a value outside that scope is an error), and
    (3) what moved is reported as INFO, the hero's neighbourhood tie-aware. Every invariant was
    checked from the build's own data already (#1-#6 in pandas and Cypher)."""
    drift = twin["drift"]
    rep.info(f"{note} pins the pandas twin's gold of these bronze bytes (gold sha256 {drift['sha256'][1][:16]}); this "
             f"build's gold differs in {drift['cells']:,} cell(s) (Gold content above), so its golden values are "
             f"derived from its own gold, not pinned")
    check_derived(rep, man, bdir, tables, twin)
    scope = _drift_scope(drift)
    moved, outside = [], []
    for d in diffs:
        path = d.split(":", 1)[0]
        movable = scope == "all" or (scope == "similar_to" and path.startswith(SIMILAR_TO_PATHS))
        (moved if movable else outside).append(d)
    if outside:
        why = {"similar_to": "only SIMILAR_TO features drifted", "none": "the drifted features feed no golden value"}
        rep.error(f"{len(outside)} golden value(s) of {name}.json differ that the drift cannot move "
                  f"({why.get(scope, scope)}: {', '.join(drift['columns'])}): " + "; ".join(outside[:8]))
    if moved:
        kind = "SIMILAR_TO-derived" if scope == "similar_to" else "labels / dates / PIT features drifted"
        more = f" (+{len(moved) - 6} more)" if len(moved) > 6 else ""
        rep.info(f"{len(moved)} golden value(s) of {name}.json moved with the drift ({kind}; each checked from the "
                 f"build's own data by #1-#6 and Cypher = oracle): " + "; ".join(moved[:6]) + more)
        rep.info(_similar_summary(golden["values"], v))
    if not outside:
        rep.ok(f"{name}.json: no golden value outside the drift's reach differs ({len(moved)} moved within it)")
    return name


def _explained_by_drift(res: dict, twin: dict | None) -> list | None:
    """The export mismatches when every one of them is a recorded gold drift cell, else None."""
    drift = (twin or {}).get("drift")
    if not drift or res["status"] != "mismatch" or not res.get("cells_complete") or \
            drift["cells"] > len(drift["cell_list"]):
        return None
    cells = {(c[oracle.GOLD_KEY], c["column"]) for c in drift["cell_list"]}
    alias = {"feature_as_of": "as_of"}
    return res["cells"] if all((uid, alias.get(col, col)) in cells for uid, col in res["cells"]) else None


def check_export(rep: Report, man: dict, tables: dict, strict: bool, twin: dict | None = None) -> None:
    rep.section("Export cross-check")
    edir = mf.resolve_path(man["exports"]["dir"])
    res = oracle.export_crosscheck(tables, edir)
    explained = _explained_by_drift(res, twin)
    if explained is not None:
        cells = sorted({(uid, col) for uid, col in explained})
        rep.info(f"the exports in {mf.display_path(edir)} differ from this build in {len(cells)} cell(s), exactly the "
                 f"recorded gold drift cells (they hold the pandas twin's gold): "
                 + ", ".join(f"{uid} {col}" for uid, col in cells[:8]) + ("..." if len(cells) > 8 else ""))
        res = {**res, "status": "ok", "explained_by_gold_drift": explained, "mismatches": {}}
    rep.results["export"] = res
    rep.results["exports_sha256"] = mf.export_hashes(edir)
    if res["status"] == "missing":
        profile = man["profile"]
        how = {"default": "make churn-gold-local (writes data/export; graph-sample refuses PROFILE=default)",
               "tiny": "make graph-sample PROFILE=tiny (writes its exports only; the bronze is the committed fixture)"
               }.get(profile, f"make graph-sample PROFILE={profile}")
        rep.warn(f"export cross-check skipped: {res['path']} not found "
                 f"(run {how}{'; required under --strict' if strict else ''})")
        return
    rep.check(res["status"] == "ok",
              f"Renewal features = {mf.display_path(res['path'])} for {res['rows']:,} rows x {res['columns']} columns "
              f"(built_at excluded; train ids {res.get('train_rows', 'n/a')}; hero record keys "
              f"{res.get('hero_record_keys', 'n/a')})", _fmt(res["mismatches"]))
    now = rep.results["exports_sha256"]
    if now != man["exports"]["sha256"]:
        rep.warn(f"exports changed since the build (manifest export sha256 differs): run make graph-build "
                 f"PROFILE={man['profile']} to re-pin them (an unchanged build is kept, not rebuilt)")
    else:
        rep.ok(f"export sha256 pinned in the manifest ({len(now)} files)")


def _catalog_args(man: dict, uri: str | None, warehouse: str | None) -> tuple[str | None, str | None]:
    """(catalog uri, warehouse) to re-read an Iceberg build's pins: the ones given, else the PyIceberg
    configuration of catalog ``lakehouse`` (None: iceberg_source reads it), else the SQLite catalog the
    manifest recorded (a file path; it holds no credentials, unlike a redacted server URI)."""
    if uri:
        return uri, warehouse
    ice = man.get("iceberg") or {}
    recorded = str(ice.get("catalog_uri") or "")
    try:
        from pyiceberg.utils.config import Config
        configured = bool((Config().get_catalog_config(iceberg_source.CATALOG) or {}).get("uri"))
    except ImportError:
        configured = False
    if configured or not recorded.startswith("sqlite:"):
        return None, warehouse
    return recorded, warehouse or ice.get("warehouse") or None


def check_iceberg_determinism(rep: Report, bdir: Path, man: dict, uri: str | None, warehouse: str | None) -> None:
    """Re-read the inputs at the recorded tag + snapshot ids and rebuild (iceberg_source.verify_build)."""
    ice = man["iceberg"]
    uri, warehouse = _catalog_args(man, uri, warehouse)
    try:
        iceberg_source.close_catalog(iceberg_source.open_catalog(uri, warehouse))
    except ImportError as e:
        rep.warn(f"Iceberg determinism check skipped: pyiceberg is not installed here ({e.name or e}); re-read the "
                 f"pins from .venv-graph-spark or the ldl-graph image")
        return
    except iceberg_source.ProvenanceUnavailable as e:
        rep.warn(f"Iceberg determinism check skipped: {e} (pass --catalog-uri / --warehouse or set "
                 f"PYICEBERG_CATALOG__LAKEHOUSE__*)")
        return
    try:
        res = iceberg_source.verify_build(bdir, uri, warehouse)
    except (iceberg_source.ProvenanceUnavailable, build.GraphBuildError, KeyError, ValueError) as e:
        rep.error(f"the Iceberg pins of this build no longer hold: {e}. It cannot be reproduced from the lakehouse "
                  f"(a tag moved, a snapshot expired or a row count changed since {ice.get('tag')} was read)")
        return
    rep.results["iceberg_verify"] = res
    n = sum(1 for p in (ice.get("tables") or {}).values() if p.get("role") == "input")
    rep.check(res["byte_identical"], f"re-read the {n} inputs at {res['tag']} (tag -> the recorded snapshot id, row "
                                     f"counts) and rebuilt: byte-identical Parquet ({len(man['files'])} files) on "
                                     f"{man['platform']}", f"files differ: {res['files_differ']}")
    rep.check(res["fresh"], f"... and the identity recomputed from the pins = business_build_id "
                            f"{man['business_build_id']}")


def check_determinism(rep: Report, bdir: Path, man: dict, fresh: bool, catalog_uri: str | None = None,
                      warehouse: str | None = None) -> None:
    rep.section("Determinism")
    if not fresh:
        rep.warn("determinism check skipped: the build is stale (inputs or code changed since it was built)")
        return
    if man.get("iceberg") is not None:
        check_iceberg_determinism(rep, bdir, man, catalog_uri, warehouse)
        if mf.iceberg_pins(man) is not None:
            return   # its own identity: the bronze CSVs do not reproduce it (the drift is in the gold section)
    sdir = mf.resolve_path(man["inputs"]["sample_dir"])
    mod = build.load_gold_twin(sdir)
    silver, gold, today = build.run_gold(mod)
    tables = build.build_tables(silver, gold, today, build.gold_constants(mod))
    parent = bdir.parent.parent if bdir.parent.parent.is_dir() else None
    with tempfile.TemporaryDirectory(prefix=".contract-rebuild-", dir=parent) as td:
        files = build.write_tables(tables, Path(td))
    diffs = sorted(k for k in set(files) | set(man["files"])
                   if files.get(k, {}).get("sha256") != man["files"].get(k, {}).get("sha256"))
    rep.check(not diffs, f"rebuild from the same inputs + code: same business_build_id {man['business_build_id']} "
                         f"and byte-identical Parquet ({len(files)} files, sha256 equal) on {man['platform']}",
              f"files differ: {diffs}")


def check_interference(rep: Report, man: dict, before: dict) -> None:
    rep.section("Non-interference")
    now = mf.guarded_hashes()
    rep.check(now == before, f"data/sample/churn/* and data/export/* unchanged by this check ({len(now)} files)",
              _fmt(sorted(k for k in set(now) | set(before) if now.get(k) != before.get(k))))
    built = man.get("guarded_sha256", {})
    if built == now:
        rep.ok("... and unchanged since the build (manifest guarded sha256 equal)")
    else:
        changed = sorted(k for k in set(now) | set(built) if now.get(k) != built.get(k))
        rep.note(f"user files changed since the build: {changed[:6]}. No graph target writes them (the builder, "
                 f"graph-sample and this check each assert it); a churn-* target ran in between")


def check_lint(rep: Report) -> None:
    rep.section("Template lint")
    bad = [k for k, q in queries.TEMPLATES.items()
           if not re.search(r"\bORDER\s+BY\b", q, re.IGNORECASE) or not re.search(r"\bLIMIT\b", q, re.IGNORECASE)]
    rep.check(not bad, f"#10 all {len(queries.TEMPLATES)} Cypher templates have ORDER BY and LIMIT", str(bad))
    problems = queries.lint()
    rep.check(not problems,
              f"#10 leak lint: {len(queries.TOOL_TEMPLATES)} tool templates bound source events by as_of, never return "
              f"BILLED outcome evidence, read neighbour outcomes only under the visibility rule and describe their "
              f"source only (no second renewal pinned by a parameter, literal or selection, or anchoring another "
              f"renewal through a hub or by value), return named "
              f"properties only (no .*), give every name one meaning, cut the calendar (CUT_CAP, pricing changes, "
              f"incidents) at a named renewal's as_of and return population outcomes as cohort aggregates only "
              f"(every grouping, every parameter and literal), and each is a vetted shape (fingerprint in "
              f"queries.VETTED_TOOL_TEMPLATES); {len(queries.CONTRACT_ONLY)} templates are contract-only",
              "; ".join(problems[:6]))


def report_resources(rep: Report, man: dict) -> None:
    """Reported, never gated (a note, not a warning: --strict ignores it). The builder's max RSS is
    ru_maxrss of the process that ran the build: the builder's own peak for the CLI (make graph-build,
    build_graph_local.py), the host's peak so far for a build run inside another process (pytest, a
    long-lived worker), which can be far above what the build needed. The loader's is its own child's."""
    rep.section("Resources (reported, not gated)")
    b, lb = man["builder"], man["ladybug"]
    rep.note(f"builder {b['seconds']} s, max RSS {b['max_rss_mib']} MiB (ru_maxrss of the process that ran the "
             f"build: the builder's own via the CLI, the host's peak for an in-process build; soft limit "
             f"{b['rss_soft_limit_mib']} MiB); Ladybug {lb['version']} load {lb['load_s']} s, "
             f"{lb['db_bytes'] / 1e6:.1f} MB, loader max RSS {lb['max_rss_bytes'] / 2**20:.0f} MiB, "
             f"pool {lb['buffer_pool_mb']} MB, {lb['threads']} threads")
    if b.get("rss_over_soft_limit"):
        rep.note(f"above the soft limit (reported, not gated): builder max RSS {b['max_rss_mib']} MiB > "
                 f"{b['rss_soft_limit_mib']} MiB")


def record(bdir: Path, man: dict, doc: dict, lock_timeout: float = 600.0) -> str:
    """Write <build>/contract.json, the record `make graph-promote` relies on.

    Every strict run and every failure is recorded. A plain (non-strict) pass is recorded
    too, unless the build already holds a strict pass that is still valid for its manifest:
    that one is kept, so a casual re-check never takes a build's promotability away.

    The write happens under the BuildLock of the build's graph root, after checking again that
    the directory still holds the build that was checked (same id and manifest sha256; for a
    pass, the Parquet bytes too). The builder swaps a --rebuild in under the same lock, so a
    record is never written into a directory that is about to be replaced and never describes
    bytes other than the ones on disk. Returns "written", "kept" (the earlier strict pass stays)
    or "changed" (the build changed during the check: nothing is recorded).
    """
    def write() -> str:
        try:
            now = mf.read_manifest(bdir)
        except (OSError, ValueError):
            return "changed"
        same = now.get("business_build_id") == man["business_build_id"] and \
            {k: x["sha256"] for k, x in now.get("files", {}).items()} == doc["files_sha256"]
        if same and doc["status"] == "pass":
            same = all((bdir / rel).is_file() and mf.sha256_file(bdir / rel) == sha
                       for rel, sha in doc["files_sha256"].items())
        if not same:
            return "changed"
        if not doc["strict"] and doc["status"] == "pass" and store.is_strict_pass(store.read_contract(bdir), now):
            return "kept"
        mf.write_json_atomic(bdir / store.CONTRACT_FILE, doc)
        return "written"

    if bdir.parent.name != "builds" or not spec.is_profile(bdir.parent.parent.name):
        return write()  # not in a <root>/<profile>/builds tree: no builder ever swaps this directory
    with store.BuildLock(bdir.parent.parent.parent, timeout=lock_timeout, log=print):
        return write()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate a renewal graph build (contract renewal-graph/v1).")
    ap.add_argument("--profile", default="default")
    ap.add_argument("--build", default=None, help="build directory (default: the profile's latest build)")
    ap.add_argument("--graph-root", default=None)
    ap.add_argument("--strict", action="store_true", help="warnings fail too; exports are required")
    ap.add_argument("--no-ladybug", action="store_true", help="skip the Cypher half (warns)")
    ap.add_argument("--no-determinism", action="store_true", help="skip the in-memory rebuild (warns)")
    ap.add_argument("--json", default=None, help="write the full result document here")
    ap.add_argument("--lock-timeout", type=float, default=600.0,
                    help="seconds to wait for $GRAPH_ROOT/.lock before writing contract.json (build, promote and gc "
                         "share it)")
    ap.add_argument("--catalog-uri", default=None,
                    help="Iceberg-sourced builds: the JDBC catalog to re-read the pins from (default: "
                         "PYICEBERG_CATALOG__LAKEHOUSE__URI, else the SQLite catalog the manifest recorded)")
    ap.add_argument("--warehouse", default=None, help="Iceberg-sourced builds: the warehouse URI (default: as above)")
    a = ap.parse_args(argv)

    try:
        bdir = Path(a.build).absolute() if a.build else spec.latest_link(a.profile, a.graph_root)
    except ValueError as e:
        print(f"Graph contract FAILED: {e}", file=sys.stderr)
        return 1
    if not (bdir / mf.MANIFEST_FILE).is_file():
        print(f"Graph contract FAILED: no build at {bdir} (run make graph-build PROFILE={a.profile})", file=sys.stderr)
        return 1
    bdir = bdir.resolve()
    man = mf.read_manifest(bdir)
    rep = Report()
    before = mf.guarded_hashes()
    print(f"Graph contract {spec.CONTRACT_VERSION}: profile {man['profile']}, build {man['business_build_id']} "
          f"({bdir}){' [strict]' if a.strict else ''}")

    source = "iceberg" if man.get("iceberg") is not None else "csv"
    fresh = check_integrity(rep, bdir, man)
    tables = oracle.load_tables(bdir)
    v = oracle.compute(bdir, tables)
    check_oracle(rep, man, v)
    if a.no_ladybug:
        rep.section("Ladybug")
        rep.warn("Cypher checks skipped (--no-ladybug): only the pandas half of the contract ran")
    else:
        try:
            check_cypher(rep, bdir, v, tables)
        except (RuntimeError, OSError, KeyError, IndexError, ImportError) as e:
            rep.error(f"Ladybug checks could not complete ({type(e).__name__}: {str(e)[:300]}); Parquet is canonical, "
                      f"rebuild graph.lbdb with make graph-build")
    twin = check_gold_source(rep, man, tables) if source == "iceberg" else None
    golden_name = check_goldens(rep, man, v, tables, bdir, twin)
    check_export(rep, man, tables, a.strict, twin)
    del twin   # the pandas twin's silver: not needed past here
    if a.no_determinism:
        rep.section("Determinism")
        rep.warn("determinism check skipped (--no-determinism)")
    else:
        check_determinism(rep, bdir, man, fresh, a.catalog_uri, a.warehouse)
    check_interference(rep, man, before)
    check_lint(rep)
    report_resources(rep, man)

    errors = list(rep.errors)
    if a.strict and rep.warnings:
        errors.append(f"{len(rep.warnings)} warning(s) are fatal with --strict")
    status = "fail" if errors else "pass"
    ice = man.get("iceberg") or {}
    drift = rep.results.get("gold_drift")
    doc = {"contract": spec.CONTRACT_VERSION, "status": status, "strict": bool(a.strict),
           "business_build_id": man["business_build_id"], "profile": man["profile"],
           "source": {"kind": source, **({"tag": ice.get("tag"), "identity": ice.get("identity"),
                                         "lakehouse_build_id": ice.get("lakehouse_build_id")} if ice else {})},
           "files_sha256": {k: x["sha256"] for k, x in man["files"].items()},
           "exports_sha256": rep.results.get("exports_sha256", {}),
           "golden": golden_name, "golden_mode": ("derived" if drift and drift["cells"] + len(drift["only_build"]) +
                                                  len(drift["only_twin"]) else "exact") if golden_name else None,
           "errors": rep.errors, "warnings": rep.warnings, "info": rep.infos,
           "skipped": [s for s, on in (("ladybug", a.no_ladybug), ("determinism", a.no_determinism)) if on],
           "checked_at": mf.utc_now(),
           "summary": {"counts": v["counts"], "routes": v["routes"], "model_lapses": v["model_lapses"],
                       "invariants": v["invariants"], "export": rep.results.get("export"),
                       "gold_drift": drift, "iceberg_verify": rep.results.get("iceberg_verify")}}
    try:
        recorded = record(bdir, man, doc, a.lock_timeout)
    except TimeoutError as e:
        recorded = "changed"
        errors.append(f"{store.CONTRACT_FILE} not written: {e}")
    else:
        if recorded == "changed":
            errors.append(f"the build changed during the check (rebuilt or tampered with): nothing was recorded in "
                          f"{store.CONTRACT_FILE}; run the check again")
    if a.json:
        Path(a.json).write_text(json.dumps({**doc, "values": v}, indent=1, sort_keys=True, default=str) + "\n")

    print()
    for w in rep.warnings:
        print(f"WARN: {w}", file=sys.stderr)
    if errors:
        print("Graph contract FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    c, inv = v["counts"], v["invariants"]
    nv = inv["naive_mismatches"]
    derived = " (derived)" if doc["golden_mode"] == "derived" else ""
    where = f"; source Iceberg {ice.get('tag')}" if ice else ""
    drifted = ""
    if drift and drift["cells"]:
        beyond = drift["beyond_rounding"]
        drifted = f"; gold drift {drift['cells']:,} cell(s), " + \
            (f"{beyond:,} beyond rounding (warned)" if beyond else "info")
    print(f"Graph contract OK ({spec.CONTRACT_VERSION}, profile {man['profile']}, build {man['business_build_id']}): "
          f"{c['total_nodes']:,} nodes / {c['total_edges']:,} edges; PIT parity 0 mismatches x "
          f"{len(inv['parity_mismatches'])} features in pandas{'' if a.no_ladybug else ' and Cypher'}; naive wrong in "
          f"{nv['limit_hits_14d']:,} / {nv['incident_exposed_28d']:,} / {nv['support_tickets_90d']:,} renewals; "
          f"golden {golden_name or 'n/a'}{derived}{where}{drifted}{'; strict' if a.strict else ''}")
    if recorded == "kept":
        print(f"  note  {store.CONTRACT_FILE} keeps the earlier strict pass of this build (this run was not strict)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
