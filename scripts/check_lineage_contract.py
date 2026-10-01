#!/usr/bin/env python3
"""Validate a lineage build against the lineage contract (metadata-graph/0.1).

Checks the lineage tables of one graph build (default: the latest build of
``--graph-profile``) the way check_graph_contract.py checks the business graph: structural
problems always fail; warnings print; ``--strict`` makes warnings fail. Environment notes of
the full profile (no RADAR_DIR, no export files on this machine) describe the machine, not
the repo: they are printed and never fail, strict or not.

  1. integrity     Parquet sha256 = lineage manifest; lineage_build_id still matches the
                   files it was extracted from (stale -> warning)
  2. structure     every edge joins existing nodes of an allowed label pair; Ladybug counts =
                   Parquet counts; the Tier-1 / Tier-2 tables exist, and hold rows only when the
                   lineage manifest records the overlay that wrote them
  3. extraction    no unresolved name (a name the extractor saw and could not resolve)
  4b. tiers        with --iceberg / --openlineage overlays: every snapshot belongs to its table,
                   every ref points at its snapshot, SUPERSEDES chains each table, PRODUCED_BY_RUN
                   = the snapshot's spark.app.id, one job per run, no PARENT cycle, the graph
                   build consumed what its manifest pins, the catalog schema was unchanged by the
                   read; the per-table / per-run summary equal in Cypher and in Python
  4. semantics     every gold SQL output column resolves; features are compliant or declared
                   exceptions and agree with lakehouse_graph.spec.FEATURE_CARDS; the stored
                   pit_status / range guarantee / parameter consistency equal a recomputation
                   from the edge windows and values; parameters agree across SQL / pandas /
                   generator; PLAN 6.6 invariant 9; the bridge covers every business graph
                   node label and edge type
  5. twice         every golden answer in Cypher (lineage.lbdb, read only, 128 MB pool) =
                   the pure-Python oracle over the Parquet tables
  6. goldens       oracle answers = lineage/goldens/core.json (core profile: gated; full
                   profile: differences are reported). Whole-graph totals are reported only.
  7. interference  data/sample/churn/* and data/export/* sha256 unchanged
  8. lint          every lineage Cypher template has ORDER BY and LIMIT
  9. latency       the plan's seven lineage questions, warm, and the cold cost of a fresh
                   read-only connection (open + each first call) (reported, never gated)

A run is recorded in <build>/lineage/contract.json.

Usage:
  python scripts/check_lineage_contract.py [--graph-profile default] [--strict]
  python scripts/check_lineage_contract.py --build <build_dir> --strict --json out.json
  python scripts/check_lineage_contract.py --print-golden > src/lakehouse_graph/lineage/goldens/core.json
      (regenerate the golden from the oracle after an intended change of the pipeline; review the diff)
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import manifest as mf  # noqa: E402 (after the sys.path line above)
from lakehouse_graph import spec as gspec  # noqa: E402
from lakehouse_graph.lineage import build as lbuild  # noqa: E402
from lakehouse_graph.lineage import oracle, queries, spec, tools  # noqa: E402

LATENCY_TARGET_MS = 10.0
LATENCY_RUNS = 25


class Report:
    """Collects errors / warnings and prints a readable, sectioned summary."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def section(self, title: str) -> None:
        print(f"\n== {title}")

    def ok(self, msg: str) -> None:
        print(f"  ok    {msg}")

    def note(self, msg: str) -> None:
        print(f"  note  {msg}")

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
    return s if len(s) <= 140 else s[:137] + "..."


def _shown(items: list[str], n: int = 6) -> str:
    return "; ".join(items[:n]) + (f" (+{len(items) - n} more)" if len(items) > n else "")


def _ends(days: int | None) -> str:
    """Upper bound of a feature's reads, for display (None: a read with no time bound at all)."""
    return "no upper time bound" if days is None else f"window ends as_of{days:+d}"


# --------------------------------------------------------------------------- 1-3
def check_integrity(rep: Report, bdir: Path, man: dict) -> bool:
    """False when the Parquet tables are not the ones the manifest describes (nothing else is checked then)."""
    rep.section("Integrity and identity")
    ldir = bdir / spec.LINEAGE_DIR
    bad = [f for f, meta in man["files"].items()
           if not (ldir / f).is_file() or mf.sha256_file(ldir / f) != meta["sha256"]]
    intact = rep.check(not bad, f"{len(man['files'])} Parquet files match the lineage manifest sha256",
                       f"changed or missing: {bad}")
    rep.equal("spec version", man.get("spec"), spec.SPEC_VERSION)
    want = [name for _n, name, _s in lbuild.table_files()]
    rep.check(sorted(man["files"]) == sorted(want),
              f"one table per node label ({len(spec.NODE_SCHEMA)}) and edge type ({len(spec.EDGE_SCHEMA)})",
              f"missing {sorted(set(want) - set(man['files']))}")
    if lbuild.is_fresh(man):
        rep.ok(f"lineage_build_id {man['lineage_build_id']} = sha256(files read, lineage code, spec, versions): "
               f"{len(man['inputs_sha256'])} files unchanged since the build")
    else:
        rep.warn(f"stale lineage build {man['lineage_build_id']}: a file it was extracted from (or the lineage "
                 f"code) changed since; run make lineage-local")
    try:
        business = mf.read_manifest(bdir)
    except (OSError, ValueError):
        rep.note("no business manifest.json in this directory (a scratch lineage build)")
    else:
        rep.check(business.get("lineage", {}).get("lineage_build_id") == man["lineage_build_id"],
                  f"manifest.json[\"lineage\"] records this lineage build (business_build_id "
                  f"{business.get('business_build_id')} is a separate id)")
    rep.note(f"profile {man['profile']}; commit {man.get('commit')}{' +dirty' if man.get('dirty') else ''}; "
             + ", ".join(f"{k} {v}" for k, v in sorted(man["versions"].items())))
    return intact


def check_structure(rep: Report, man: dict, t: oracle.Tables) -> None:
    rep.section("Structure (Parquet)")
    counts = t.counts()
    rep.equal("manifest counts", man["counts"], counts, "oracle recount")
    dangling, pairs = [], []
    for rel, rows in t.edges.items():
        allowed = set(spec.EDGE_SCHEMA[rel].pairs)
        for e in rows:
            if e["src"] not in t.nodes or e["dst"] not in t.nodes:
                dangling.append(f"{rel} {e['src']} -> {e['dst']}")
            elif (t.label[e["src"]], t.label[e["dst"]]) not in allowed or \
                    (e["src_label"], e["dst_label"]) != (t.label[e["src"]], t.label[e["dst"]]):
                pairs.append(f"{rel} {e['src']} -> {e['dst']}")
    rep.check(not dangling, f"every edge joins two existing nodes ({counts['total_edges']:,} edges)", _shown(dangling))
    rep.check(not pairs, "every edge uses a label pair its type allows", _shown(pairs))
    degrees = [c["id"] for c in t.by_label["DataColumn"]
               if c["n_readers"] != len(t.into("DERIVED_FROM", c["id"]))
               or c["n_sources"] != len(t.out("DERIVED_FROM", c["id"])) + len(t.out("COUNTS_ROWS_OF", c["id"]))]
    rep.check(not degrees, "stored column degrees (n_sources, n_readers) = the edges counted", _shown(degrees))
    refs = [c["ref"] for c in t.by_label["DataColumn"] if c["ref"]]
    rep.check(len(refs) == len(set(refs)) and all(spec.is_column_ref(r) for r in refs),
              f"{len(refs):,} of {counts['nodes']['DataColumn']:,} columns are addressable by a unique ColumnRef")
    tier_nodes = [n.label for n in spec.NODE_SCHEMA.values() if n.placeholder]
    tier_edges = [e.rel for e in spec.EDGE_SCHEMA.values() if e.placeholder]
    filled = [x for x in tier_nodes if counts["nodes"][x]] + [x for x in tier_edges if counts["edges"][x]]
    overlays = sorted(man.get("overlays") or {})
    if not filled and not overlays:
        rep.ok(f"Tier-1 / Tier-2 tables ({', '.join(tier_nodes)} and their {len(tier_edges)} edge types) exist and "
               f"are empty (no --iceberg / --openlineage overlay)")
    else:
        rep.check(bool(overlays), f"Tier-1 / Tier-2 tables are filled by the recorded overlay(s) "
                                  f"{', '.join(overlays) or 'none'}: " + ", ".join(
                                      f"{x} {counts['nodes'].get(x, counts['edges'].get(x))}" for x in filled),
                  "rows without an overlay in the lineage manifest")


def check_extraction(rep: Report, man: dict) -> None:
    rep.section("Extraction")
    for u in man["unresolved"]:
        rep.error(f"unresolved name: {u['where']}: {u['what']}")
    if not man["unresolved"]:
        rep.ok(f"no unresolved name in {len(man['inputs_sha256'])} files")
    for w in man["warnings"]:
        rep.warn(f"extractor: {w}")
    for w in man.get("environment_warnings", []):
        rep.note(f"environment (not gated): {w}")


# --------------------------------------------------------------------------- 4
def check_semantics(rep: Report, v: dict, t: oracle.Tables) -> None:
    rep.section("Semantics (pure-Python oracle)")
    gc = v["gold_columns"]
    rep.check(gc["sql_columns"] > 0 and not gc["unresolved"],
              f"all {gc['sql_columns']} gold SQL output columns resolve to source columns or row counts"
              f" (+ {', '.join(gc['added_by_job']) or 'none'} added by the job)", f"unresolved: {gc['unresolved']}")
    pit = {r["feature"]: r for r in v["pit"]}
    rep.equal("features with a point-in-time status", sorted(pit), sorted(gspec.GOLD_FEATURES),
              "lakehouse_graph.spec.GOLD_FEATURES")
    bad = sorted(f for f, r in pit.items() if r["pit_status"] not in spec.FEATURE_PIT_STATUSES)
    if not rep.check(not bad, "every feature is compliant or a declared exception",
                     f"undeclared exception (reads after as_of, not declared in FEATURE_CARDS): {bad}"):
        ends = "; ".join(f"{f}: {_ends(pit[f]['max_upper_vs_as_of_days'])}" for f in bad)
        rep.note(f"{ends}. lineage_pit(feature=...) lists the late window or the read with no time bound. A "
                 f"genuine exception is declared in lakehouse_graph/spec.py FEATURE_CARDS; a read that is in fact "
                 f"bounded is one the walk does not recognise (lineage/scope_walk.py, 'Known conservative cases'): "
                 f"join the base table in the feature CTE and write the bounds as top-level AND conditions of its "
                 f"ON / WHERE, in whole days relative to r.as_of")
    differ = sorted(f for f, r in pit.items() if gspec.FEATURE_CARDS.get(f, {}).get("pit_status") != r["pit_status"])
    rep.check(not differ, f"lineage pit_status = FEATURE_CARDS pit_status for all {len(pit)} features",
              f"differ: {differ}")
    exceptions = sorted(f for f, r in pit.items() if r["pit_status"] != spec.PIT_COMPLIANT)
    rep.ok(f"{len(pit) - len(exceptions)} of {len(pit)} features compliant; exceptions: "
           + (", ".join(f"{f} ({_ends(pit[f]['max_upper_vs_as_of_days'])})" for f in exceptions) or "none"))
    late = sorted(f for f, r in pit.items() if r["pit_status"] == spec.PIT_COMPLIANT and r["reads_after_as_of"])
    rep.check(not late, "no compliant feature has a window that ends after as_of", str(late))
    mismatches = oracle.derivation_mismatches(t)
    rep.check(not mismatches, "stored pit_status, range_guarantee and parameter consistency = recomputed from the "
                              "edge windows, clamps and values", _shown(mismatches))
    loose = sorted(f"{f} <- {u['dataset']} (matched to {u['matched_window']})" for f, r in pit.items()
                   for u in r["unbounded_reads"] if r["pit_status"] == spec.PIT_COMPLIANT)
    if loose:
        rep.note(f"reference data read without a time bound of its own: {'; '.join(loose)}")
    windows = {w["display"]: w for w in t.by_label["Window"]}
    no_window = [r["column"] for r in v["counts_rows_of"] if r["window"] not in windows and r["window"] != "unbounded"]
    rep.check(not no_window, "COUNT(*) columns carry their row window: "
              + ", ".join(f"{r['column'].rsplit('.', 1)[-1]} {r['window']}" for r in v["counts_rows_of"]),
              str(no_window))
    params = v["parameters"]
    inconsistent = [p["name"] for p in params if p["consistent"] is False]
    rep.check(bool(params) and not inconsistent,
              "parameters agree across gold SQL, pandas twin and generator: "
              + ", ".join(f"{p['name']} {p['sql_value']:g}" for p in params if p["consistent"]),
              f"inconsistent: {inconsistent}")
    unused = [p["name"] for p in params if p["unused"]]
    if unused:
        rep.note(f"defined but never read: {', '.join(unused)}")
    inv = v["invariant_9"]
    i9 = spec.INVARIANT_9
    rep.check(inv["silver_gold_descendants"] == [],
              f"invariant 9: {i9['silver']} has no gold descendant", _fmt(inv["silver_gold_descendants"]))
    rep.equal(f"invariant 9: {i9['bronze']} reaches gold only via {i9['via'].rsplit('.', 1)[-1]}",
              inv["bronze_paths_to_gold"], [[i9["bronze"], i9["via"], i9["gold"]]])
    b = v["bridge"]
    want = len(gspec.NODE_SCHEMA) + len(gspec.EDGE_SCHEMA)
    unsourced = [e["element"] for e in b["by_element"] if not (e["datasets"] and e["source_columns"] and e["tables"])]
    rep.check(b["elements"] == want and b["nodes"] == len(gspec.NODE_SCHEMA) and not unsourced,
              f"bridge: all {want} business graph types ({b['nodes']} node labels + {b['edges']} edge types) are "
              f"SOURCED_FROM a silver / gold dataset and its columns", f"{b['elements']} elements; {unsourced}")
    lk = v["leaky"]
    if lk["dangling"]:
        rep.note(f"LEAKY names a column no dataset has: {', '.join(lk['dangling'])} "
                 f"(check_repo_contracts.py warns about it)")


# --------------------------------------------------------------------------- 4b
def check_tiers(rep: Report, bdir: Path, man: dict, t: oracle.Tables, no_ladybug: bool) -> dict:
    """Tier-1 / Tier-2 facts (--iceberg / --openlineage): structural rules (Python) and Cypher = Python.
    The facts are the catalog / file as they were at build time: the contract reads neither again."""
    overlays = man.get("overlays") or {}
    if not overlays:
        return {}
    rep.section(f"Tier 1 / Tier 2 facts (overlays {', '.join(sorted(overlays))}; as read at build time)")
    problems = oracle.tier_invariants(t)
    rep.check(not problems, "every snapshot belongs to its table, every ref points at its snapshot, SUPERSEDES chains "
                            "each table, PRODUCED_BY_RUN = the snapshot's spark.app.id, one job per run, no PARENT "
                            "cycle", _shown(problems))
    py = queries.tier_section(oracle.tier_rows(t))
    ice = overlays.get("iceberg")
    if ice:
        from lakehouse_graph.lineage import iceberg_facts

        rep.check(ice.get("catalog_schema_unchanged") is True,
                  f"Iceberg catalog {ice.get('catalog')} ({ice.get('catalog_uri')}): its own schema was the same "
                  f"before and after the metadata read (no ALTER)")
        in_catalog = sum(x["snapshots"] - x["not_in_catalog"] for x in py["tables"].values())
        rep.equal("Iceberg snapshots in the graph", in_catalog, ice["snapshots"], "the facts read")
        rep.note(iceberg_facts.summary_line(ice))
        if ice.get("graph_build"):
            b = ice["graph_build"]
            consumed = py["runs"].get(b["run"], {}).get("consumed", 0)
            rep.equal(f"snapshots the graph build {b['run'].rsplit(':', 1)[-1]} CONSUMED at {b['tag']}", consumed,
                      b["consumed"], "manifest.json[\"iceberg\"]")
    ol = overlays.get("openlineage")
    if ol:
        from lakehouse_graph.lineage import openlineage

        apps = sum(1 for r in py["runs"].values() if r["kind"] == "spark_application")   # + Iceberg-only runs
        rep.check(apps >= ol["application_runs"],
                  f"OpenLineage: {ol['application_runs']} application runs are Run nodes "
                  f"({ol['action_runs_collapsed']} action runs collapsed into them)",
                  f"{apps} spark_application runs in the graph")
        rep.note(openlineage.summary_line(ol))
    if no_ladybug:
        rep.warn("Tier-1 / Tier-2 Cypher check skipped (--no-ladybug)")
    else:
        with tools.LineageContext(bdir) as ctx:
            cy = queries.tier_section(queries.tier_rows(ctx.lineage_conn))
        diffs = oracle.diff(py, cy)
        rep.check(not diffs, f"tier facts: Cypher = oracle ({len(py['tables'])} tables, {len(py['runs'])} runs)",
                  _shown(diffs))
    return py


# --------------------------------------------------------------------------- 5, 9
def cypher_values(ctx, features: list[str]) -> dict:
    """The same document as oracle.compute(), from Cypher on lineage.lbdb."""
    def answer(tool, **args):
        return tools.TOOLS[tool](ctx, **args)
    return {**queries.contract_sections(ctx.lineage_conn), "pit": oracle.pit_all(answer, features),
            "questions": oracle.questions(answer)}


def _ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


def cold_latency(bdir: Path) -> dict:
    """What a fresh read-only connection costs: opening it, then the first call of each question on it
    (in the plan's order, so the first one also pays for the empty page cache and query compilation)."""
    t0 = time.perf_counter()
    with tools.LineageContext(bdir) as fresh:
        if fresh.lineage_conn is None:
            return {}
        cold = {"open_ms": _ms(t0), "first_call_ms": {}}
        for key, tool, args in spec.QUESTIONS:
            t1 = time.perf_counter()
            tools.TOOLS[tool](fresh, **args)
            cold["first_call_ms"][key] = _ms(t1)
    return cold


def check_twice(rep: Report, bdir: Path, v: dict, t: oracle.Tables) -> tuple[dict, dict]:
    rep.section("Ladybug (Cypher on lineage.lbdb, read only, 128 MB pool) = Python oracle")
    timings: dict[str, dict] = {}
    cold = cold_latency(bdir)   # before any other connection has warmed the pages up
    with tools.LineageContext(bdir) as ctx:
        conn = ctx.lineage_conn
        if conn is None:
            rep.error(f"{spec.DB_FILE} is missing: run make lineage-local")
            return timings, cold
        want = t.counts()
        got_nodes = {label: queries.fetch(conn, "count_nodes", label=label)[0]["n"] for label in spec.NODE_SCHEMA}
        got_edges = {rel: queries.fetch(conn, "count_rels", rel=rel)[0]["n"] for rel in spec.EDGE_SCHEMA}
        rep.equal("Ladybug node counts", got_nodes, want["nodes"], "Parquet")
        rep.equal("Ladybug edge counts", got_edges, want["edges"], "Parquet")
        c = cypher_values(ctx, [r["feature"] for r in v["pit"]])
        for key in v:
            if key == "questions":
                continue
            diffs = oracle.diff(v[key], c.get(key))
            rep.check(not diffs, f"{key}: Cypher = oracle", _shown(diffs))
        for key, want_q in v["questions"].items():
            diffs = oracle.diff(want_q, c["questions"].get(key))
            data = want_q["data"]
            size = len(data.get("edges", data.get("features", data.get("assertions", data.get("columns",
                       data.get("unguarded", []))))))
            rep.check(not diffs, f"{key}: {want_q['tool']}({', '.join(f'{k}={a}' for k, a in want_q['args'].items())})"
                                 f" -> {size} rows, Cypher = oracle", _shown(diffs))
        rep.section(f"Latency (warm, {LATENCY_RUNS} runs each, and cold; reported, never gated)")
        for key, tool, args in spec.QUESTIONS:
            fn = tools.TOOLS[tool]
            fn(ctx, **args)
            runs = []
            for _ in range(LATENCY_RUNS):
                t0 = time.perf_counter()
                fn(ctx, **args)
                runs.append((time.perf_counter() - t0) * 1000)
            timings[key] = {"best_ms": round(min(runs), 2), "median_ms": round(statistics.median(runs), 2),
                            "max_ms": round(max(runs), 2)}
            slow = "" if timings[key]["median_ms"] < LATENCY_TARGET_MS else \
                f"  (median above {LATENCY_TARGET_MS:g} ms: a busy machine, or a regression)"
            rep.note(f"{key}: best {timings[key]['best_ms']} ms, median {timings[key]['median_ms']} ms, max "
                     f"{timings[key]['max_ms']} ms{slow}")
    if cold:
        first = cold["first_call_ms"]
        rep.note(f"cold (a fresh read-only connection, 128 MB pool): open {cold['open_ms']} ms, then the first call "
                 f"of each question {', '.join(f'{k} {ms} ms' for k, ms in first.items())}; the {LATENCY_TARGET_MS:g} "
                 f"ms target is for warm calls (a served connection stays open)")
    return timings, cold


def latency_summary(timings: dict, cold: dict) -> str:
    """``; slowest question Q15_x 2.5 ms (median, warm); slowest first call Q13_y 23 ms (cold, after a 55 ms
    open)`` for the final line: the warm figure the target is about, next to the dearest first call on a fresh
    connection (named: it need not be the first question asked on it)."""
    out = ""
    if timings:
        key, x = max(timings.items(), key=lambda kv: kv[1]["median_ms"])
        out += f"; slowest question {key} {x['median_ms']} ms (median, warm)"
    first = (cold or {}).get("first_call_ms") or {}
    if first:
        key, ms = max(first.items(), key=lambda kv: kv[1])
        out += f"; slowest first call {key} {ms} ms (cold, after a {cold['open_ms']} ms open)"
    return out


# --------------------------------------------------------------------------- 6-8
def check_goldens(rep: Report, man: dict, v: dict) -> str | None:
    rep.section("Goldens (semantic answers; generated by the oracle, never typed)")
    doc, note, exact = oracle.find_golden(man)
    if doc is None:
        rep.warn(f"goldens skipped: {note}")
        return None
    diffs = oracle.diff(doc["values"], v)
    what = ("gold columns, PIT statuses, COUNT(*) windows, unused and unguarded columns, range guarantees, LEAKY, "
            "parameters, invariant 9, churn-gold sub-graph counts, bridge, the plan's lineage questions")
    if man["profile"] != oracle.GOLDEN_PROFILE:
        if diffs:
            rep.note(f"{man['profile']} profile: {len(diffs)} value(s) differ from the core golden (extra consumers "
                     f"and snapshots; reported, not gated): {_shown(diffs, 3)}")
        else:
            rep.ok(f"oracle = {note}")
        return Path(oracle.golden_path()).name
    if exact:
        rep.check(not diffs, f"oracle = {note}: {what}", _shown(diffs, 8))
    elif diffs:
        rep.warn(f"the pipeline code differs from {note} and {len(diffs)} golden value(s) differ: {_shown(diffs, 8)}. "
                 f"If the renewal model changed on purpose, regenerate the golden: python -m "
                 f"lakehouse_graph.lineage.oracle --build <dir> --print-golden")
    else:
        rep.ok(f"oracle = {note}: {what}")
        rep.note("the pipeline files differ from the golden's (other bytes, same answers)")
    sub, g = v["subgraph"], v["gold_columns"]
    rep.note(f"churn gold sub-graph: {sub['gold_derived_from_edges']} DERIVED_FROM + "
             f"{sub['gold_counts_rows_of_edges']} COUNTS_ROWS_OF edges from {g['sql_columns']} SQL columns, reading "
             f"{sub['distinct_silver_columns_read']} distinct silver columns; window usage "
             + ", ".join(f"{k} {n}" for k, n in sorted(sub["window_usage"].items(), key=lambda kv: -kv[1])))
    return Path(oracle.golden_path()).name


def check_interference(rep: Report, before: dict) -> None:
    rep.section("Non-interference")
    now = mf.guarded_hashes()
    rep.check(now == before, f"data/sample/churn/* and data/export/* unchanged by this check ({len(now)} files)",
              _fmt(sorted(k for k in set(now) | set(before) if now.get(k) != before.get(k))))


def check_lint(rep: Report) -> None:
    rep.section("Template lint")
    bad = []
    for name, text in queries.TEMPLATES.items():
        last = text.rstrip().splitlines()[-1]
        if not re.search(r"\bORDER\s+BY\b", text, re.IGNORECASE) or not re.search(r"\bLIMIT\s+(\$\w+|\d+)\s*$", last):
            bad.append(name)
    rep.check(not bad, f"all {len(queries.TEMPLATES)} lineage Cypher templates have ORDER BY and end with LIMIT",
              str(bad))


def report_totals(rep: Report, man: dict, v: dict) -> None:
    rep.section("Whole-graph totals (reported, not gated)")
    c = man["counts"]
    rep.note(f"{c['total_nodes']:,} nodes: " + ", ".join(f"{k} {n}" for k, n in c["nodes"].items() if n))
    rep.note(f"{c['total_edges']:,} edges: " + ", ".join(f"{k} {n}" for k, n in c["edges"].items() if n))
    lb = man["ladybug"]
    rep.note(f"build {man['builder']['seconds']} s; Ladybug {lb['version']} load {lb['load_s']} s, "
             f"{lb['db_bytes'] / 1e6:.1f} MB, pool {lb['buffer_pool_mb']} MB, {lb['threads']} threads")
    q17, q16 = v["questions"]["Q17_unused_silver"]["data"], v["questions"]["Q16_unguarded_gold"]["data"]
    rg = v["range_guarantees"]
    rep.note(f"{len(q17['columns'])} silver churn columns never read by gold; "
             f"{len(q16['unguarded'])} gold columns with no value check "
             f"({', '.join(r.rsplit('.', 1)[-1] for r in q16['unguarded'])}); {rg['guaranteed']} of {rg['ranges']} "
             f"contract ranges guaranteed by the SQL")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate a lineage build (contract metadata-graph/0.1).")
    ap.add_argument("--graph-profile", default="default", help="business build profile (its latest build)")
    ap.add_argument("--build", default=None, help="build directory holding lineage/ and lineage.lbdb")
    ap.add_argument("--graph-root", default=None)
    ap.add_argument("--strict", action="store_true", help="warnings fail too")
    ap.add_argument("--no-ladybug", action="store_true", help="skip the Cypher half (warns)")
    ap.add_argument("--json", default=None, help="write the full result document here")
    ap.add_argument("--print-golden", action="store_true",
                    help="print the golden document of this build (from the Python oracle) and exit; commit it as "
                         "src/lakehouse_graph/lineage/goldens/core.json after reviewing the diff")
    a = ap.parse_args(argv)

    try:
        bdir = Path(a.build).absolute() if a.build else gspec.latest_link(a.graph_profile, a.graph_root)
    except ValueError as e:
        print(f"Lineage contract FAILED: {e}", file=sys.stderr)
        return 1
    if not (bdir / spec.LINEAGE_DIR / spec.MANIFEST_FILE).is_file():
        print(f"Lineage contract FAILED: no lineage build in {bdir} (run make lineage-local, or "
              f"scripts/build_lineage_local.py --build <dir>)", file=sys.stderr)
        return 1
    bdir = bdir.resolve()
    man = lbuild.read_manifest(bdir)
    if a.print_golden:
        if man["profile"] != oracle.GOLDEN_PROFILE:
            print(f"Lineage golden FAILED: goldens are generated from a {oracle.GOLDEN_PROFILE} build, this one "
                  f"is {man['profile']}", file=sys.stderr)
            return 1
        sys.stdout.write(oracle.dumps(oracle.golden(bdir)))
        return 0
    rep = Report()
    before = mf.guarded_hashes()
    print(f"Lineage contract {spec.CONTRACT_VERSION}: profile {man['profile']}, lineage build "
          f"{man['lineage_build_id']} ({bdir}){' [strict]' if a.strict else ''}")

    v: dict = {}
    tiers: dict = {}
    timings: dict = {}
    cold: dict = {}
    golden_name = None
    intact = check_integrity(rep, bdir, man)
    check_extraction(rep, man)
    if intact:
        t = oracle.load_tables(bdir)
        check_structure(rep, man, t)
        v = oracle.compute(t)
        check_semantics(rep, v, t)
        try:
            tiers = check_tiers(rep, bdir, man, t, a.no_ladybug)
        except (RuntimeError, OSError, KeyError, IndexError, ImportError, ValueError) as e:
            rep.error(f"Tier-1 / Tier-2 checks could not complete ({type(e).__name__}: {str(e)[:300]})")
        if a.no_ladybug:
            rep.section("Ladybug")
            rep.warn("Cypher checks skipped (--no-ladybug): only the Python half of the contract ran")
        else:
            try:
                timings, cold = check_twice(rep, bdir, v, t)
            except (RuntimeError, OSError, KeyError, IndexError, ImportError, ValueError) as e:
                rep.error(f"Ladybug checks could not complete ({type(e).__name__}: {str(e)[:300]}); Parquet is "
                          f"canonical, rebuild {spec.DB_FILE} with make lineage-local")
        golden_name = check_goldens(rep, man, v)
        report_totals(rep, man, v)
    else:
        rep.note("the remaining checks need intact tables: rebuild with make lineage-local")
    check_interference(rep, before)
    check_lint(rep)

    errors = list(rep.errors)
    if a.strict and rep.warnings:
        errors.append(f"{len(rep.warnings)} warning(s) are fatal with --strict")
    status = "fail" if errors else "pass"
    doc = {"contract": spec.CONTRACT_VERSION, "status": status, "strict": bool(a.strict), "profile": man["profile"],
           "lineage_build_id": man["lineage_build_id"],
           "files_sha256": {k: x["sha256"] for k, x in man["files"].items()}, "golden": golden_name,
           "errors": rep.errors, "warnings": rep.warnings, "skipped": ["ladybug"] if a.no_ladybug else [],
           "latency_ms": timings, "latency_cold_ms": cold, "counts": man["counts"],
           "overlays": sorted(man.get("overlays") or {}), "tiers": tiers, "checked_at": mf.utc_now()}
    mf.write_json_atomic(bdir / spec.LINEAGE_DIR / spec.CONTRACT_FILE, doc)
    if a.json:
        Path(a.json).write_text(json.dumps({**doc, "values": v}, indent=1, sort_keys=True, default=str) + "\n")

    print()
    for w in rep.warnings:
        print(f"WARN: {w}", file=sys.stderr)
    if errors:
        print("Lineage contract FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    pit = v["pit"]
    exceptions = [r["feature"] for r in pit if r["pit_status"] != spec.PIT_COMPLIANT]
    sub = v["subgraph"]
    print(f"Lineage contract OK ({spec.CONTRACT_VERSION}, profile {man['profile']}, lineage build "
          f"{man['lineage_build_id']}): {v['gold_columns']['sql_columns']} gold SQL columns resolve "
          f"({sub['gold_derived_from_edges']} DERIVED_FROM + {sub['gold_counts_rows_of_edges']} COUNTS_ROWS_OF); "
          f"{len(pit) - len(exceptions)} of {len(pit)} features compliant, declared exceptions "
          f"{', '.join(exceptions)}; Cypher = oracle{latency_summary(timings, cold)}; golden "
          f"{golden_name or 'n/a'}"
          + (f"; Tier 1/2 {'+'.join(sorted(man['overlays']))} ({len(tiers.get('tables', {}))} tables, "
             f"{len(tiers.get('runs', {}))} runs)" if man.get("overlays") else "")
          + f"{'; strict' if a.strict else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
