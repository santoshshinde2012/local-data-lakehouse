"""Named Cypher templates for the lineage graph (LadybugDB dialect) and the answer shapes.

Rules (lint: tests/graph/test_lineage_contract.py and the lineage contract):
  * every template has ORDER BY and LIMIT: result order is undefined without ORDER BY;
  * no UNION, no write or file statement; values always travel as ``$parameters`` (ids come
    from the graph itself, user input is validated before it reaches a template);
  * LIMIT is a literal (``ROW_LIMIT``): measured on LadybugDB 0.21.1, ``LIMIT $param`` makes
    the same query 3-4x slower than a literal limit;
  * labels are back-quoted: ``Export`` and friends collide with engine keywords, and the
    column label is ``DataColumn`` because ``Column`` is reserved;
  * ``{label}`` / ``{rel}`` placeholders are filled by ``render()`` from spec names only;
  * templates whose name starts with ``contract_`` are used by the contract script, not by
    the agent tools.

The ``*_result`` and ``*_caveats`` functions below turn raw rows into the dictionaries the
tools return. They are shared by the Cypher path (``tools.py``) and the pure-Python oracle
(``oracle.py``), so the contract compares how the rows were *found*, not how they are laid out.
"""
from __future__ import annotations

from .. import spec as gspec
from . import spec

INF = float("inf")
ROW_LIMIT = 10_000   # the literal LIMIT of every multi-row template (the whole graph is ~2k edges)

TEMPLATES: dict[str, str] = {
    # ---- counts ----------------------------------------------------------------------
    "count_nodes": "MATCH (n:`{label}`) RETURN count(n) AS n ORDER BY n LIMIT 1",
    "count_rels": "MATCH ()-[e:`{rel}`]->() RETURN count(e) AS n ORDER BY n LIMIT 1",

    # ---- ColumnRef resolution ----------------------------------------------------------
    "column_by_ref": """
MATCH (c:`DataColumn`) WHERE c.ref = $ref
RETURN c.id AS id, c.ref AS ref, c.name AS name, c.dataset AS dataset, c.layer AS layer, c.domain AS domain,
       c.n_sources AS n_sources, c.n_readers AS n_readers
ORDER BY id LIMIT 2""",
    "column_refs": """
MATCH (c:`DataColumn`) WHERE c.ref IS NOT NULL
RETURN c.ref AS ref
ORDER BY ref LIMIT 10000""",

    # ---- lineage_trace: one hop per call, the tool walks level by level. to_more is the
    # reached column's own degree in the walk direction (n_sources / n_readers, stored at
    # build time), so a column with nothing beyond it is never queried again.
    "trace_upstream_step": """
MATCH (c:`DataColumn`)-[e:DERIVED_FROM|COUNTS_ROWS_OF]->(s) WHERE c.id IN $ids
RETURN label(e) AS rel, c.id AS from_id, s.id AS to_id, s.ref AS to_ref, label(s) AS to_label, s.layer AS to_layer,
       s.n_sources AS to_more, e.roles AS roles, e.cte AS cte, e.window AS window,
       e.matched_window AS matched_window, e.transform AS transform
ORDER BY rel, from_id, to_id, cte, roles, window LIMIT 10000""",
    "trace_downstream_step": """
MATCH (x:`DataColumn`)-[e:DERIVED_FROM]->(c:`DataColumn`) WHERE c.id IN $ids
RETURN c.id AS from_id, x.id AS to_id, x.ref AS to_ref, x.layer AS to_layer, x.n_readers AS to_more,
       e.roles AS roles, e.cte AS cte, e.window AS window, e.matched_window AS matched_window,
       e.transform AS transform
ORDER BY from_id, to_id, cte, roles, window LIMIT 10000""",
    "trace_parameters": """
MATCH (p:`Parameter`)-[:USED_BY]->(c:`DataColumn`) WHERE c.id IN $ids
RETURN c.id AS from_id, p.id AS to_id, p.sql_value AS sql_value, p.consistent AS consistent
ORDER BY from_id, to_id LIMIT 10000""",
    "trace_consumers": """
MATCH (x:`Assertion`:`GraphElement`:`Metric`)-[e:CHECKS|SOURCED_FROM|USES|MATERIALIZED_AS]->(c:`DataColumn`)
WHERE c.id IN $ids
RETURN label(e) AS rel, c.id AS from_id, x.id AS to_id, label(x) AS to_label, x.kind AS kind,
       x.severity AS severity
ORDER BY rel, from_id, to_id LIMIT 10000""",
    "trace_contracts": """
MATCH (k:`Contract`)-[:HAS_ASSERTION]->(a:`Assertion`) WHERE a.id IN $ids
RETURN a.id AS from_id, k.id AS to_id
ORDER BY from_id, to_id LIMIT 10000""",
    "trace_export_consumers": """
MATCH (x:`Export`)-[:HAS_COLUMN]->(c:`DataColumn`) WHERE c.id IN $ids
MATCH (y:`DownstreamRepo`:`Contract`)-[e:CONSUMES|CONSUMES_VIA]->(x)
RETURN label(e) AS rel, c.id AS from_id, y.id AS to_id, label(y) AS to_label, x.ref AS export_ref
ORDER BY rel, from_id, to_id LIMIT 10000""",

    # ---- lineage_pit -------------------------------------------------------------------
    "pit_features": """
MATCH (c:`DataColumn`) WHERE c.dataset = $dataset AND c.role = 'feature'
RETURN c.id AS id, c.name AS feature, c.pit_status AS pit_status,
       c.max_upper_vs_as_of_days AS max_upper_vs_as_of_days, c.reads_after_as_of AS reads_after_as_of,
       c.declared_leaky AS declared_leaky, c.ordinal AS ordinal
ORDER BY ordinal LIMIT 10000""",
    "pit_windows": """
MATCH (c:`DataColumn`)-[u:USES_WINDOW]->(w:`Window`) WHERE c.id IN $ids
RETURN c.id AS id, w.display AS window, w.reads_after_as_of AS reads_after_as_of,
       u.source_dataset AS source_dataset, u.event_column AS event_column
ORDER BY id, window, source_dataset, event_column LIMIT 10000""",
    "pit_rule": """
MATCH (r:`PointInTimeRule`) WHERE r.id = $id
RETURN r.id AS id, r.statement AS statement, r.as_of_offset_days AS as_of_offset_days, r.source AS source
ORDER BY id LIMIT 1""",
    # reads with no time bound of their own (a declared reference table matched to bounded event
    # rows, or an exception): they have no USES_WINDOW edge, so the tool lists them separately
    "pit_unbounded_reads": """
MATCH (c:`DataColumn`)-[e:DERIVED_FROM]->(s:`DataColumn`)<-[:HAS_COLUMN]-(d:`Dataset`)
WHERE c.id IN $ids AND e.window = 'unbounded'
RETURN c.id AS id, d.ref AS dataset, s.name AS source_column, e.matched_window AS matched_window,
       d.reference_data AS reference_data
ORDER BY id, dataset, source_column, matched_window LIMIT 10000""",
    "pit_unbounded_rowcounts": """
MATCH (c:`DataColumn`)-[e:COUNTS_ROWS_OF]->(d:`Dataset`) WHERE c.id IN $ids AND e.window = 'unbounded'
RETURN c.id AS id, d.ref AS dataset, d.reference_data AS reference_data
ORDER BY id, dataset LIMIT 10000""",

    # ---- lineage_guards ----------------------------------------------------------------
    "guards_export_columns": """
MATCH (x:`DataColumn`)-[:DERIVED_FROM]->(c:`DataColumn`) WHERE c.id = $id AND x.layer = 'export'
RETURN x.id AS id
ORDER BY id LIMIT 10000""",
    "guards_assertions": """
MATCH (a:`Assertion`)-[:CHECKS]->(c:`DataColumn`) WHERE c.id IN $ids
RETURN a.id AS id, a.contract AS contract, a.kind AS kind, a.severity AS severity, a.min AS min_value,
       a.max AS max_value, a.source_line AS source_line, a.source AS source, c.ref AS checked_column
ORDER BY contract, kind, checked_column, id LIMIT 10000""",
    "guards_same_rule": """
MATCH (a:`Assertion`)-[s:SAME_RULE_AS]-(b:`Assertion`) WHERE a.id IN $ids
RETURN a.id AS id, b.id AS other, b.contract AS contract, b.severity AS severity,
       s.severity_differs AS severity_differs, s.bounds_equal AS bounds_equal
ORDER BY id, other LIMIT 10000""",
    "guards_checked_layers": """
MATCH (:`Assertion`)-[:CHECKS]->(c:`DataColumn`)
RETURN DISTINCT c.layer AS layer
ORDER BY layer LIMIT 100""",
    "guards_dataset_columns": """
MATCH (c:`DataColumn`) WHERE c.dataset = $dataset
RETURN c.id AS id, c.ref AS ref, c.ordinal AS ordinal
ORDER BY ordinal LIMIT 10000""",
    "guards_value_checked_direct": """
MATCH (a:`Assertion`)-[:CHECKS]->(c:`DataColumn`) WHERE c.dataset = $dataset AND NOT a.kind IN $non_value
RETURN DISTINCT c.id AS id
ORDER BY id LIMIT 10000""",
    "guards_value_checked_via_export": """
MATCH (a:`Assertion`)-[:CHECKS]->(x:`DataColumn`)-[:DERIVED_FROM]->(c:`DataColumn`)
WHERE c.dataset = $dataset AND x.layer = 'export' AND NOT a.kind IN $non_value
RETURN DISTINCT c.id AS id
ORDER BY id LIMIT 10000""",

    # ---- lineage_unused ----------------------------------------------------------------
    "unused_columns": """
MATCH (c:`DataColumn`) WHERE c.layer = $layer AND c.domain = $domain
OPTIONAL MATCH (x:`DataColumn`)-[e:DERIVED_FROM]->(c)
WITH c, count(e) AS readers
RETURN c.id AS id, c.ref AS ref, c.dataset AS dataset, c.ordinal AS ordinal, readers
ORDER BY dataset, ordinal LIMIT 10000""",
    "unused_graph_readers": """
MATCH (g:`GraphElement`)-[:SOURCED_FROM]->(c:`DataColumn`) WHERE c.id IN $ids
RETURN c.ref AS ref, g.id AS element
ORDER BY ref, element LIMIT 10000""",

    # ---- contract-only: the golden sections that are not tool answers -------------------
    "contract_gold_columns": """
MATCH (c:`DataColumn`) WHERE c.dataset = $dataset
OPTIONAL MATCH (c)-[d:DERIVED_FROM]->(:`DataColumn`)
WITH c, count(d) AS derived_from
OPTIONAL MATCH (c)-[r:COUNTS_ROWS_OF]->(:`Dataset`)
WITH c, derived_from, count(r) AS counts_rows_of
RETURN c.name AS name, c.ordinal AS ordinal, c.role AS role, c.pit_status AS pit_status,
       c.expr_sql AS expr_sql, derived_from, counts_rows_of
ORDER BY ordinal LIMIT 10000""",
    "contract_counts_rows_of": """
MATCH (c:`DataColumn`)-[r:COUNTS_ROWS_OF]->(d:`Dataset`)
RETURN c.ref AS column_ref, d.ref AS dataset, r.window AS window
ORDER BY column_ref LIMIT 10000""",
    "contract_ranges": """
MATCH (c:`DataColumn`) WHERE c.dataset = $dataset AND c.range_guarantee IS NOT NULL
RETURN c.name AS name, c.contract_min AS contract_min, c.contract_max AS contract_max,
       c.range_guarantee AS range_guarantee, c.sql_clamp_min AS sql_clamp_min, c.sql_clamp_max AS sql_clamp_max
ORDER BY name LIMIT 10000""",
    "contract_leaky": """
MATCH (k:`Contract`)-[:HAS_ASSERTION]->(a:`Assertion`) WHERE a.kind = 'no_leak'
RETURN k.id AS contract, k.dangling_refs AS dangling, a.accepted_values AS declared
ORDER BY contract LIMIT 10000""",
    "contract_parameters": """
MATCH (p:`Parameter`)
RETURN p.name AS name, p.sql_value AS sql_value, p.pandas_value AS pandas_value,
       p.generator_value AS generator_value, p.consistent AS consistent, p.unused AS unused
ORDER BY name LIMIT 10000""",
    "contract_feature_reads": """
MATCH (c:`DataColumn`)-[e:DERIVED_FROM|COUNTS_ROWS_OF]->(s) WHERE c.dataset = $dataset AND c.role = 'feature'
RETURN c.name AS feature, c.ordinal AS ordinal, label(e) AS rel, e.window AS window,
       e.matched_window AS matched_window, label(s) AS to_label, s.name AS name, s.dataset AS dataset, e.cte AS cte
ORDER BY ordinal, rel, dataset, name, cte, window, matched_window LIMIT 10000""",
    "contract_gold_descendants": """
MATCH (s:`DataColumn`)<-[e:DERIVED_FROM*1..4]-(g:`DataColumn`) WHERE s.id = $id AND g.layer = 'gold'
RETURN g.ref AS gold, properties(nodes(e), 'ref') AS via
ORDER BY gold LIMIT 10000""",
    "contract_subgraph_edges": """
MATCH (c:`DataColumn`)-[e:DERIVED_FROM]->(s:`DataColumn`) WHERE c.dataset = $dataset
RETURN count(e) AS edges, count(DISTINCT s.id) AS sources
ORDER BY edges LIMIT 1""",
    "contract_subgraph_rowcounts": """
MATCH (c:`DataColumn`)-[e:COUNTS_ROWS_OF]->(:`Dataset`) WHERE c.dataset = $dataset
RETURN count(e) AS edges
ORDER BY edges LIMIT 1""",
    "contract_window_usage": """
MATCH (c:`DataColumn`)-[u:USES_WINDOW]->(w:`Window`) WHERE c.dataset = $dataset
RETURN w.display AS window, count(u) AS n
ORDER BY window LIMIT 10000""",
    "contract_bridge": """
MATCH (g:`GraphElement`)
OPTIONAL MATCH (g)-[:SOURCED_FROM]->(d:`Dataset`)
WITH g, count(d) AS datasets
OPTIONAL MATCH (g)-[:SOURCED_FROM]->(c:`DataColumn`)
WITH g, datasets, count(c) AS source_columns
OPTIONAL MATCH (g)-[:MATERIALIZED_AS]->(t:`Dataset`)
RETURN g.id AS element, g.kind AS kind, datasets, source_columns, count(t) AS tables
ORDER BY element LIMIT 10000""",

    # ---- contract-only: Tier 1 / Tier 2 facts (filled by --iceberg / --openlineage) ---------
    "contract_tier_snapshots": """
MATCH (d:`Dataset`)-[:HAS_SNAPSHOT]->(s:`Snapshot`)
RETURN d.name AS dataset, s.id AS snapshot, s.snapshot_id AS snapshot_id, s.is_current AS is_current,
       s.in_catalog AS in_catalog
ORDER BY dataset, snapshot LIMIT 10000""",
    "contract_tier_supersedes": """
MATCH (a:`Snapshot`)-[e:SUPERSEDES]->(b:`Snapshot`)
RETURN a.id AS newer, b.id AS older, e.parent_matches AS parent_matches
ORDER BY newer, older LIMIT 10000""",
    "contract_tier_refs": """
MATCH (r:`Ref`)
OPTIONAL MATCH (r)-[:POINTS_TO]->(s:`Snapshot`)
RETURN r.id AS ref, r.name AS name, r.kind AS kind, r.table_name AS table_name, s.id AS snapshot
ORDER BY ref, snapshot LIMIT 10000""",
    "contract_tier_runs": """
MATCH (r:`Run`)
OPTIONAL MATCH (r)-[:RAN_AS]->(j:`Job`)
RETURN r.id AS run, r.kind AS kind, j.id AS job
ORDER BY run, job LIMIT 10000""",
    "contract_tier_parents": """
MATCH (a:`Run`)-[e:PARENT]->(b:`Run`)
RETURN a.id AS child, b.id AS parent, e.kind AS kind
ORDER BY child, parent LIMIT 10000""",
    "contract_tier_produced": """
MATCH (s:`Snapshot`)-[:PRODUCED_BY_RUN]->(r:`Run`)
RETURN s.id AS snapshot, r.id AS run
ORDER BY snapshot, run LIMIT 10000""",
    "contract_tier_consumed": """
MATCH (r:`Run`)-[e:CONSUMED_SNAPSHOT]->(s:`Snapshot`)
RETURN r.id AS run, s.id AS snapshot, e.role AS role
ORDER BY run, snapshot LIMIT 10000""",
}
TIER_TEMPLATES = tuple(k for k in TEMPLATES if k.startswith("contract_tier_"))
CONTRACT_ONLY = sorted(k for k in TEMPLATES if k.startswith("contract_"))


def render(name: str, **identifiers: str) -> str:
    """Template text with ``{label}`` / ``{rel}`` filled from spec names (never from user input)."""
    q = TEMPLATES[name]
    for key, value in identifiers.items():
        allowed = spec.NODE_SCHEMA if key == "label" else spec.EDGE_SCHEMA if key == "rel" else None
        if allowed is None or value not in allowed:
            raise ValueError(f"{key}={value!r} is not a lineage graph {key}")
    return q.format(**identifiers) if identifiers else q


def fetch(conn, name: str, params: dict | None = None, **identifiers: str) -> list[dict]:
    """Run a named template; rows as dicts keyed by the RETURN aliases."""
    q = render(name, **identifiers)
    res = conn.execute(q, {k: v for k, v in (params or {}).items() if f"${k}" in q})
    cols = res.get_column_names()
    out = []
    while res.has_next():
        out.append(dict(zip(cols, res.get_next(), strict=True)))
    return out


# --------------------------------------------------------------------------- lineage_trace
def trace_row(depth: int, rel: str, source: str, target: str, roles=None, cte=None, window=None,
              transform=None, matched=None) -> dict:
    """One hop away from the target: ``from`` is the nearer node, ``to`` the node reached.
    ``matched`` (DERIVED_FROM.matched_window) is shown with the window of an unbounded read."""
    if window and matched:
        window = f"{window}, matched to event rows in {matched}"
    return {"depth": depth, "rel": rel, "from": source, "to": target, "roles": roles, "cte": cte, "window": window,
            "transform": transform}


def trace_sort_key(row: dict) -> tuple:
    """Depth, then relation and ids; CTE, roles, window and transform make the order total (one
    column can be read twice in one CTE through two different windows)."""
    return (row["depth"], row["rel"], row["from"], row["to"], row["cte"] or "", row["roles"] or "",
            row["window"] or "", row["transform"] or "")


def assertion_text(kind, severity) -> str:
    return f"{kind} [{severity}]"


_REACHED = {"Dataset": "datasets", "Assertion": "assertions", "Contract": "contracts",
            "GraphElement": "graph_elements", "Metric": "metrics", "Parameter": "parameters",
            "DownstreamRepo": "consumers"}


def trace_result(target: str, direction: str, max_depth: int, rows: list[dict], info: dict[str, tuple],
                 stopped: bool) -> dict:
    """The ``lineage_trace`` answer from its hop rows.

    ``info`` maps every ``to`` name to (node label, layer); ``stopped`` says the walk still
    had columns to expand when it reached ``max_depth``.
    """
    rows = sorted(rows, key=trace_sort_key)
    reached: dict = {"columns": {}, **{k: [] for k in _REACHED.values()}}
    for r in rows:
        label, layer = info[r["to"]]
        bucket = reached["columns"].setdefault(layer, []) if label == "DataColumn" else reached[_REACHED[label]]
        if r["to"] not in bucket:
            bucket.append(r["to"])
    reached["columns"] = {layer: sorted(v) for layer, v in sorted(reached["columns"].items())}
    for k in _REACHED.values():
        reached[k].sort()
    # the counts stay readable when an envelope has to cut the reached lists and the edge rows
    counts = {**{f"{layer}_columns": len(v) for layer, v in reached["columns"].items()},
              **{k: len(reached[k]) for k in _REACHED.values() if reached[k]}}
    summary = {"edges": len(rows), "columns": sum(len(v) for v in reached["columns"].values()),
               "reached_counts": counts, "max_depth_reached": max((r["depth"] for r in rows), default=0),
               "stopped_at_max_depth": stopped}
    lineage = [r for r in rows if r["rel"] == "DERIVED_FROM"]
    if direction == "downstream":
        children: dict[str, list[str]] = {}
        for r in lineage:
            children.setdefault(r["from"], []).append(r["to"])
        branches = []
        for first in sorted({r["to"] for r in lineage if r["depth"] == 1}):
            seen, stack = {first}, [first]
            while stack:
                for nxt in children.get(stack.pop(), []):
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            hop = next(r for r in lineage if r["depth"] == 1 and r["to"] == first)
            gold = sorted(c for c in seen - {first} if info[c][1] == "gold")
            exports = sorted(c for c in seen - {first} if info[c][1] == "export")
            branches.append({
                "via": first, "transform": hop["transform"], "gold": gold, "exports": exports,
                "graph_elements": sorted(r["to"] for r in rows if r["rel"] == "SOURCED_FROM" and r["from"] == first),
                "dead_end": not children.get(first) and info[first][1] != "export"})
        summary["branches"] = branches
    else:
        has_upstream = {r["from"] for r in lineage}
        summary["sources"] = sorted({r["to"] for r in lineage if r["to"] not in has_upstream})
    # summary and reached come before the edge rows: an envelope that truncates cuts the rows, not the answer
    return {"target": target, "direction": direction, "max_depth": max_depth, "summary": summary, "reached": reached,
            "edges": rows}


def trace_caveats(data: dict) -> list[str]:
    out = ["Code-derived lineage (Tier 0): it says what the code reads and writes, not which rows a run touched."]
    profile_has_radar = any(c.startswith("contract:radar_") for c in data["reached"]["contracts"])
    if data["summary"]["stopped_at_max_depth"]:
        out.append(f"The walk stopped at max_depth={data['max_depth']}; more columns lie beyond it.")
    if data["summary"]["edges"] > spec.TRACE_LARGE_EDGES:
        out.append(f"Large answer ({data['summary']['edges']} edge rows, ordered by depth): summary and reached are "
                   f"complete even if the edge rows are cut; use a smaller max_depth or trace a column further "
                   f"{data['direction']} for fewer rows.")
    if data["direction"] == "downstream":
        dead = [b["via"] for b in data["summary"].get("branches", []) if b["dead_end"]]
        if dead:
            out.append("Dead end (no gold or export column reads it): " + ", ".join(dead) + ".")
        if not data["reached"]["columns"]:
            out.append("No gold or export column is derived from this column"
                       + (": only the consumers listed read it." if data["edges"] else "; nothing reads it."))
        if data["reached"]["columns"].get("export") and not profile_has_radar:
            out.append("The retention-radar contract is not in this build (core profile, or no RADAR_DIR): the "
                       "consumer side shows only what this repo's code and CI state.")
    elif not data["edges"]:
        out.append("This column has no upstream column in the graph (a source column, or a column a job adds).")
    return out


# --------------------------------------------------------------------------- lineage_pit
def window_upper(display: str | None) -> float | None:
    """Upper bound, in days after as_of, of a window as displayed (``(as_of-28, as_of]`` -> 0,
    ``(-inf, as_of+7)`` -> 7); None for ``unbounded`` or no window."""
    if not display or display == "unbounded":
        return None
    term = display[1:-1].split(",")[1].strip()
    if term in ("inf", "-inf"):
        return INF if term == "inf" else -INF
    if not term.startswith("as_of"):
        raise ValueError(f"not a window relative to as_of: {display!r}")
    return float(term[len("as_of"):] or 0)


def read_upper(window: str | None, matched_window: str | None, table: str | None) -> float | None:
    """Upper bound of one read of a gold column. None: a column of the as-of snapshot row (no
    window is stored). An unbounded read counts as the window it is matched to only for a
    declared reference table (spec.GLOBAL_DIMENSION_TABLES); otherwise it has no upper bound."""
    if window is None:
        return None
    upper = window_upper(window)
    if upper is not None:
        return upper
    if matched_window and table in spec.GLOBAL_DIMENSION_TABLES:
        matched = window_upper(matched_window)
        return INF if matched is None else matched
    return INF


def pit_derivation_section(reads: list[dict]) -> list[dict]:
    """Point-in-time status of every gold feature recomputed from its DERIVED_FROM / COUNTS_ROWS_OF
    edge windows alone (the loosest upper bound over its reads), in gold column order. The
    contract compares it with the pit_status the assembler stored on the column.

    ``reads``: one row per edge {feature, ordinal, window, matched_window, table}.
    """
    by_feature: dict[tuple[int, str], list[dict]] = {}
    for r in reads:
        by_feature.setdefault((r["ordinal"], r["feature"]), []).append(r)
    out = []
    for (_ordinal, feature), rows in sorted(by_feature.items()):
        highs = [u for r in rows if (u := read_upper(r["window"], r["matched_window"], r["table"])) is not None]
        upper = max(highs, default=0.0)
        declared = gspec.FEATURE_CARDS.get(feature, {}).get("pit_status")
        status = spec.PIT_COMPLIANT if upper <= 0 else (
            spec.PIT_DECLARED_EXCEPTION if declared == spec.PIT_DECLARED_EXCEPTION else spec.PIT_UNDECLARED_EXCEPTION)
        out.append({"feature": feature, "pit_status": status,
                    "max_upper_vs_as_of_days": None if upper == INF else int(upper), "reads_after_as_of": upper > 0,
                    "reads": len(rows), "windows": sorted({r["window"] for r in rows if r["window"] is not None})})
    return out


def pit_result(rule: dict | None, features: list[dict], windows: list[dict], feature: str | None,
               unbounded: list[dict] | None = None) -> dict:
    """``features`` holds every gold feature; ``windows`` the USES_WINDOW rows of the selected
    ones; ``unbounded`` their reads with no time bound {id, dataset, source_column, matched_window,
    reference_data}."""
    by_id: dict[str, list[dict]] = {}
    for w in windows:
        by_id.setdefault(w["id"], []).append(w)
    loose: dict[str, dict[tuple, set]] = {}
    for u in unbounded or []:
        key = (u["dataset"], u.get("matched_window") or "", u.get("reference_data") or "")
        loose.setdefault(u["id"], {}).setdefault(key, set()).add(u.get("source_column") or "*")
    chosen = [f for f in features if (f["feature"] == feature if feature else f["pit_status"] != spec.PIT_COMPLIANT)]
    rows = []
    for f in sorted(chosen, key=lambda f: f["feature"]):
        ws = by_id.get(f["id"], [])
        late = sorted({(spec.dataset_ref(w["source_dataset"]), w["event_column"] or "", w["window"])
                       for w in ws if w["reads_after_as_of"]})
        rows.append({"feature": f["feature"], "pit_status": f["pit_status"],
                     "max_upper_vs_as_of_days": f["max_upper_vs_as_of_days"],
                     "reads_after_as_of": bool(f["reads_after_as_of"]), "declared_leaky": bool(f["declared_leaky"]),
                     "windows": sorted({w["window"] for w in ws}),
                     "late_sources": [{"dataset": d, "event_column": c or None, "window": w} for d, c, w in late],
                     "unbounded_reads": [{"dataset": d, "columns": sorted(cols), "matched_window": m or None,
                                          "reference_data": note or None}
                                         for (d, m, note), cols in sorted(loose.get(f["id"], {}).items())]})
    statuses = [f["pit_status"] for f in features]
    summary = {"features": len(features), **{s: statuses.count(s) for s in spec.FEATURE_PIT_STATUSES},
               "exceptions": sorted(f["feature"] for f in features if f["pit_status"] != spec.PIT_COMPLIANT)}
    undeclared = statuses.count(spec.PIT_UNDECLARED_EXCEPTION)
    if undeclared:
        summary[spec.PIT_UNDECLARED_EXCEPTION] = undeclared
    return {"rule": rule, "feature": feature, "features": rows, "summary": summary}


def pit_caveats(data: dict) -> list[str]:
    out = ["Derived from the gold SQL (windows relative to as_of): it says what a feature can read, not how many "
           "rows it did read after as_of."]
    for f in data["features"]:
        for u in f["unbounded_reads"]:
            if u["matched_window"] and u["reference_data"]:
                out.append(f"{f['feature']}: {u['dataset']} is read without a time bound of its own "
                           f"({u['reference_data']}); its rows count only where they are matched to event rows in "
                           f"{u['matched_window']}.")
            else:
                out.append(f"{f['feature']}: {u['dataset']} ({', '.join(u['columns'])}) is read without any time "
                           f"bound.")
        if f["pit_status"] == spec.PIT_COMPLIANT:
            continue
        where = "; ".join(f"{s['dataset']}{'.' + s['event_column'] if s['event_column'] else ' rows'} in "
                          f"{s['window']}" for s in f["late_sources"])
        kind = "declared exception" if f["pit_status"] == spec.PIT_DECLARED_EXCEPTION else "UNDECLARED exception"
        ends = "it has no upper time bound" if f["max_upper_vs_as_of_days"] is None else \
            f"its window ends {f['max_upper_vs_as_of_days']} day(s) after as_of"
        out.append(f"{f['feature']}: {kind}; {ends}{' (' + where + ')' if where else ''}.")
    if data["feature"] is None and not data["features"]:
        out.append("No feature reads data after as_of.")
    return out


# --------------------------------------------------------------------------- lineage_guards
def guards_result(column: str, assertions: list[dict], same_rule: list[dict]) -> dict:
    same: dict[str, list[dict]] = {}
    for s in same_rule:
        same.setdefault(s["id"], []).append({"assertion": s["other"], "contract": s["contract"],
                                             "severity": s["severity"], "severity_differs": s["severity_differs"],
                                             "bounds_equal": s["bounds_equal"]})
    rows = [{"assertion": a["id"], "contract": a["contract"], "kind": a["kind"], "severity": a["severity"],
             "min": a["min_value"], "max": a["max_value"], "source_line": a["source_line"], "source": a["source"],
             "checked_column": a["checked_column"],
             "same_rule_as": sorted(same.get(a["id"], []), key=lambda s: s["assertion"])}
            for a in sorted(assertions, key=lambda a: (a["contract"], a["kind"], a["checked_column"] or "", a["id"]))]
    value = [r for r in rows if r["kind"] not in spec.NON_VALUE_ASSERTION_KINDS]
    return {"column": column, "assertions": rows,
            "summary": {"assertions": len(rows), "value_checks": len(value),
                        "contracts": sorted({r["contract"] for r in rows}),
                        "severity_differs": sorted(r["assertion"] for r in rows
                                                   if any(s["severity_differs"] for s in r["same_rule_as"]))}}


def unguarded_result(columns: list[dict], checked_ids: set[str]) -> dict:
    unguarded = [c["ref"] for c in sorted(columns, key=lambda c: c["ordinal"]) if c["id"] not in checked_ids]
    return {"column": None, "dataset": spec.GOLD_DATASET_REF, "unguarded": unguarded,
            "summary": {"columns": len(columns), "value_checked": len(columns) - len(unguarded),
                        "unguarded": len(unguarded)},
            "rule": "a value check is an executed assertion on the column or on an export column derived from it; "
                    "absence checks (no_leak) and README expected values do not count"}


def guards_caveats(data: dict, checked_layers: list[str] | None = None) -> list[str]:
    """``checked_layers``: the layers whose columns carry any assertion in this build (asked for only
    when the column has none, to say where the checks are instead)."""
    if data["column"] is None:
        return ["Unguarded means no executed check looks at the column's values; it does not mean the values "
                "are wrong."]
    out = []
    layer = data["column"].split(".", 1)[0]
    if not data["assertions"]:
        out.append("No assertion checks this column.")
        if checked_layers and layer not in checked_layers:
            out.append(f"Column checks in this build attach to {' and '.join(checked_layers)} columns, not to "
                       f"{layer} columns: lineage_trace(target='{data['column']}', direction='downstream') lists "
                       f"the columns that read this one and the assertions on them.")
    elif not data["summary"]["value_checks"]:
        out.append("Only absence or documentation checks mention this column; none checks its values.")
    ranges = [a for a in data["assertions"] if a["kind"] == "range"]
    if any("warn" in (a["severity"] or "") for a in ranges):
        out.append("Range breaches are warnings in check_churn_export.py and fail only with --strict.")
    if ranges and not any(a["same_rule_as"] for a in ranges):
        out.append("No retention-radar rule is linked to these range checks in this build (core profile, or no "
                   "RADAR_DIR): the severity comparison needs the full profile with a local radar checkout.")
    return out


# --------------------------------------------------------------------------- lineage_unused
def unused_result(layer: str, domain: str, columns: list[dict], graph_readers: list[dict]) -> dict:
    unused = [c for c in columns if not c["readers"]]
    by_ref: dict[str, list[str]] = {}
    for g in graph_readers:
        by_ref.setdefault(g["ref"], []).append(g["element"])
    refs = [c["ref"] or c["id"] for c in unused]
    return {"layer": layer, "domain": domain, "read_by": {"bronze": "silver", "silver": "gold"}[layer],
            "columns": refs, "summary": {"columns": len(columns), "unused": len(unused)},
            "also_read_by_graph": {r: sorted(by_ref[r]) for r in refs if r in by_ref}}


def unused_caveats(data: dict) -> list[str]:
    out = [f"Unused means no {data['read_by']} column reads it, as a value or in a predicate (code-derived)."]
    if data["also_read_by_graph"]:
        out.append(f"The business graph reads {len(data['also_read_by_graph'])} of them (also_read_by_graph).")
    if data["layer"] == "bronze" and data["columns"]:
        out.append("Bronze lineage columns (_source_file, _ingested_at) are dropped before silver on purpose.")
    return out


# --------------------------------------------------------------------------- contract sections
def range_section(rows: list[dict]) -> dict:
    """The contract ranges of the gold features and whether the SQL guarantees them."""
    by_kind: dict[str, list[str]] = {}
    for r in rows:
        by_kind.setdefault(r["range_guarantee"].split("(")[0], []).append(r["name"])
    guaranteed = sorted(n for k, names in by_kind.items() if k != "data_dependent" for n in names)
    return {"ranges": len(rows), "guaranteed": len(guaranteed),
            "by_kind": {k: sorted(v) for k, v in sorted(by_kind.items())},
            "rows": [{"name": r["name"], "contract_min": r["contract_min"], "contract_max": r["contract_max"],
                      "range_guarantee": r["range_guarantee"]} for r in sorted(rows, key=lambda r: r["name"])]}


def gold_columns_section(rows: list[dict]) -> dict:
    sql = [r for r in rows if r["role"] != "lake_metadata"]
    unresolved = [r["name"] for r in sql if not (r["derived_from"] + r["counts_rows_of"])]
    return {"sql_columns": len(sql), "resolved": len(sql) - len(unresolved), "unresolved": unresolved,
            "added_by_job": [r["name"] for r in rows if r["role"] == "lake_metadata"],
            "rows": [{"name": r["name"], "ordinal": r["ordinal"], "role": r["role"], "pit_status": r["pit_status"],
                      "derived_from": r["derived_from"], "counts_rows_of": r["counts_rows_of"]}
                     for r in sorted(rows, key=lambda r: r["ordinal"])]}


def split_names(value) -> list[str]:
    return sorted(x for x in (value or "").split(",") if x)


def contract_sections(conn) -> dict:
    """The golden sections that are not tool answers, computed in Cypher on lineage.lbdb."""
    p = {"dataset": spec.GOLD_TABLE}
    leaky = fetch(conn, "contract_leaky")
    inv = spec.INVARIANT_9

    def paths(ref: str) -> list[list[str]]:
        col = fetch(conn, "column_by_ref", {"ref": ref})
        rows = fetch(conn, "contract_gold_descendants", {"id": col[0]["id"]}) if col else []
        return [list(x) for x in sorted({(ref, *r["via"], r["gold"]) for r in rows})]

    sub = fetch(conn, "contract_subgraph_edges", p)[0]
    bridge = fetch(conn, "contract_bridge")
    return {
        "gold_columns": gold_columns_section(fetch(conn, "contract_gold_columns", p)),
        "counts_rows_of": [{"column": r["column_ref"], "dataset": r["dataset"], "window": r["window"]}
                           for r in fetch(conn, "contract_counts_rows_of")],
        "range_guarantees": range_section(fetch(conn, "contract_ranges", p)),
        "leaky": {"declared": sorted({x for r in leaky for x in split_names(r["declared"])}),
                  "dangling": sorted({x for r in leaky for x in split_names(r["dangling"])})},
        "parameters": [{k: r[k] for k in ("name", "sql_value", "pandas_value", "generator_value", "consistent",
                                          "unused")} for r in fetch(conn, "contract_parameters")],
        "invariant_9": {"silver_column": inv["silver"], "silver_gold_descendants": paths(inv["silver"]),
                        "bronze_column": inv["bronze"], "bronze_paths_to_gold": paths(inv["bronze"])},
        "pit_derivation": pit_derivation_section([
            {"feature": r["feature"], "ordinal": r["ordinal"], "window": r["window"],
             "matched_window": r["matched_window"],
             "table": r["dataset"] if r["to_label"] == "DataColumn" else r["name"]}
            for r in fetch(conn, "contract_feature_reads", p)]),
        "subgraph": {"gold_derived_from_edges": int(sub["edges"]),
                     "gold_counts_rows_of_edges": int(fetch(conn, "contract_subgraph_rowcounts", p)[0]["edges"]),
                     "distinct_silver_columns_read": int(sub["sources"]),
                     "window_usage": {r["window"]: int(r["n"]) for r in fetch(conn, "contract_window_usage", p)}},
        "bridge": {"elements": len(bridge), "nodes": sum(r["kind"] == "node" for r in bridge),
                   "edges": sum(r["kind"] == "edge" for r in bridge),
                   "by_element": [{"element": r["element"], "kind": r["kind"], "datasets": int(r["datasets"]),
                                   "source_columns": int(r["source_columns"]), "tables": int(r["tables"])}
                                  for r in bridge]},
    }


# --------------------------------------------------------------------------- Tier 1 / Tier 2 (contract)
def tier_rows(conn) -> dict[str, list[dict]]:
    """The Tier-1 / Tier-2 rows from Cypher (the oracle builds the same rows from the Parquet)."""
    return {name.removeprefix("contract_tier_"): fetch(conn, name) for name in TIER_TEMPLATES}


def _new_run(kind=None) -> dict:
    return {"kind": kind, "jobs": [], "parents": [], "produced": 0, "consumed": 0}


def tier_section(rows: dict[str, list[dict]]) -> dict:
    """What the Iceberg / OpenLineage overlays put in the graph, per table and per run, from the rows
    of ``tier_rows`` (Cypher) or ``oracle.tier_rows`` (Python); row order does not matter."""
    tables: dict[str, dict] = {}

    def table(name: str) -> dict:
        return tables.setdefault(name, {"snapshots": 0, "current": [], "not_in_catalog": 0, "supersedes": 0,
                                        "parent_cut": 0, "tags": [], "branches": []})
    table_of = {}
    for s in rows["snapshots"]:
        t = table(s["dataset"])
        table_of[s["snapshot"]] = s["dataset"]
        t["snapshots"] += 1
        t["not_in_catalog"] += s["in_catalog"] is False
        if s["is_current"]:
            t["current"].append(s["snapshot_id"])
    for e in rows["supersedes"]:
        t = table(table_of.get(e["newer"], "?"))
        t["supersedes"] += 1
        t["parent_cut"] += e["parent_matches"] is False
    dangling = []
    for r in rows["refs"]:
        table(r["table_name"])["tags" if r["kind"] == "tag" else "branches"].append(r["name"])
        if r["snapshot"] is None:
            dangling.append(r["ref"])
    runs: dict[str, dict] = {}
    for r in rows["runs"]:
        run = runs.setdefault(r["run"], _new_run(r["kind"]))
        if r["job"] is not None:
            run["jobs"].append(r["job"])
    for p in rows["parents"]:
        runs.setdefault(p["child"], _new_run())["parents"].append(f"{p['kind']}:{p['parent']}")
    for p in rows["produced"]:
        runs.setdefault(p["run"], _new_run())["produced"] += 1
    for c in rows["consumed"]:
        runs.setdefault(c["run"], _new_run())["consumed"] += 1
    for t in tables.values():
        for k in ("current", "tags", "branches"):
            t[k].sort()
    for r in runs.values():
        r["jobs"].sort()
        r["parents"].sort()
    return {"tables": dict(sorted(tables.items())), "dangling_refs": sorted(dangling),
            "runs": dict(sorted(runs.items()))}
