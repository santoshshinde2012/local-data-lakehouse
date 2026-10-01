"""Pure-Python oracle for the lineage graph: every golden answer, from the Parquet tables.

No graph engine and no graph library: the node and edge tables are read with pyarrow into
dictionaries and every answer is a plain breadth-first walk over adjacency lists. The
lineage contract computes each answer twice, here and in Cypher on lineage.lbdb
(``tools.py`` / ``queries.contract_sections``), and requires them to be equal. Only the
row-to-answer layout functions (``queries.*_result``) are shared between the two paths.

  load_tables(build_dir) -> Tables
  lineage_trace / lineage_pit / lineage_guards / lineage_unused (tables, ...) -> (data, caveats)
  compute(tables) -> the golden value document (sections + the plan's lineage questions)
  derivation_mismatches(tables) -> stored derived properties (pit_status, range_guarantee,
      Parameter.consistent) that differ from a recomputation out of the edges and values
  golden(build_dir) / find_golden(manifest) / diff(golden, values)

Goldens are generated, never typed:
  python -m lakehouse_graph.lineage.oracle --build <build_dir> --print-golden \
      > src/lakehouse_graph/lineage/goldens/core.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from .. import manifest as mf
from . import build as lbuild
from . import queries as q
from . import spec

GOLDEN_VERSION = 1
GOLDEN_PROFILE = "core"


class Tables:
    """The lineage Parquet tables as dictionaries with out / in adjacency per edge type."""

    def __init__(self, nodes: dict[str, list[dict]], edges: dict[str, list[dict]]):
        self.by_label = nodes
        self.edges = edges
        self.nodes: dict[str, dict] = {}
        self.label: dict[str, str] = {}
        for label, rows in nodes.items():
            for r in rows:
                self.nodes[r["id"]] = r
                self.label[r["id"]] = label
        self._out: dict[tuple[str, str], list[dict]] = {}
        self._in: dict[tuple[str, str], list[dict]] = {}
        for rel, rows in edges.items():
            for e in rows:
                self._out.setdefault((rel, e["src"]), []).append(e)
                self._in.setdefault((rel, e["dst"]), []).append(e)
        self.ref_to_id = {r["ref"]: r["id"] for r in nodes["DataColumn"] if r["ref"]}

    def out(self, rel: str, node_id: str) -> list[dict]:
        return self._out.get((rel, node_id), [])

    def into(self, rel: str, node_id: str) -> list[dict]:
        return self._in.get((rel, node_id), [])

    def name(self, node_id: str) -> str:
        """ColumnRef / dataset ref when the node has one, else its id."""
        n = self.nodes[node_id]
        return (n.get("ref") or node_id) if self.label[node_id] in ("DataColumn", "Dataset") else node_id

    def counts(self) -> dict:
        nodes = {label: len(rows) for label, rows in self.by_label.items()}
        edges = {rel: len(rows) for rel, rows in self.edges.items()}
        return {"nodes": nodes, "edges": edges, "total_nodes": sum(nodes.values()), "total_edges": sum(edges.values())}


def load_tables(build_dir: str | Path) -> Tables:
    return Tables(*lbuild.read_tables(Path(build_dir) / spec.LINEAGE_DIR))


# --------------------------------------------------------------------------- the four answers
def lineage_trace(t: Tables, target: str, direction: str = "upstream", max_depth: int = spec.MAX_TRACE_DEPTH):
    start = t.ref_to_id[target]
    depth_of = {start: 0}
    info: dict[str, tuple] = {}
    rows: list[dict] = []
    frontier, expanded = [start], []
    for depth in range(1, max_depth + 1):
        if not frontier:
            break
        expanded += frontier
        reached = []
        for node in frontier:
            if direction == "upstream":
                hops = [("DERIVED_FROM", e, e["dst"]) for e in t.out("DERIVED_FROM", node)] + \
                       [("COUNTS_ROWS_OF", e, e["dst"]) for e in t.out("COUNTS_ROWS_OF", node)]
            else:
                hops = [("DERIVED_FROM", e, e["src"]) for e in t.into("DERIVED_FROM", node)]
            for rel, e, other in hops:
                info[t.name(other)] = (t.label[other], t.nodes[other].get("layer"))
                rows.append(q.trace_row(depth, rel, t.name(node), t.name(other), e.get("roles"), e.get("cte"),
                                        e.get("window"), e.get("transform"), e.get("matched_window")))
                if t.label[other] == "DataColumn" and other not in depth_of:
                    depth_of[other] = depth
                    reached.append(other)
        frontier = sorted(reached)
    # stopped: a column at max_depth still has lineage beyond it (frontier is empty when the walk ran out)
    stopped = any(bool(t.out("DERIVED_FROM", n) or t.out("COUNTS_ROWS_OF", n)) if direction == "upstream"
                  else bool(t.into("DERIVED_FROM", n)) for n in frontier)
    if direction == "upstream":
        for node in expanded:
            for e in t.into("USED_BY", node):
                p = t.nodes[e["src"]]
                info[p["id"]] = ("Parameter", None)
                rows.append(q.trace_row(depth_of[node] + 1, "USED_BY", t.name(node), p["id"],
                                        transform=f"value {p['sql_value']:g}; consistent={bool(p['consistent'])}"))
    else:
        assertion_depth: dict[str, int] = {}
        for node in expanded:
            depth = depth_of[node] + 1
            for rel in ("CHECKS", "SOURCED_FROM", "USES", "MATERIALIZED_AS"):
                for e in t.into(rel, node):
                    other = t.nodes[e["src"]]
                    label = t.label[other["id"]]
                    info[other["id"]] = (label, None)
                    text = q.assertion_text(other["kind"], other["severity"]) if label == "Assertion" else None
                    rows.append(q.trace_row(depth, rel, t.name(node), other["id"], transform=text))
                    if label == "Assertion":
                        assertion_depth[other["id"]] = min(depth, assertion_depth.get(other["id"], depth))
            for c in t.into("HAS_COLUMN", node):
                if t.label[c["src"]] != "Export":
                    continue
                for rel in ("CONSUMES", "CONSUMES_VIA"):
                    for e in t.into(rel, c["src"]):
                        info[e["src"]] = (t.label[e["src"]], None)
                        rows.append(q.trace_row(depth, rel, t.name(node), e["src"],
                                                transform=f"via {t.nodes[c['src']]['ref']}"))
        for assertion, depth in assertion_depth.items():
            if depth + 1 > max_depth:
                continue
            for e in t.into("HAS_ASSERTION", assertion):
                info[e["src"]] = ("Contract", None)
                rows.append(q.trace_row(depth + 1, "HAS_ASSERTION", assertion, e["src"]))
    data = q.trace_result(target, direction, max_depth, rows, info, stopped)
    return data, q.trace_caveats(data)


def _feature_rows(t: Tables) -> list[dict]:
    cols = [c for c in t.by_label["DataColumn"] if c["dataset"] == spec.GOLD_TABLE and c["role"] == "feature"]
    return [{"id": c["id"], "feature": c["name"], "pit_status": c["pit_status"],
             "max_upper_vs_as_of_days": c["max_upper_vs_as_of_days"], "reads_after_as_of": c["reads_after_as_of"],
             "declared_leaky": c["declared_leaky"], "ordinal": c["ordinal"]}
            for c in sorted(cols, key=lambda c: c["ordinal"])]


def lineage_pit(t: Tables, feature: str | None = None):
    features = _feature_rows(t)
    chosen = [f["id"] for f in features if (f["feature"] == feature if feature else
                                            f["pit_status"] != spec.PIT_COMPLIANT)]
    windows = []
    for cid in chosen:
        for e in t.out("USES_WINDOW", cid):
            w = t.nodes[e["dst"]]
            windows.append({"id": cid, "window": w["display"], "reads_after_as_of": w["reads_after_as_of"],
                            "source_dataset": e["source_dataset"], "event_column": e["event_column"]})
    unbounded = []
    for cid in chosen:
        for e in t.out("DERIVED_FROM", cid):
            if e["window"] == "unbounded":
                s = t.nodes[e["dst"]]
                d = t.nodes[f"ds:{s['dataset']}"]
                unbounded.append({"id": cid, "dataset": d["ref"], "source_column": s["name"],
                                  "matched_window": e["matched_window"], "reference_data": d["reference_data"]})
        for e in t.out("COUNTS_ROWS_OF", cid):
            if e["window"] == "unbounded":
                d = t.nodes[e["dst"]]
                unbounded.append({"id": cid, "dataset": d["ref"], "reference_data": d["reference_data"]})
    r = t.nodes.get("pit:features_le_as_of")
    rule = None if r is None else {k: r[k] for k in ("id", "statement", "as_of_offset_days", "source")}
    data = q.pit_result(rule, features, windows, feature, unbounded)
    return data, q.pit_caveats(data)


def _value_checked(t: Tables, column_id: str) -> bool:
    cols = [column_id] + [e["src"] for e in t.into("DERIVED_FROM", column_id)
                          if t.nodes[e["src"]]["layer"] == "export"]
    return any(t.nodes[e["src"]]["kind"] not in spec.NON_VALUE_ASSERTION_KINDS
               for c in cols for e in t.into("CHECKS", c))


def lineage_guards(t: Tables, column: str | None = None):
    if column is None:
        cols = [c for c in t.by_label["DataColumn"] if c["dataset"] == spec.GOLD_TABLE]
        checked = {c["id"] for c in cols if _value_checked(t, c["id"])}
        data = q.unguarded_result([{"id": c["id"], "ref": c["ref"], "ordinal": c["ordinal"]} for c in cols], checked)
        return data, q.guards_caveats(data)
    start = t.ref_to_id[column]
    ids = [start]
    if t.nodes[start]["layer"] == "gold":
        ids += [e["src"] for e in t.into("DERIVED_FROM", start) if t.nodes[e["src"]]["layer"] == "export"]
    assertions, same = [], []
    for cid in ids:
        for e in t.into("CHECKS", cid):
            a = t.nodes[e["src"]]
            assertions.append({"id": a["id"], "contract": a["contract"], "kind": a["kind"],
                               "severity": a["severity"], "min_value": a["min"], "max_value": a["max"],
                               "source_line": a["source_line"], "source": a["source"],
                               "checked_column": t.nodes[cid]["ref"]})
    for aid in sorted({a["id"] for a in assertions}):
        for e, other in [(e, e["dst"]) for e in t.out("SAME_RULE_AS", aid)] + \
                        [(e, e["src"]) for e in t.into("SAME_RULE_AS", aid)]:
            b = t.nodes[other]
            same.append({"id": aid, "other": other, "contract": b["contract"], "severity": b["severity"],
                         "severity_differs": e["severity_differs"], "bounds_equal": e["bounds_equal"]})
    data = q.guards_result(column, assertions, same)
    layers = sorted({t.nodes[e["dst"]]["layer"] for e in t.edges["CHECKS"]
                     if t.label[e["src"]] == "Assertion" and t.label[e["dst"]] == "DataColumn"}) \
        if not assertions else None
    return data, q.guards_caveats(data, layers)


def lineage_unused(t: Tables, layer: str = "silver", domain: str = "churn"):
    cols = sorted((c for c in t.by_label["DataColumn"] if c["layer"] == layer and c["domain"] == domain),
                  key=lambda c: (c["dataset"], c["ordinal"]))
    columns = [{"id": c["id"], "ref": c["ref"], "dataset": c["dataset"], "ordinal": c["ordinal"],
                "readers": len(t.into("DERIVED_FROM", c["id"]))} for c in cols]
    graph = [{"ref": c["ref"], "element": e["src"]} for c in columns if not c["readers"]
             for e in t.into("SOURCED_FROM", c["id"])]
    data = q.unused_result(layer, domain, columns, graph)
    return data, q.unused_caveats(data)


ANSWERS = {"lineage_trace": lineage_trace, "lineage_pit": lineage_pit, "lineage_guards": lineage_guards,
           "lineage_unused": lineage_unused}


# --------------------------------------------------------------------------- golden sections
def _gold_paths(t: Tables, ref: str, max_hops: int = 4) -> list[list[str]]:
    """Every DERIVED_FROM path (as refs) from column ``ref`` down to a gold column, by DFS."""
    start = t.ref_to_id.get(ref)
    found: set[tuple[str, ...]] = set()
    if start is None:
        return []
    stack = [(start, (ref,))]
    while stack:
        node, path = stack.pop()
        if len(path) > max_hops:
            continue
        for e in t.into("DERIVED_FROM", node):
            nxt = t.nodes[e["src"]]
            step = path + (nxt["ref"],)
            if nxt["layer"] == "gold":
                found.add(step)
            stack.append((e["src"], step))
    return [list(p) for p in sorted(found)]


def _feature_reads(t: Tables) -> list[dict]:
    """One row per DERIVED_FROM / COUNTS_ROWS_OF edge out of a gold feature (input of pit_derivation_section)."""
    rows = []
    for c in t.by_label["DataColumn"]:
        if c["dataset"] != spec.GOLD_TABLE or c["role"] != "feature":
            continue
        for e in t.out("DERIVED_FROM", c["id"]):
            rows.append({"feature": c["name"], "ordinal": c["ordinal"], "window": e["window"],
                         "matched_window": e["matched_window"], "table": t.nodes[e["dst"]]["dataset"]})
        for e in t.out("COUNTS_ROWS_OF", c["id"]):
            rows.append({"feature": c["name"], "ordinal": c["ordinal"], "window": e["window"], "matched_window": None,
                         "table": t.nodes[e["dst"]]["name"]})
    return rows


def _range_guarantee(t: Tables, c: dict) -> str:
    """The range guarantee of a gold column recomputed from its stored SQL clamp, its contract
    range and its VALUE edge (COUNT(DISTINCT <date>) inside one bounded window of L days)."""
    lo, hi = c["contract_min"], c["contract_max"]
    if c["sql_clamp_min"] is not None and c["sql_clamp_max"] is not None \
            and c["sql_clamp_min"] >= lo and c["sql_clamp_max"] <= hi:
        return "sql_clamp"
    values = [e for e in t.out("DERIVED_FROM", c["id"]) if "VALUE" in (e["roles"] or "").split(",")]
    if len(values) == 1 and (c["expr_sql"] or "").upper().startswith("CAST(") \
            and t.nodes[values[0]["dst"]]["name"].endswith("_date") \
            and "COUNT(DISTINCT" in (values[0]["transform"] or "").upper():
        window = t.nodes.get("win:" + (values[0]["window"] or "").replace(" ", ""))
        if window is not None and window["length_days"] is not None and lo <= 0 and window["length_days"] <= hi:
            return f"window_length({window['length_days']}d)"
    return "data_dependent"


def derivation_mismatches(t: Tables) -> list[str]:
    """Derived properties the assembler stored, recomputed here from other rows of the graph:
    a feature's pit_status from its edge windows, a range guarantee from the clamp and the
    window length, a parameter's consistency from its three values. Empty when all agree."""
    out = []
    stored = {f["feature"]: f for f in _feature_rows(t)}
    derived = {r["feature"]: r for r in q.pit_derivation_section(_feature_reads(t))}
    for name, f in stored.items():
        d = derived.get(name)
        if d is None:
            out.append(f"{name}: the feature has no lineage edge to recompute pit_status from")
            continue
        for key in ("pit_status", "max_upper_vs_as_of_days", "reads_after_as_of"):
            if f[key] != d[key]:
                out.append(f"{name}: stored {key} {f[key]!r}, recomputed from the edge windows {d[key]!r}")
        for e in t.out("SUBJECT_TO", f["id"]):
            if e["status"] != d["pit_status"]:
                out.append(f"{name}: SUBJECT_TO status {e['status']!r}, recomputed {d['pit_status']!r}")
    for c in t.by_label["DataColumn"]:
        if c["dataset"] == spec.GOLD_TABLE and c["range_guarantee"] is not None:
            want = _range_guarantee(t, c)
            if c["range_guarantee"] != want:
                out.append(f"{c['name']}: stored range_guarantee {c['range_guarantee']!r}, recomputed {want!r}")
    for p in t.by_label["Parameter"]:
        if p["consistent"] is None:
            continue
        values = [p["sql_value"], p["pandas_value"], p["generator_value"]]
        want = None not in values and len(set(values)) == 1
        if bool(p["consistent"]) != want:
            out.append(f"parameter {p['name']}: stored consistent {p['consistent']!r}, its values are {values}")
    return out


def sections(t: Tables) -> dict:
    """The golden sections that are not tool answers (the Python twin of queries.contract_sections)."""
    gold = sorted((c for c in t.by_label["DataColumn"] if c["dataset"] == spec.GOLD_TABLE),
                  key=lambda c: c["ordinal"])
    gold_ids = {c["id"] for c in gold}
    derived = [e for e in t.edges["DERIVED_FROM"] if e["src"] in gold_ids]
    rowcounts = [e for e in t.edges["COUNTS_ROWS_OF"] if e["src"] in gold_ids]
    usage = Counter(t.nodes[e["dst"]]["display"] for e in t.edges["USES_WINDOW"] if e["src"] in gold_ids)
    leaky = [(k, a) for k in t.by_label["Contract"] for e in t.out("HAS_ASSERTION", k["id"])
             if (a := t.nodes[e["dst"]])["kind"] == "no_leak"]
    inv = spec.INVARIANT_9
    elements = sorted(t.by_label["GraphElement"], key=lambda g: g["id"])
    return {
        "gold_columns": q.gold_columns_section([
            {"name": c["name"], "ordinal": c["ordinal"], "role": c["role"], "pit_status": c["pit_status"],
             "derived_from": len(t.out("DERIVED_FROM", c["id"])),
             "counts_rows_of": len(t.out("COUNTS_ROWS_OF", c["id"]))}
            for c in gold]),
        "counts_rows_of": sorted(({"column": t.nodes[e["src"]]["ref"], "dataset": t.nodes[e["dst"]]["ref"],
                                   "window": e["window"]} for e in t.edges["COUNTS_ROWS_OF"]),
                                 key=lambda r: r["column"]),
        "range_guarantees": q.range_section([
            {"name": c["name"], "contract_min": c["contract_min"], "contract_max": c["contract_max"],
             "range_guarantee": c["range_guarantee"]} for c in gold if c["range_guarantee"] is not None]),
        "leaky": {"declared": sorted({x for _k, a in leaky for x in q.split_names(a["accepted_values"])}),
                  "dangling": sorted({x for k, _a in leaky for x in q.split_names(k["dangling_refs"])})},
        "parameters": [{k: p[k] for k in ("name", "sql_value", "pandas_value", "generator_value", "consistent",
                                          "unused")} for p in sorted(t.by_label["Parameter"], key=lambda p: p["name"])],
        "invariant_9": {"silver_column": inv["silver"], "silver_gold_descendants": _gold_paths(t, inv["silver"]),
                        "bronze_column": inv["bronze"], "bronze_paths_to_gold": _gold_paths(t, inv["bronze"])},
        "pit_derivation": q.pit_derivation_section(_feature_reads(t)),
        "subgraph": {"gold_derived_from_edges": len(derived), "gold_counts_rows_of_edges": len(rowcounts),
                     "distinct_silver_columns_read": len({e["dst"] for e in derived}),
                     "window_usage": dict(sorted(usage.items()))},
        "bridge": {"elements": len(elements), "nodes": sum(g["kind"] == "node" for g in elements),
                   "edges": sum(g["kind"] == "edge" for g in elements),
                   "by_element": [{"element": g["id"], "kind": g["kind"],
                                   "datasets": sum(t.label[e["dst"]] == "Dataset"
                                                   for e in t.out("SOURCED_FROM", g["id"])),
                                   "source_columns": sum(t.label[e["dst"]] == "DataColumn"
                                                         for e in t.out("SOURCED_FROM", g["id"])),
                                   "tables": len(t.out("MATERIALIZED_AS", g["id"]))} for g in elements]},
    }


def questions(answer, keys=spec.QUESTIONS + spec.EXTRA_QUESTIONS) -> dict:
    """{key: {tool, args, data, caveats}} with ``answer(tool, **args) -> (data, caveats)``."""
    out = {}
    for key, tool, args in keys:
        data, caveats = answer(tool, **args)
        out[key] = {"tool": tool, "args": args, "data": data, "caveats": caveats}
    return out


def pit_all(answer, features) -> list[dict]:
    """The lineage_pit row of every gold feature (22), in feature order."""
    return [answer("lineage_pit", feature=f)[0]["features"][0] for f in features]


def compute(t: Tables) -> dict:
    """The golden value document of a lineage build, from the Parquet tables alone."""
    def answer(tool, **args):
        return ANSWERS[tool](t, **args)
    features = [f["feature"] for f in _feature_rows(t)]
    return {**sections(t), "pit": pit_all(answer, features), "questions": questions(answer)}


# --------------------------------------------------------------------------- Tier 1 / Tier 2
def tier_rows(t: Tables) -> dict[str, list[dict]]:
    """The rows of queries.TIER_TEMPLATES, from the Parquet tables (the Python half of the comparison)."""
    n = t.nodes
    runs = []
    for r in t.by_label["Run"]:
        jobs = [e["dst"] for e in t.out("RAN_AS", r["id"])] or [None]
        runs += [{"run": r["id"], "kind": r["kind"], "job": j} for j in jobs]
    refs = []
    for r in t.by_label["Ref"]:
        targets = [e["dst"] for e in t.out("POINTS_TO", r["id"])] or [None]
        refs += [{"ref": r["id"], "name": r["name"], "kind": r["kind"], "table_name": r["table_name"], "snapshot": s}
                 for s in targets]
    return {
        "snapshots": [{"dataset": n[e["src"]]["name"], "snapshot": e["dst"], "snapshot_id": n[e["dst"]]["snapshot_id"],
                       "is_current": n[e["dst"]]["is_current"], "in_catalog": n[e["dst"]]["in_catalog"]}
                      for e in t.edges["HAS_SNAPSHOT"]],
        "supersedes": [{"newer": e["src"], "older": e["dst"], "parent_matches": e["parent_matches"]}
                       for e in t.edges["SUPERSEDES"]],
        "refs": refs, "runs": runs,
        "parents": [{"child": e["src"], "parent": e["dst"], "kind": e["kind"]} for e in t.edges["PARENT"]],
        "produced": [{"snapshot": e["src"], "run": e["dst"]} for e in t.edges["PRODUCED_BY_RUN"]],
        "consumed": [{"run": e["src"], "snapshot": e["dst"], "role": e["role"]} for e in t.edges["CONSUMED_SNAPSHOT"]],
    }


def tier_invariants(t: Tables) -> list[str]:
    """Structural rules of the Tier-1 / Tier-2 facts; each problem as one readable line (empty: all hold).

    Every snapshot belongs to exactly one dataset of its table; a ref points at one snapshot of its
    own table with its snapshot id; SUPERSEDES chains each table's snapshots (one predecessor and one
    successor at most, both of the same table, n - 1 edges for n catalog snapshots, sequence numbers
    increasing when ordered by them); a snapshot with a spark.app.id is PRODUCED_BY_RUN exactly the run
    of that id; a run RAN_AS at most one job; PARENT has no cycle; only a graph build CONSUMED_SNAPSHOT."""
    n, out = t.nodes, []
    for s in t.by_label["Snapshot"]:
        owners = [e["src"] for e in t.into("HAS_SNAPSHOT", s["id"])]
        if len(owners) != 1 or n[owners[0]]["name"] != s["table_name"]:
            out.append(f"{s['id']}: HAS_SNAPSHOT from {owners}, expected exactly ds:{s['table_name']}")
        produced = [e["dst"] for e in t.out("PRODUCED_BY_RUN", s["id"])]
        if s["spark_app_id"] and (len(produced) != 1 or n[produced[0]]["spark_app_id"] != s["spark_app_id"]):
            out.append(f"{s['id']}: spark.app.id {s['spark_app_id']} but PRODUCED_BY_RUN {produced}")
        nxt, prev = t.out("SUPERSEDES", s["id"]), t.into("SUPERSEDES", s["id"])
        if len(nxt) > 1 or len(prev) > 1:
            out.append(f"{s['id']}: {len(nxt)} predecessor(s) and {len(prev)} successor(s) by SUPERSEDES (at most 1)")
        for e in nxt:
            older = n[e["dst"]]
            if older["table_name"] != s["table_name"]:
                out.append(f"{s['id']} SUPERSEDES a snapshot of another table ({e['dst']})")
            elif e["ordered_by"] == "sequence_number" and \
                    not (s["sequence_number"] or 0) > (older["sequence_number"] or 0):
                out.append(f"{s['id']} SUPERSEDES {e['dst']} without a higher sequence number")
    per_table: dict[str, list[int]] = {}
    for s in t.by_label["Snapshot"]:
        counts = per_table.setdefault(s["table_name"], [0, 0])
        counts[0] += bool(s["in_catalog"])
        counts[1] += len(t.out("SUPERSEDES", s["id"]))
    for table, (snaps, edges) in sorted(per_table.items()):
        if snaps and edges != snaps - 1:
            out.append(f"{table}: {edges} SUPERSEDES edges for {snaps} catalog snapshots (a chain has {snaps - 1})")
    for r in t.by_label["Ref"]:
        targets = [n[e["dst"]] for e in t.out("POINTS_TO", r["id"])]
        if len(targets) > 1 or any(x["table_name"] != r["table_name"] or x["snapshot_id"] != r["snapshot_id"]
                                   for x in targets):
            out.append(f"{r['id']}: POINTS_TO {[x['id'] for x in targets]}, expected snapshot {r['snapshot_id']} of "
                       f"{r['table_name']}")
    for r in t.by_label["Run"]:
        jobs = [e["dst"] for e in t.out("RAN_AS", r["id"])]
        if len(jobs) > 1:
            out.append(f"{r['id']}: RAN_AS {len(jobs)} jobs {jobs}")
        if t.out("CONSUMED_SNAPSHOT", r["id"]) and r["kind"] != "graph_build":
            out.append(f"{r['id']}: a {r['kind']} run has CONSUMED_SNAPSHOT edges (only a graph build reads pins)")
        seen, cur = {r["id"]}, r["id"]
        while True:
            ups = [e["dst"] for e in t.out("PARENT", cur)]
            if not ups:
                break
            if ups[0] in seen:
                out.append(f"{r['id']}: PARENT edges form a cycle through {ups[0]}")
                break
            seen.add(ups[0])
            cur = ups[0]
    return out


# --------------------------------------------------------------------------- goldens
def semantic_inputs(man: dict) -> dict[str, str]:
    """sha256 of the files that decide the semantic answers (a subset of the build's inputs)."""
    inputs = man.get("inputs_sha256", {})
    return {rel: inputs[rel] for rel in spec.SEMANTIC_FILES if rel in inputs}


def golden(build_dir: str | Path) -> dict:
    """The golden document to commit for this build (values + what they were measured on)."""
    bdir = Path(build_dir)
    man = lbuild.read_manifest(bdir)
    t = load_tables(bdir)
    return {"golden_version": GOLDEN_VERSION, "spec": spec.SPEC_VERSION, "profile": man["profile"],
            "inputs_sha256": semantic_inputs(man),
            "measured_on": {"commit": man.get("commit"), "dirty": man.get("dirty"), "versions": man.get("versions"),
                            "lineage_build_id": man["lineage_build_id"]},
            "reported_not_gated": {"counts": t.counts(),
                                   "note": "whole-graph totals grow with every Make target, CI step and script; "
                                           "they are reported by the contract, never compared"},
            "values": compute(t)}


def golden_path(name: str = GOLDEN_PROFILE) -> Path:
    return Path(__file__).resolve().parent / spec.GOLDEN_DIR / f"{name}.json"


def find_golden(man: dict) -> tuple[dict | None, str, bool]:
    """(golden, note, exact): the committed core golden and whether its semantic inputs are
    byte-identical to this build's (then any difference is an error, not a model change)."""
    path = golden_path()
    if not path.is_file():
        return None, f"no golden at {mf.display_path(path)} (generate it with --print-golden)", False
    doc = json.loads(path.read_text())
    exact = doc.get("inputs_sha256") == semantic_inputs(man)
    return doc, f"golden {path.name} (commit {doc.get('measured_on', {}).get('commit')})", exact


def diff(want, got, path: str = "") -> list[str]:
    """Human-readable differences between two JSON-like values (empty when equal)."""
    if isinstance(want, dict) and isinstance(got, dict):
        out = []
        for k in sorted(set(want) | set(got)):
            if k not in got:
                out.append(f"{path}/{k}: missing")
            elif k not in want:
                out.append(f"{path}/{k}: unexpected")
            else:
                out += diff(want[k], got[k], f"{path}/{k}")
        return out
    if isinstance(want, list) and isinstance(got, list):
        if len(want) != len(got):
            return [f"{path}: {len(got)} items, expected {len(want)}"]
        return [d for i, (a, b) in enumerate(zip(want, got, strict=True)) for d in diff(a, b, f"{path}[{i}]")]
    if isinstance(want, float) or isinstance(got, float):
        same = isinstance(want, (int, float)) and isinstance(got, (int, float)) and float(want) == float(got)
    else:
        same = want == got
    return [] if same else [f"{path}: {json.dumps(got, default=str)[:80]} != expected "
                            f"{json.dumps(want, default=str)[:80]}"]


def dumps(doc: dict) -> str:
    return json.dumps(doc, indent=1, sort_keys=True, default=str) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Lineage oracle: golden answers from the lineage Parquet tables.")
    ap.add_argument("--build", required=True, help="build directory holding lineage/")
    ap.add_argument("--print-golden", action="store_true", help="print the golden document (commit it as "
                                                                 "lineage/goldens/core.json)")
    a = ap.parse_args(argv)
    if a.print_golden:
        sys.stdout.write(dumps(golden(a.build)))
    else:
        sys.stdout.write(dumps(compute(load_tables(a.build))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
