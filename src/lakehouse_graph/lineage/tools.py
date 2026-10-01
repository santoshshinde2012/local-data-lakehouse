"""The four lineage tools (toolset ``lakehouse-lineage``): pure functions over lineage.lbdb.

  lineage_trace(ctx, target, direction="upstream", max_depth=6)
  lineage_pit(ctx, feature=None)
  lineage_guards(ctx, column=None)
  lineage_unused(ctx, layer="silver", domain="churn")

Each returns ``(data, caveats)``: ``data`` is a plain dict with deterministic, ordered
content; ``caveats`` is a list of sentences the agent must pass on. Invalid input raises
``ValueError`` with a message that lists close matches. ``ctx`` needs two attributes:
``lineage_conn`` (a read-only Ladybug connection to <build_dir>/lineage.lbdb, or None when
the build has no lineage) and ``build_dir``. ``LineageContext`` is the minimal one the
contract script and the tests use.

Junk arguments: an optional argument sent as None, "", "null" or "None" (small models do
this for an argument they mean to leave out) takes its documented default: no feature / no
column for lineage_pit / lineage_guards, "upstream" / 6 for lineage_trace, "silver" /
"churn" for lineage_unused. Anything else that is not in a closed list is an error.

Safety: no free text reaches a query. A ``ColumnRef`` is validated by ``spec.is_column_ref``
(a full match of ``COLUMN_REF_RE``) and then looked up with a ``$ref`` parameter; features,
layers, domains and directions are closed lists; every query is a named template from
``queries.py`` (ORDER BY + LIMIT).

Size: a trace can be large (a few answers exceed 200 edge rows). ``lineage_trace`` puts
``summary`` (with ``reached_counts``) and ``reached`` before ``edges`` so that an envelope
which truncates cuts the edge rows and keeps the answer; it adds a caveat above 200 rows.
"""
from __future__ import annotations

import difflib
from pathlib import Path

from .. import spec as gspec
from .. import store
from . import queries as q
from . import spec

FEATURES_RULE = "pit:features_le_as_of"
UNUSED_LAYERS = ("bronze", "silver")
_JUNK = ("", "null", "none")


class LineageUnavailable(RuntimeError):
    """The build has no lineage.lbdb (run scripts/build_lineage_local.py / make lineage-local)."""


class LineageContext:
    """Minimal tool context: a build directory and a lazily opened read-only lineage connection."""

    def __init__(self, build_dir: str | Path, buffer_pool_mb: int = store.SERVE_BUFFER_POOL_MB):
        self.build_dir = Path(build_dir)
        self.buffer_pool_mb = buffer_pool_mb
        self._db = None
        self._conn = None

    @property
    def lineage_conn(self):
        if self._conn is None:
            path = self.build_dir / spec.DB_FILE
            if not path.exists():
                return None
            self._db, self._conn = store.open_readonly(path, buffer_pool_mb=self.buffer_pool_mb)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._db.close()
            self._db = self._conn = None

    def __enter__(self) -> LineageContext:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# --------------------------------------------------------------------------- validation
def _conn(ctx):
    conn = getattr(ctx, "lineage_conn", None)
    if conn is None:
        raise LineageUnavailable(f"no {spec.DB_FILE} in {getattr(ctx, 'build_dir', 'this build')}: run "
                                 f"make lineage-local (scripts/build_lineage_local.py)")
    return conn


def _absent(value):
    """Small models send "", "null" or "None" for an optional argument they mean to leave out."""
    return None if isinstance(value, str) and value.strip().lower() in _JUNK else value


def _or_default(value, default):
    """``value``, or ``default`` when it is absent (None or a junk string, see ``_absent``)."""
    return default if _absent(value) is None else value


def _choice(name: str, value, allowed: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in allowed:
        # close matches only for a string: str(None) = "None" is "close" to whatever starts with "n" / "o"
        near = difflib.get_close_matches(value, allowed, n=3, cutoff=0.5) if isinstance(value, str) else []
        hint = f" Did you mean {', '.join(near)}?" if near else ""
        raise ValueError(f"invalid {name} {value!r}: expected one of {', '.join(allowed)}.{hint}")
    return value


def resolve_column(conn, ref) -> dict:
    """The DataColumn a ColumnRef names; ValueError (with close matches) when it is malformed or unknown."""
    if not spec.is_column_ref(ref):
        raise ValueError(f"invalid column reference {ref!r}: expected <layer>.<table>.<column> with layer one of "
                         f"{', '.join(spec.LAYERS)} (for example gold.churn_renewal_features.limit_hits_14d)")
    rows = q.fetch(conn, "column_by_ref", {"ref": ref})
    if rows:
        return rows[0]
    known = [r["ref"] for r in q.fetch(conn, "column_refs")]
    table = ref.rsplit(".", 1)[0]
    near = difflib.get_close_matches(ref, known, n=5, cutoff=0.6) or [k for k in known if k.startswith(table + ".")][:8]
    hint = f" Close matches: {', '.join(near)}." if near else \
        f" Known tables: {', '.join(sorted({k.rsplit('.', 1)[0] for k in known}))}."
    raise ValueError(f"unknown column {ref!r}: no such column in this lineage build.{hint}")


# --------------------------------------------------------------------------- lineage_trace
def lineage_trace(ctx, target, direction: str = "upstream", max_depth: int = spec.MAX_TRACE_DEPTH):
    """Column lineage from ``target``: ``upstream`` to the columns (and row counts) it is derived
    from, ``downstream`` to everything that reads it (columns, checks and their contracts, graph
    elements, metrics, the consumer repo). Edges are ordered by depth, then relation and ids."""
    conn = _conn(ctx)
    direction = _choice("direction", _or_default(direction, "upstream"), spec.TRACE_DIRECTIONS)
    max_depth = _or_default(max_depth, spec.MAX_TRACE_DEPTH)
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or not 1 <= max_depth <= spec.MAX_TRACE_DEPTH:
        raise ValueError(f"invalid max_depth {max_depth!r}: expected an integer from 1 to {spec.MAX_TRACE_DEPTH}")
    col = resolve_column(conn, target)
    upstream = direction == "upstream"
    depth_of = {col["id"]: 0}
    name_of = {col["id"]: col["ref"]}
    layer_of = {col["id"]: col["layer"]}
    info: dict[str, tuple] = {}
    rows: list[dict] = []
    # frontier: columns that have something beyond them in the walk direction (their stored degree);
    # expanded: every column whose parameters / consumers are attached (depth <= max_depth - 1)
    frontier = [col["id"]] if col["n_sources" if upstream else "n_readers"] else []
    expanded = [col["id"]]
    template = "trace_upstream_step" if upstream else "trace_downstream_step"
    for depth in range(1, max_depth + 1):
        if not frontier:
            break
        reached, more = [], []
        for r in q.fetch(conn, template, {"ids": frontier}):
            name = r["to_ref"] or r["to_id"]
            label = r.get("to_label", "DataColumn")
            info[name] = (label, r["to_layer"])
            rows.append(q.trace_row(depth, r.get("rel", "DERIVED_FROM"), name_of[r["from_id"]], name, r["roles"],
                                    r["cte"], r["window"], r["transform"], r["matched_window"]))
            if label == "DataColumn" and r["to_id"] not in depth_of:
                depth_of[r["to_id"]], name_of[r["to_id"]], layer_of[r["to_id"]] = depth, name, r["to_layer"]
                reached.append(r["to_id"])
                if r["to_more"]:
                    more.append(r["to_id"])
        if depth < max_depth:
            expanded += sorted(reached)
        frontier = sorted(more)
    stopped = bool(frontier)
    if upstream:
        for r in q.fetch(conn, "trace_parameters", {"ids": expanded}):
            info[r["to_id"]] = ("Parameter", None)
            rows.append(q.trace_row(depth_of[r["from_id"]] + 1, "USED_BY", name_of[r["from_id"]], r["to_id"],
                                    transform=f"value {r['sql_value']:g}; consistent={bool(r['consistent'])}"))
    else:
        assertion_depth: dict[str, int] = {}
        for r in q.fetch(conn, "trace_consumers", {"ids": expanded}):
            depth = depth_of[r["from_id"]] + 1
            info[r["to_id"]] = (r["to_label"], None)
            is_assertion = r["to_label"] == "Assertion"
            rows.append(q.trace_row(depth, r["rel"], name_of[r["from_id"]], r["to_id"],
                                    transform=q.assertion_text(r["kind"], r["severity"]) if is_assertion else None))
            if is_assertion:
                assertion_depth[r["to_id"]] = min(depth, assertion_depth.get(r["to_id"], depth))
        for r in q.fetch(conn, "trace_contracts", {"ids": sorted(assertion_depth)}) if assertion_depth else []:
            depth = assertion_depth[r["from_id"]] + 1
            if depth <= max_depth:
                info[r["to_id"]] = ("Contract", None)
                rows.append(q.trace_row(depth, "HAS_ASSERTION", r["from_id"], r["to_id"]))
        exported = [c for c in expanded if layer_of[c] == "export"]   # only exports have file-level consumers
        for r in q.fetch(conn, "trace_export_consumers", {"ids": exported}) if exported else []:
            info[r["to_id"]] = (r["to_label"], None)
            rows.append(q.trace_row(depth_of[r["from_id"]] + 1, r["rel"], name_of[r["from_id"]], r["to_id"],
                                    transform=f"via {r['export_ref']}"))
    data = q.trace_result(col["ref"], direction, max_depth, rows, info, stopped)
    return data, q.trace_caveats(data)


# --------------------------------------------------------------------------- lineage_pit
def lineage_pit(ctx, feature=None):
    """Point-in-time status of one gold feature (its windows relative to as_of and any read with
    no time bound of its own), or, with no feature, every feature that can read data after as_of."""
    conn = _conn(ctx)
    feature = _absent(feature)
    if feature is not None and feature not in gspec.GOLD_FEATURES:
        near = difflib.get_close_matches(feature, gspec.GOLD_FEATURES, n=3, cutoff=0.5) \
            if isinstance(feature, str) else []
        hint = f" Did you mean {', '.join(near)}?" if near else ""
        raise ValueError(f"unknown feature {feature!r}: expected one of the {len(gspec.GOLD_FEATURES)} gold "
                         f"features ({', '.join(gspec.GOLD_FEATURES)}).{hint}")
    features = q.fetch(conn, "pit_features", {"dataset": spec.GOLD_TABLE})
    if feature is not None and feature not in {f["feature"] for f in features}:
        raise ValueError(f"feature {feature!r} is not in this lineage build (a stale build: run make lineage-local)")
    ids = [f["id"] for f in features if (f["feature"] == feature if feature else
                                         f["pit_status"] != spec.PIT_COMPLIANT)]
    windows = q.fetch(conn, "pit_windows", {"ids": ids})
    unbounded = q.fetch(conn, "pit_unbounded_reads", {"ids": ids}) + \
        q.fetch(conn, "pit_unbounded_rowcounts", {"ids": ids})
    rule = (q.fetch(conn, "pit_rule", {"id": FEATURES_RULE}) or [None])[0]
    data = q.pit_result(rule, features, windows, feature, unbounded)
    return data, q.pit_caveats(data)


# --------------------------------------------------------------------------- lineage_guards
def lineage_guards(ctx, column=None):
    """The assertions that check a column (for a gold column also the ones on the export
    columns copied from it), with contract, kind, severity, bounds and source line; with no
    column, the gold columns whose values no executed check looks at."""
    conn = _conn(ctx)
    column = _absent(column)
    if column is None:
        columns = q.fetch(conn, "guards_dataset_columns", {"dataset": spec.GOLD_TABLE})
        p = {"dataset": spec.GOLD_TABLE, "non_value": list(spec.NON_VALUE_ASSERTION_KINDS)}
        checked = {r["id"] for r in q.fetch(conn, "guards_value_checked_direct", p)} | \
                  {r["id"] for r in q.fetch(conn, "guards_value_checked_via_export", p)}
        data = q.unguarded_result(columns, checked)
        return data, q.guards_caveats(data)
    col = resolve_column(conn, column)
    ids = [col["id"]]
    if col["layer"] == "gold":
        ids += [r["id"] for r in q.fetch(conn, "guards_export_columns", {"id": col["id"]})]
    assertions = q.fetch(conn, "guards_assertions", {"ids": ids})
    same = q.fetch(conn, "guards_same_rule", {"ids": sorted({a["id"] for a in assertions})})
    data = q.guards_result(col["ref"], assertions, same)
    layers = [r["layer"] for r in q.fetch(conn, "guards_checked_layers")] if not assertions else None
    return data, q.guards_caveats(data, layers)


# --------------------------------------------------------------------------- lineage_unused
def lineage_unused(ctx, layer: str = "silver", domain: str = "churn"):
    """Columns of a layer that the next layer never reads (as a value or in a predicate)."""
    conn = _conn(ctx)
    layer = _choice("layer", _or_default(layer, "silver"), UNUSED_LAYERS)
    domain = _choice("domain", _or_default(domain, "churn"), spec.DOMAINS)
    columns = q.fetch(conn, "unused_columns", {"layer": layer, "domain": domain})
    unused_ids = [c["id"] for c in columns if not c["readers"]]
    graph = q.fetch(conn, "unused_graph_readers", {"ids": unused_ids})
    data = q.unused_result(layer, domain, columns, graph)
    return data, q.unused_caveats(data)


TOOLS = {"lineage_trace": lineage_trace, "lineage_pit": lineage_pit, "lineage_guards": lineage_guards,
         "lineage_unused": lineage_unused}
