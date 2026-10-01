"""Column lineage and point-in-time windows of the churn gold SQL (sqlglot qualify + scope walk).

``sqlglot.lineage.lineage()`` gives projection lineage only: it returns a sourceless node
for ``COUNT(*)`` and says nothing about the predicates that decide *which rows* a feature
reads. The scope walk in this module covers both, over the same qualified tree:

  role          how a gold column reads a silver column
                VALUE | FILTER | JOIN_KEY | WINDOW_BOUND | ANCHOR | PREDICATE | TEMPORAL_JOIN
                (ROWCOUNT marks a COUNT(*) over the table; the assembler turns it into a
                COUNTS_ROWS_OF edge)
  cte           the CTE whose FROM / JOIN reads the base table (the aggregation level)
  window        the date window the read is restricted to, in days relative to as_of:
                (lower, lower_inclusive, upper, upper_inclusive); None = no time bound
  matched       for a read with no time bound of its own: the row window of the event
                column its rows are matched to (``EXISTS(i.windows, w -> u.activity_date
                BETWEEN w.starts_on AND w.ends_on)``); None when it is not matched that way
  post_cmp      comparisons with an anchor that are applied after aggregation
                (``paid_at = renewal_date``); kept apart from the row-level window

The point-in-time rule itself is read from the SQL: the ``renewals`` CTE selects the rows
with ``snapshot_date = date_sub(current_period_end, N)``, so ``as_of`` is 0 and
``renewal_date`` is ``+N`` days. ``T-7`` is not hard-coded here.

What counts as a time bound (everything else leaves the read unbounded, which the
assembler reports as a point-in-time exception; the walk never guesses a bound):

  * a top-level AND conjunct of a WHERE clause, or of the ON clause of a join that does
    not preserve the table's rows (INNER / CROSS; the joined table of a LEFT join; the
    other tables of a RIGHT join; never FULL), of the form ``<column> <op> <anchor +- N days>``;
    conjuncts of one clause intersect. Nothing under OR or NOT is a bound;
  * a top-level AND conjunct of the single WHEN of ``<aggregate>(CASE WHEN ... THEN x
    [ELSE <neutral>] END)`` where the aggregate is SUM / MAX / MIN / COUNT / AVG and rows
    that fail the condition contribute nothing (no ELSE, ELSE NULL, or ELSE 0 under SUM,
    under MAX of a literal >= 0 and under MIN of a literal <= 0). Such a bound covers only
    the columns inside that WHEN / THEN; a column anywhere else in the expression keeps
    the scope's own window, and two CASEs in one expression are two separate reads (the
    assembler takes the loosest). With ELSE 0 the aggregate is 0 when rows exist only
    outside the window and NULL when there is no row at all, so whoever reads that CTE
    column must read it as ``COALESCE(<column>, 0)``; any other use stops the extraction.

A table that a scope joins without naming any of its columns (``CROSS JOIN t`` with
``SUM(1)``) is still read: it yields a ROWCOUNT leaf with the scope's window for it.
A CTE that is inner-joined only to filter rows contributes its own WHERE / ON reads.

Bounds through a plain row filter. A CTE or derived table of the shape ``SELECT <row
expressions> FROM <one base table> [WHERE ...]`` (``plain_filter``) yields one row per
table row it keeps. A WHERE / ON bound that the reading scope puts on a column such a
filter passes through unchanged (``l.hit_date <= r.as_of`` with ``l`` = ``(SELECT
subscription_id, hit_date FROM silver.churn_limit_events)``) therefore bounds the table's
rows exactly as it would on the table itself: it is pushed down one level
(``pushed_bounds``), and a ``COUNT(*)`` over the filter is a ROWCOUNT of its table.

The renewals CTE is the as-of snapshot row and its reads are exempt from the feature
windows, so it must be exactly such a filter: SELECT / FROM / WHERE over one table, with
no window function, aggregate or table-generating function. ``MAX(snapshot_date) OVER ()``
there would read other renewals' rows, later ones included, under that exemption.

Constructs the walk does not model stop the extraction instead of being skipped: a set
operation (UNION), LATERAL VIEW / LATERAL / PIVOT / TABLESAMPLE, time travel (VERSION /
TIMESTAMP AS OF), a WITH nested inside a CTE or subquery (it could shadow ``renewals``), a
subquery inside a WHERE / ON / HAVING condition, a multi-row CTE joined without naming a
column of it, and a renewals CTE that is more than a row filter over one table (above).

Renewal grain. A window is relative to the as_of of *one* renewal row, so a bound only
means something while every scope stays at that grain. The walk therefore also stops when
a scope built on the renewals CTE joins it to itself, groups by anything that does not
include ``<alias>.subscription_id`` (``spec.RENEWAL_KEY``) or adds subtotals (ROLLUP /
CUBE / GROUPING SETS), aggregates with no GROUP BY, selects DISTINCT without that key,
uses a window function (in a projection, QUALIFY, HAVING or ORDER BY) not partitioned by
that key, cuts its rows with LIMIT / OFFSET, or joins two renewal-derived CTEs on anything
but that key: each of these would mix rows bounded by different as_of dates, or let other
renewals decide which renewals keep a row.

Known conservative cases: a safe read that is reported as unbounded (so the feature shows
as an undeclared exception). The remedy is the same for all of them: join the base table
directly in the feature CTE and write the bounds as top-level AND conditions of its ON /
WHERE, in whole days relative to r.as_of / r.renewal_date.

  * a bound on a column of a CTE / derived table that is not a plain row filter (it
    aggregates, joins, deduplicates, sorts, limits or reads another CTE: any clause beyond
    SELECT / FROM / WHERE), or on a column the filter computes (``date_add(hit_date, 1) AS
    hit_date``): nothing is pushed down;
  * a CASE bound on a column of a CTE / derived table (CASE bounds cover base-table
    columns only);
  * a bound under OR / NOT, in HAVING / QUALIFY, in months or another non-day unit
    (``add_months``, ``INTERVAL``), or against anything but the two anchors.

What the walk cannot see (stated, not checked): whether a silver column was itself filled
in after its row's event date (a late-arriving update), the values of the rows read (so
also whether ``spec.RENEWAL_KEY`` is unique among the renewals rows, which the grain rules
assume), and functions of the build's wall clock (``current_date()``), which read no table.

Everything is read-only and offline. An unresolved name raises ``LineageExtractError``
(the contract must fail loudly rather than lose a lineage edge).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope

from .spec import RENEWAL_KEY, LineageExtractError

INF = float("inf")
DIALECT = "spark"
Window = tuple[float, bool, float, bool]   # (lower, lower_inclusive, upper, upper_inclusive) vs as_of
Bound = tuple[str, float, bool]            # ("lo" | "hi" | "eq", days vs as_of, inclusive)
# Aggregates that skip NULL inputs: a CASE without ELSE under one of them drops the rows that fail its WHEN.
NULL_SKIPPING_AGGREGATES = (exp.Sum, exp.Max, exp.Min, exp.Count, exp.Avg)
# SELECT clauses that change which rows a scope reads in ways the walk does not model.
UNMODELLED_CLAUSES = ("laterals", "pivots", "sample", "connect", "match")
# The same on a table reference: sqlglot keeps TABLESAMPLE, PIVOT / UNPIVOT and time travel (VERSION /
# TIMESTAMP AS OF: another state of the table than the one the build reads) on the table, not on the SELECT.
TABLE_MODIFIERS = {"sample": "TABLESAMPLE", "pivots": "PIVOT / UNPIVOT", "version": "time travel (AS OF)",
                   "when": "time travel (AS OF)", "system_time": "time travel (AS OF)", "changes": "CHANGES",
                   "laterals": "LATERAL VIEW", "joins": "a nested join", "rows_from": "ROWS FROM"}
# The only clauses of a plain row filter, as sqlglot names them (``from`` is ``from_`` since sqlglot 28).
ROW_FILTER_ARGS = frozenset({"expressions", "from", "from_", "where"})
# Expressions that make a SELECT read, or return, more than the one row it is evaluated for.
NOT_ROW_LOCAL = ((exp.Window, "a window function"), (exp.AggFunc, "an aggregate"),
                 (exp.UDTF, "a table-generating function"), (exp.Query, "a subquery"))
CLAUSE_NAMES = {"joins": "JOIN", "group": "GROUP BY", "order": "ORDER BY", "sort": "SORT BY", "cluster": "CLUSTER BY",
                "distribute": "DISTRIBUTE BY", "windows": "WINDOW", "laterals": "LATERAL VIEW", "pivots": "PIVOT"}


@dataclass
class Leaf:
    """One read of a base-table column (or of its rows, column ``*``) by a gold column."""
    table: str                  # lakehouse.silver.<table>
    column: str                 # '*' for a row count
    role: str
    path: tuple[str, ...]       # CTE hops, outermost first
    cte: str | None             # scope whose FROM / JOIN reads the base table
    cte_expr: str | None        # projection SQL at that scope
    window: Window | None = None
    post_cmp: tuple[Bound, ...] = ()
    matched: Window | None = None   # unbounded read matched to event rows in this window (temporal join)


@dataclass
class Guard:
    """One accepted ``<aggregate>(CASE WHEN <bounds> THEN <value> [ELSE <neutral>] END)`` branch."""
    branch: exp.If
    bounds: dict[str, list[Bound]]   # table alias -> the bounds a row must satisfy to contribute
    events: set[int]                 # id() of the event columns those bounds compare
    zero_default: bool = False       # ELSE 0 (not NULL): 0 and NULL must be read as the same value


@dataclass
class ColumnLineage:
    name: str
    ordinal: int
    final_sql: str
    leaves: list[Leaf] = field(default_factory=list)
    sqlglot_leaves: list[tuple[str, str]] = field(default_factory=list)   # (table, column) per lineage()
    sqlglot_unresolved: list[str] = field(default_factory=list)           # sourceless nodes (COUNT(*))


def literal_number(e) -> float | None:
    """Numeric value of a literal expression (supports unary minus, product, parentheses)."""
    if isinstance(e, exp.Literal) and not e.is_string:
        return float(e.this)
    if isinstance(e, exp.Neg):
        v = literal_number(e.this)
        return None if v is None else -v
    if isinstance(e, exp.Mul):
        a, b = literal_number(e.this), literal_number(e.expression)
        return None if a is None or b is None else a * b
    if isinstance(e, exp.Paren):
        return literal_number(e.this)
    return None


def format_window(w: Window | None) -> str:
    """``(as_of-28, as_of]``; ``unbounded`` when there is no time filter at all."""
    if w is None:
        return "unbounded"
    lo, lo_incl, hi, hi_incl = w

    def term(v: float) -> str:
        if v in (INF, -INF):
            return "inf" if v > 0 else "-inf"
        return "as_of" if int(v) == 0 else f"as_of{int(v):+d}"
    return f"{'[' if lo_incl else '('}{term(lo)}, {term(hi)}{']' if hi_incl else ')'}"


def format_bounds(bounds: tuple[Bound, ...]) -> str | None:
    """``eq= as_of+7;hi= as_of`` for post-aggregation comparisons with an anchor."""
    if not bounds:
        return None
    return ";".join(f"{side}{'=' if incl else ''} as_of{int(v):+d}".replace("as_of+0", "as_of")
                    for side, v, incl in sorted(bounds))


def conjuncts(e) -> list:
    """The AND-ed parts of a condition. An OR or a NOT is one opaque part: nothing inside it is split out."""
    if e is None:
        return []
    if isinstance(e, exp.And):
        return conjuncts(e.this) + conjuncts(e.expression)
    if isinstance(e, exp.Paren):
        return conjuncts(e.this)
    return [e]


def intersect(bounds) -> Window | None:
    """The window a conjunction of bounds leaves (None when there is no bound). At equal
    values the exclusive bound wins."""
    bounds = list(bounds)
    if not bounds:
        return None
    lo, lo_incl, hi, hi_incl = -INF, False, INF, False
    for side, v, incl in bounds:
        if side in ("lo", "eq") and (v > lo or (v == lo and not incl)):
            lo, lo_incl = v, incl
        if side in ("hi", "eq") and (v < hi or (v == hi and not incl)):
            hi, hi_incl = v, incl
    return (lo, lo_incl, hi, hi_incl)


def neutral_default(aggregate, then, default) -> bool:
    """True when a row that fails the WHEN of ``aggregate(CASE WHEN c THEN then ELSE default END)``
    contributes nothing to the aggregate (so the WHEN bounds which rows are read)."""
    if default is None or isinstance(default, exp.Null):
        return True
    if literal_number(default) != 0:
        return False
    if isinstance(aggregate, exp.Sum):
        return True
    value = literal_number(then)
    if isinstance(aggregate, exp.Max):
        return value is not None and value >= 0
    if isinstance(aggregate, exp.Min):
        return value is not None and value <= 0
    return False   # COUNT counts the ELSE 0 rows, AVG divides by them


def window_bounds(w: Window | None) -> list[Bound]:
    """A window as the two bounds it stands for (the inverse of ``intersect``; none for None)."""
    return [] if w is None else [("lo", w[0], w[1]), ("hi", w[2], w[3])]


def beyond_a_row_filter(sel: exp.Select) -> list[str]:
    """What makes a SELECT more than ``SELECT <row expressions> FROM ... [WHERE ...]``: every other
    clause it has (JOIN, GROUP BY, HAVING, QUALIFY, DISTINCT, ORDER BY, LIMIT, ...) and every window
    function, aggregate, table-generating function and subquery in it. Empty for a plain row filter."""
    found = [CLAUSE_NAMES.get(k, k.rstrip("_").upper()) for k, v in sel.args.items()
             if v and k not in ROW_FILTER_ARGS]
    for cls, what in NOT_ROW_LOCAL:
        for node in sel.find_all(cls):
            if node is sel or (cls is exp.AggFunc and node.find_ancestor(exp.Window) is not None):
                continue   # the SELECT itself; an aggregate that is a window function's (already listed)
            found.append(f"{what} ({node.sql(DIALECT)[:60]})")
            break
    return found


def table_name(t: exp.Table) -> str:
    return ".".join(p for p in (t.catalog, t.db, t.name) if p)


class GoldLineage:
    """Qualified scope tree of the gold SQL plus the walk that resolves every output column.

    ``sql`` is the statement with placeholders substituted; ``schema`` is the sqlglot schema
    mapping ``{catalog: {db: {table: {column: type}}}}`` of the silver tables it reads.
    """

    def __init__(self, sql: str, schema: dict, source: str = "gold SQL"):
        self.sql, self.schema, self.source = sql, schema, source
        try:
            tree = sqlglot.parse_one(sql, read=DIALECT)
            self.qualified = qualify(tree, schema=schema, dialect=DIALECT, validate_qualify_columns=True)
        except SqlglotError as e:
            raise LineageExtractError(
                f"{source}: sqlglot {sqlglot.__version__} cannot parse / qualify the statement against the silver "
                f"schema derived from the bronze and silver jobs ({type(e).__name__}: {e}). A column the SQL reads "
                f"is not a column of its silver table, or the SQL uses a construct the extractor does not "
                f"understand") from e
        root = build_scope(self.qualified)
        if root is None or not isinstance(root.expression, exp.Select):
            raise LineageExtractError(f"{source}: expected one SELECT statement with CTEs")
        self.root: Scope = root
        self.cte_name: dict[int, str] = {id(cs): cs.expression.parent.alias for cs in root.cte_scopes}
        by_name = {name: cs for cs in root.cte_scopes if (name := self.cte_name[id(cs)])}
        if "renewals" not in by_name:
            raise LineageExtractError(f"{source}: the 'renewals' CTE (the T-N row selection) is missing")
        self.renewals: Scope = by_name["renewals"]
        self.cte_scopes = by_name
        sources = [src for _node, src in self.renewals.selected_sources.values()]
        if len(sources) != 1 or not isinstance(sources[0], exp.Table) or self.renewals.subquery_scopes \
                or self.renewals.expression.args.get("joins"):
            raise LineageExtractError(
                f"{source}: the renewals CTE must select from one table, with no join and no subquery (its rows "
                f"are the as-of snapshot rows and are exempt from the feature windows; read anything else in a "
                f"feature CTE)")
        extra = beyond_a_row_filter(self.renewals.expression)
        if extra:
            raise LineageExtractError(
                f"{source}: the renewals CTE must be a plain row filter over one table (SELECT <columns> FROM "
                f"<table> WHERE ...), but it has {', '.join(extra)}. Its reads are the as-of snapshot row and are "
                f"exempt from the feature windows, so anything that looks at other rows (a window function, an "
                f"aggregate, QUALIFY, DISTINCT, LIMIT, ...) would read other renewals' rows unchecked: compute it "
                f"in a feature CTE")
        self._derived: dict[int, bool] = {}
        self._plain: dict[int, str | None] = {}
        for scope in root.traverse():
            self._check_shape(scope)
        if RENEWAL_KEY not in self.renewals.expression.named_selects:
            raise LineageExtractError(f"{source}: the renewals CTE must project its key {RENEWAL_KEY} "
                                      f"(lineage/spec.py RENEWAL_KEY)")
        for scope in root.traverse():
            self._check_grain(scope)
        self.as_of_column, self.renewal_column, self.t_offset = self._pit_predicate()
        # anchors: aliases of the renewals CTE expressed as days relative to as_of
        self.anchor_offset: dict[str, float] = {}
        for p in self.renewals.expression.selects:
            inner = p.this if isinstance(p, exp.Alias) else p
            if isinstance(inner, exp.Column) and inner.name == self.as_of_column:
                self.anchor_offset[p.alias_or_name] = 0
            elif isinstance(inner, exp.Column) and inner.name == self.renewal_column:
                self.anchor_offset[p.alias_or_name] = self.t_offset
        if sorted(self.anchor_offset.values()) != [0, self.t_offset]:
            raise LineageExtractError(f"{source}: the renewals CTE must project both the as_of column "
                                      f"({self.as_of_column}) and the renewal column ({self.renewal_column})")

    def _check_shape(self, scope: Scope) -> None:
        """Stop on a scope the walk cannot read faithfully (never skip it)."""
        sel = scope.expression
        if not isinstance(sel, exp.Select):
            raise LineageExtractError(
                f"{self.source}: {type(sel).__name__.upper()} is not modelled by the lineage walk: every CTE, derived "
                f"table and subquery must be one plain SELECT (teach lineage/scope_walk.py before using it)")
        where = self.scope_label(scope)
        if scope is not self.root and (sel.args.get("with_") or sel.args.get("with")):
            names = ", ".join(c.alias for c in (sel.args.get("with_") or sel.args.get("with")).expressions)
            raise LineageExtractError(
                f"{self.source}: {where} has its own WITH ({names}), which the lineage walk does not model (a nested "
                f"CTE could also shadow the renewals CTE): move these CTEs to the statement's top-level WITH")
        used = [k for k in UNMODELLED_CLAUSES if sel.args.get(k)]
        if used:
            raise LineageExtractError(f"{self.source}: {where} uses {', '.join(used)}, which the lineage walk does "
                                      f"not model (teach lineage/scope_walk.py before using it)")
        for alias, (node, _src) in scope.selected_sources.items():
            used = sorted({what for k, what in TABLE_MODIFIERS.items()
                           if isinstance(node, exp.Table) and node.args.get(k)})
            if used:
                raise LineageExtractError(
                    f"{self.source}: {where} reads {alias} with {', '.join(used)} ({node.sql(DIALECT)[:80]}), which "
                    f"the lineage walk does not model (teach lineage/scope_walk.py before using it)")
        for pred, _join, _bounding in self.predicates(scope):
            if pred.find(exp.Query) is not None:
                raise LineageExtractError(
                    f"{self.source}: {where} has a subquery inside a WHERE / ON / HAVING condition "
                    f"({pred.sql(DIALECT)[:80]}), which the lineage walk does not model: move it into a CTE and "
                    f"join it")

    def renewal_derived(self, scope: Scope) -> bool:
        """True when the scope's rows descend from the renewals CTE (each carries one renewal's as_of)."""
        if id(scope) not in self._derived:
            self._derived[id(scope)] = scope is self.renewals or any(
                isinstance(src, Scope) and self.renewal_derived(src) for _node, src in scope.selected_sources.values())
        return self._derived[id(scope)]

    def _check_grain(self, scope: Scope) -> None:
        """Stop when a scope built on the renewals CTE leaves the renewal grain (module docstring)."""
        sel = scope.expression
        derived = [a for a, (_node, src) in scope.selected_sources.items()
                   if isinstance(src, Scope) and self.renewal_derived(src)]
        if not derived:
            return
        where, key = self.scope_label(scope), RENEWAL_KEY

        def stop(what: str) -> LineageExtractError:
            return LineageExtractError(
                f"{self.source}: {where} {what}. Every feature window is relative to one renewal's as_of, so this "
                f"would mix rows bounded by different as_of dates; keep the CTE at renewal grain ({key}) or teach "
                f"lineage/scope_walk.py")

        def is_key(e, aliases) -> bool:
            return isinstance(e, exp.Column) and e.name == key and e.table in aliases
        if sum(scope.selected_sources[a][1] is self.renewals for a in derived) > 1:
            raise stop("joins the renewals CTE to itself")
        conditions = self.scope_predicates(scope)
        for j in sel.args.get("joins") or []:   # two renewal-derived sources meet only on the key
            alias = j.alias_or_name
            others = [a for a in derived if a != alias]
            if alias in derived and others and not any(
                    isinstance(p, exp.EQ) and ((is_key(p.this, [alias]) and is_key(p.expression, others))
                                               or (is_key(p.expression, [alias]) and is_key(p.this, others)))
                    for p in conditions):
                raise stop(f"joins {alias} without the condition {alias}.{key} = <renewal>.{key}")
        group = sel.args.get("group")
        if group is not None:
            if not any(is_key(g, derived) for g in group.expressions):
                raise stop(f"groups by ({group.sql(DIALECT)[9:]}) without {derived[0]}.{key}")
            if any(group.args.get(k) for k in ("rollup", "cube", "grouping_sets", "totals")) \
                    or group.find(exp.Rollup, exp.Cube, exp.GroupingSets) is not None:
                raise stop(f"groups with subtotals ({group.sql(DIALECT)[9:]}): a subtotal row aggregates the rows "
                           f"of several groups")
        elif any(not self._in_subquery(a, p) and a.find_ancestor(exp.Window) is None
                 for p in sel.selects for a in p.find_all(exp.AggFunc)):
            raise stop("aggregates over all renewals (an aggregate with no GROUP BY)")
        if sel.args.get("distinct") and not any(
                is_key(p.this if isinstance(p, exp.Alias) else p, derived) for p in sel.selects):
            raise stop(f"selects DISTINCT without {derived[0]}.{key} (rows of different renewals would merge)")
        cut = [k.upper() for k in ("limit", "offset") if sel.args.get(k)]
        if cut:
            raise stop(f"cuts its rows with {' / '.join(cut)} (which renewals keep a row would depend on the other "
                       f"renewals' rows)")
        for p in list(sel.selects) + [sel.args[k] for k in ("qualify", "having", "order") if sel.args.get(k)]:
            for win in p.find_all(exp.Window):
                if not self._in_subquery(win, p) and not any(is_key(x, derived)
                                                             for x in win.args.get("partition_by") or []):
                    raise stop(f"uses a window function that is not partitioned by {derived[0]}.{key} "
                               f"({win.sql(DIALECT)[:60]})")

    # ------------------------------------------------------------------ the PIT rule, as data
    def _pit_predicate(self) -> tuple[str, str, int]:
        """(as_of column, renewal column, N) from ``<as_of> = date_sub(<renewal>, N)`` in renewals."""
        where = self.renewals.expression.args.get("where")
        for eq in conjuncts(where.this) if where is not None else []:   # a top-level AND conjunct only
            if not isinstance(eq, exp.EQ):
                continue
            left, right = eq.this, eq.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.TsOrDsAdd) \
                    and isinstance(right.this, exp.Column) and self._day_unit(right):
                n = literal_number(right.expression)
                if n is not None and n < 0:
                    return left.name, right.this.name, int(-n)
        raise LineageExtractError(f"{self.source}: the point-in-time predicate "
                                  f"'<as_of> = date_sub(<renewal date>, N)' was not found as a top-level AND "
                                  f"condition of the renewals CTE's WHERE clause")

    @staticmethod
    def _day_unit(e: exp.TsOrDsAdd) -> bool:
        unit = e.args.get("unit")
        return unit is None or unit.name.upper() in ("DAY", "DAYS")

    def pit_rule(self, sql_text: str) -> dict:
        where = self.renewals.expression.args["where"].this
        needle = f"{self.as_of_column} = date_sub({self.renewal_column}"
        line = next((i for i, text in enumerate(sql_text.splitlines(), 1) if needle in text), None)
        return {"as_of_expr": self.as_of_column, "renewal_expr": self.renewal_column,
                "predicate": where.sql(DIALECT), "as_of_offset_days": -self.t_offset, "source_line": line}

    # ------------------------------------------------------------------ helpers
    def scope_label(self, s: Scope) -> str:
        if id(s) in self.cte_name:
            return self.cte_name[id(s)]
        if s.is_derived_table:
            return f"derived:{s.expression.parent.alias}"
        if s.is_subquery:
            p = s.expression.parent
            while p is not None and not isinstance(p, exp.Alias):
                p = p.parent
            return f"subquery:{p.alias if p is not None else '?'}"
        return "final_select"

    @staticmethod
    def _lookup(scope: Scope, alias: str):
        s = scope
        while s is not None:
            if alias in s.sources:
                return s, s.sources[alias]
            s = s.parent
        return None, None

    def _projection(self, scope: Scope, name: str):
        for p in scope.expression.selects:
            if p.alias_or_name == name:
                return p
        raise LineageExtractError(f"{self.source}: column '{name}' is not projected by {self.scope_label(scope)}")

    def _anchor(self, scope: Scope, e) -> tuple[str, float] | None:
        """(anchor name, offset vs as_of) when ``e`` is renewals.as_of / renewal_date (+- N days)."""
        off = 0.0
        if isinstance(e, exp.TsOrDsAdd):
            n = literal_number(e.expression)
            if n is None or n != int(n) or not self._day_unit(e):   # whole days only
                return None
            off, e = n, e.this
        if not isinstance(e, exp.Column):
            return None
        _s, src = self._lookup(scope, e.table)
        if not isinstance(src, Scope):
            return None
        if src is self.renewals:
            return (e.name, self.anchor_offset[e.name] + off) if e.name in self.anchor_offset else None
        # pass-through CTE column (joined.renewal_date -> renewals.renewal_date)
        proj = next((p for p in src.expression.selects if p.alias_or_name == e.name), None)
        if proj is None:
            return None
        a = self._anchor(src, proj.this if isinstance(proj, exp.Alias) else proj)
        return None if a is None else (a[0], a[1] + off)

    def _event_column(self, scope: Scope, e) -> exp.Column | None:
        if isinstance(e, exp.Column) and self._anchor(scope, e) is None and self._lookup(scope, e.table)[1] is not None:
            return e
        return None

    def classify(self, scope: Scope, pred) -> tuple[str, exp.Column | None, Bound | None]:
        """(role, event column, bound) of one conjunct of a WHERE / ON / CASE condition."""
        ops = {exp.GT: ("lo", False), exp.GTE: ("lo", True), exp.LT: ("hi", False), exp.LTE: ("hi", True)}
        if isinstance(pred, exp.EQ):
            left, right = pred.this, pred.expression
            if isinstance(left, exp.Column) and isinstance(right, exp.Column) and left.name == right.name:
                return "JOIN_KEY", None, None
            if isinstance(left, exp.Literal) or isinstance(right, exp.Literal):
                return "FILTER", None, None
            for event, anchor in ((left, right), (right, left)):
                a = self._anchor(scope, anchor)
                if a and self._event_column(scope, event) is not None:
                    return "WINDOW_BOUND", event, ("eq", a[1], True)
            return "PREDICATE", None, None
        for cls, (side, incl) in ops.items():
            if isinstance(pred, cls):
                left, right = pred.this, pred.expression
                a = self._anchor(scope, right)
                if a and self._event_column(scope, left) is not None:
                    return "WINDOW_BOUND", left, (side, a[1], incl)
                a = self._anchor(scope, left)   # anchor OP event -> flip the side
                if a and self._event_column(scope, right) is not None:
                    return "WINDOW_BOUND", right, ({"lo": "hi", "hi": "lo"}[side], a[1], incl)
                return "PREDICATE", None, None
        if isinstance(pred, exp.Between):
            return "TEMPORAL_JOIN", None, None
        if isinstance(pred, (exp.In, exp.Not, exp.Is)):
            return "FILTER", None, None
        return "PREDICATE", None, None

    def predicates(self, scope: Scope) -> list[tuple[exp.Expression, exp.Join | None, bool]]:
        """(conjunct, join, bounding) for every top-level AND conjunct of the scope's ON clauses
        (with their join), of its WHERE clause (join None) and of HAVING / QUALIFY. Only ON and
        WHERE conjuncts are ``bounding``: they filter rows before the scope aggregates them."""
        sel = scope.expression
        preds: list[tuple[exp.Expression, exp.Join | None, bool]] = []
        for j in sel.args.get("joins") or []:
            preds += [(p, j, True) for p in conjuncts(j.args.get("on"))]
        if sel.args.get("where"):
            preds += [(p, None, True) for p in conjuncts(sel.args["where"].this)]
        for clause in ("having", "qualify"):
            if sel.args.get(clause):
                preds += [(p, None, False) for p in conjuncts(sel.args[clause].this)]
        return preds

    def scope_predicates(self, scope: Scope) -> list:
        return [p for p, _join, _bounding in self.predicates(scope)]

    @staticmethod
    def restricts(join: exp.Join | None, alias: str) -> bool:
        """True when a condition in this clause removes the alias's rows that fail it. A WHERE
        always does; an ON clause does not for the side an outer join preserves."""
        if join is None:
            return True
        side = (join.side or "").upper()
        if not side:                       # INNER / CROSS
            return True
        if side == "LEFT":                 # rows before the join are preserved; the joined table is filtered
            return join.alias_or_name == alias
        if side == "RIGHT":                # the joined table is preserved
            return join.alias_or_name != alias
        return False                       # FULL: both sides preserved

    def nullable(self, join: exp.Join | None, alias: str) -> bool:
        """True when ``alias`` is the side an outer join fills with NULLs: its rows add values
        but do not decide which rows of the scope exist."""
        return join is not None and (join.side or "").upper() in ("LEFT", "RIGHT") and self.restricts(join, alias)

    @staticmethod
    def single_row(scope: Scope) -> bool:
        """A scope that always yields exactly one row: a constant SELECT, or a global aggregate."""
        sel = scope.expression
        if sel.args.get("group") or sel.args.get("having") or sel.args.get("joins"):
            return False
        return not scope.selected_sources or all(p.find(exp.AggFunc) is not None for p in sel.selects)

    def plain_filter(self, scope: Scope) -> str | None:
        """The alias of the one base table the scope reads when it is a plain row filter over it
        (``SELECT <row expressions> FROM <table> [WHERE ...]``: one output row per table row it
        keeps), else None. None for the renewals CTE too: its rows are the renewal grain (the
        as-of snapshot row), not events to bound."""
        if id(scope) not in self._plain:
            sources = list(scope.selected_sources.items())
            plain = scope is not self.renewals and len(sources) == 1 and isinstance(sources[0][1][1], exp.Table) \
                and not scope.subquery_scopes and isinstance(scope.expression, exp.Select) \
                and not beyond_a_row_filter(scope.expression)
            self._plain[id(scope)] = sources[0][0] if plain else None
        return self._plain[id(scope)]

    def pushed_bounds(self, scope: Scope, alias: str, src: Scope) -> list[Bound]:
        """The bounds the WHERE / ON clauses of ``scope`` put on the table rows behind ``alias``, when
        ``alias`` is the plain row filter ``src`` and the bounded column is a column of its table
        passed through unchanged (module docstring). Same rules as ``window_for``; nothing else is
        pushed down."""
        inner = self.plain_filter(src)
        if inner is None:
            return []
        bounds: list[Bound] = []
        for pred, join, bounding in self.predicates(scope):
            role, event, bound = self.classify(scope, pred)
            if not (bounding and role == "WINDOW_BOUND" and event.table == alias and self.restricts(join, alias)):
                continue
            proj = next((p for p in src.expression.selects if p.alias_or_name == event.name), None)
            column = proj.this if isinstance(proj, exp.Alias) else proj
            if isinstance(column, exp.Column) and column.table == inner:
                bounds.append(bound)
        return bounds

    def window_for(self, scope: Scope, alias: str, extra=()) -> Window | None:
        """Row window of a base-table alias: the intersection of the scope's WHERE / ON bounds on
        it and ``extra`` (the bounds of the CASE guards that cover the column being resolved)."""
        bounds: list[Bound] = list(extra)
        for pred, join, bounding in self.predicates(scope):
            role, event, bound = self.classify(scope, pred)
            if bounding and role == "WINDOW_BOUND" and event.table == alias and self.restricts(join, alias):
                bounds.append(bound)
        return intersect(bounds)

    # ------------------------------------------------------------------ the walk
    @staticmethod
    def _in_subquery(node, root) -> bool:
        p = node.parent
        while p is not None and p is not root:
            if isinstance(p, exp.Query):   # Subquery, or a bare SELECT (EXISTS (SELECT ...))
                return True
            p = p.parent
        return False

    def _source_of(self, scope: Scope, c: exp.Column):
        """(scope, source) of a column's table alias; None for a lambda parameter."""
        s, src = self._lookup(scope, c.table)
        if src is not None:
            return s, src
        lam = c.find_ancestor(exp.Lambda)
        if lam is not None and c.table in {p.name for p in lam.expressions}:
            return None, None   # w.starts_on inside EXISTS(i.windows, w -> ...): resolved through i.windows
        raise LineageExtractError(f"{self.source}: cannot resolve '{c.sql(DIALECT)}' in "
                                  f"{self.scope_label(scope)} (unknown table alias '{c.table}')")

    @staticmethod
    def _inside(node, ancestor, root) -> bool:
        p = node.parent
        while p is not None:
            if p is ancestor:
                return True
            if p is root:
                return False
            p = p.parent
        return False

    @staticmethod
    def _aggregate_of(case: exp.Case):
        """The NULL-skipping aggregate whose argument is this CASE (through parentheses, a cast
        or a one-expression DISTINCT), else None."""
        child, p = case, case.parent
        while isinstance(p, (exp.Paren, exp.Cast, exp.Distinct)):
            if isinstance(p, exp.Distinct) and len(p.expressions) != 1:
                return None
            child, p = p, p.parent
        return p if isinstance(p, NULL_SKIPPING_AGGREGATES) and p.this is child else None

    def case_guards(self, scope: Scope, e) -> list[Guard]:
        """The CASE branches of ``e`` that bound which rows of a base table the aggregate reads
        (module docstring). A comparison under OR / NOT, a CASE with several WHENs, a CASE that is
        not an aggregate's argument or whose ELSE still contributes yields no guard: the columns
        then keep the scope's own window."""
        guards = []
        for case in e.find_all(exp.Case):
            if self._in_subquery(case, e) or case.find_ancestor(exp.Lambda) is not None:
                continue
            ifs = case.args.get("ifs") or []
            if case.this is not None or len(ifs) != 1:
                continue
            aggregate = self._aggregate_of(case)
            if aggregate is None or not neutral_default(aggregate, ifs[0].args.get("true"), case.args.get("default")):
                continue
            bounds: dict[str, list[Bound]] = {}
            events: set[int] = set()
            for pred in conjuncts(ifs[0].this):
                role, event, bound = self.classify(scope, pred)
                if role == "WINDOW_BOUND" and isinstance(scope.sources.get(event.table), exp.Table):
                    bounds.setdefault(event.table, []).append(bound)
                    events.add(id(event))
            if bounds:
                default = case.args.get("default")
                guards.append(Guard(ifs[0], bounds, events,
                                    zero_default=default is not None and not isinstance(default, exp.Null)))
        return guards

    @staticmethod
    def _coalesced_to_zero(c: exp.Column) -> bool:
        """``COALESCE(<c>, 0)`` (also IFNULL / NVL, which sqlglot reads as COALESCE)."""
        p = c.parent
        return isinstance(p, exp.Coalesce) and p.this is c and len(p.expressions) == 1 \
            and literal_number(p.expressions[0]) == 0

    def matched_window(self, scope: Scope, c: exp.Column) -> Window | None:
        """``EXISTS(<c>, w -> <event> BETWEEN w.<lo> AND w.<hi>)``: the elements of ``c`` only
        matter where they cover an event row, so the read of ``c``'s source is matched to the row
        window of ``<event>``'s table in this scope. None when ``c`` is used in any other way."""
        fn = c.parent
        if not isinstance(fn, exp.Exists) or fn.this is not c:
            return None
        lam = fn.args.get("expression")
        if not isinstance(lam, exp.Lambda) or not isinstance(lam.this, exp.Between):
            return None
        params = {p.name for p in lam.expressions}
        event = lam.this.this

        def element_field(x) -> bool:
            return (isinstance(x, exp.Dot) and isinstance(x.this, exp.Identifier) and x.this.name in params) or \
                   (isinstance(x, exp.Column) and x.table in params)
        if not isinstance(event, exp.Column) or event.table in params \
                or not all(element_field(lam.this.args.get(k)) for k in ("low", "high")) \
                or not isinstance(scope.sources.get(event.table), exp.Table):
            return None
        return self.window_for(scope, event.table)

    def silent_sources(self, scope: Scope, e, path: tuple[str, ...]) -> list[Leaf]:
        """Base tables the scope joins without naming a column of theirs in ``e`` or in a WHERE / ON
        condition: their rows still decide how many rows ``e`` sees, so each is read as a row count."""
        named = {c.table for c in e.find_all(exp.Column)}
        named |= {c.table for pred in self.scope_predicates(scope) for c in pred.find_all(exp.Column)}
        group = scope.expression.args.get("group")
        named |= {c.table for c in group.find_all(exp.Column)} if group is not None else set()
        leaves = []
        for alias, (_node, src) in scope.selected_sources.items():
            if alias in named:
                continue
            if isinstance(src, exp.Table):
                leaves.append(Leaf(table_name(src), "*", "ROWCOUNT", path, self.scope_label(scope), e.sql(DIALECT),
                                   self.window_for(scope, alias)))
            elif len(scope.selected_sources) > 1 and not self.single_row(src):
                raise LineageExtractError(
                    f"{self.source}: {self.scope_label(scope)} joins {alias} without naming any of its columns; "
                    f"the lineage walk cannot tell which of its rows '{e.alias_or_name}' depends on (join it on a "
                    f"key, or teach lineage/scope_walk.py)")
        return leaves

    def resolve_expr(self, scope: Scope, e, path: tuple[str, ...], role: str) -> list[Leaf]:
        leaves: list[Leaf] = []
        guards = self.case_guards(scope, e)
        post: dict[tuple[str, str], list[Bound]] = {}   # comparisons of a CTE column with an anchor (after aggregation)
        anchor_ids: set[int] = set()
        for cmp in e.find_all(exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE):
            if self._in_subquery(cmp, e) or cmp.find_ancestor(exp.Lambda) is not None:
                continue
            r, event, bound = self.classify(scope, cmp)
            if r == "WINDOW_BOUND":
                if isinstance(self._lookup(scope, event.table)[1], Scope):
                    post.setdefault((event.table, event.name), []).append(bound)
                for c in cmp.find_all(exp.Column):
                    if c is not event and self._anchor(scope, c):
                        anchor_ids.add(id(c))
        for c in e.find_all(exp.Column):
            if self._in_subquery(c, e) or self._source_of(scope, c)[1] is None:
                continue
            covering = [g for g in guards if self._inside(c, g.branch, e)]
            extra = tuple(b for g in covering for b in g.bounds.get(c.table, ()))
            if id(c) in anchor_ids:
                crole = "ANCHOR"
            elif any(id(c) in g.events for g in covering):
                crole = "WINDOW_BOUND"
            else:
                crole = role
            leaves += self.resolve_column(scope, c, path, crole, extra, post, matched=self.matched_window(scope, c))
        for sub in scope.subquery_scopes:
            if not self._inside(sub.expression, e, None):
                continue
            label = self.scope_label(sub)
            for p in sub.expression.selects:
                leaves += self.resolve_expr(sub, p, path + (label,), role)
                leaves += self.silent_sources(sub, p, path + (label,))
            leaves += self.context_leaves(sub, path + (label,))
        inner = e.this if isinstance(e, exp.Alias) else e
        for cnt in inner.find_all(exp.Count):
            if isinstance(cnt.this, exp.Star) and not self._in_subquery(cnt, e):
                for alias, src in scope.sources.items():
                    if isinstance(src, exp.Table):
                        leaves.append(Leaf(table_name(src), "*", "ROWCOUNT", path, self.scope_label(scope),
                                           e.sql(DIALECT), self.window_for(scope, alias)))
                    elif alias in scope.selected_sources and (base := self.plain_filter(src)) is not None:
                        # COUNT(*) over a plain row filter counts the rows of its table that pass both filters
                        label = self.scope_label(src)
                        bounds = window_bounds(self.window_for(src, base)) + self.pushed_bounds(scope, alias, src)
                        leaves.append(Leaf(table_name(src.sources[base]), "*", "ROWCOUNT", path + (label,), label,
                                           e.sql(DIALECT), intersect(bounds)))
        return leaves

    def context_leaves(self, scope: Scope, path: tuple[str, ...]) -> list[Leaf]:
        """Columns read by the scope's WHERE / ON predicates (which rows the value is computed from)."""
        out: list[Leaf] = []
        for pred, join, bounding in self.predicates(scope):
            role, event, _bound = self.classify(scope, pred)
            if role == "WINDOW_BOUND" and not (bounding and self.restricts(join, event.table)):
                role = "PREDICATE"   # HAVING, or an ON condition on the side an outer join preserves: bounds nothing
            if scope is self.renewals:
                role = "ROW_SELECTION"
            for c in pred.find_all(exp.Column):
                if self._source_of(scope, c)[1] is None:
                    continue
                crole = "ANCHOR" if (role == "WINDOW_BOUND" and c is not event) else role
                # a CTE on the NULL-filled side of an outer join adds values only; any other CTE named in a
                # condition also decides which rows exist, so its own WHERE / ON reads count (context=False)
                out += self.resolve_column(scope, c, path, crole, (), {}, context=self.nullable(join, c.table))
        return out

    def resolve_column(self, scope: Scope, c: exp.Column, path: tuple[str, ...], role: str, extra, post: dict,
                       context: bool = False, matched: Window | None = None) -> list[Leaf]:
        """Leaves of one column reference. ``extra`` holds the bounds of the CASE guards that cover
        it (base-table columns), ``post`` the comparisons applied to CTE columns after aggregation,
        ``matched`` the event window an unbounded source is matched to."""
        s, src = self._source_of(scope, c)
        if isinstance(src, exp.Table):
            return [Leaf(table_name(src), c.name, role, path, self.scope_label(s), None,
                         self.window_for(s, c.table, extra), matched=matched)]
        label = self.scope_label(src)
        proj = self._projection(src, c.name)
        if any(g.zero_default for g in self.case_guards(src, proj)) and not self._coalesced_to_zero(c):
            raise LineageExtractError(
                f"{self.source}: {self.scope_label(scope)} reads {label}.{c.name} without COALESCE({c.name}, 0). "
                f"{label}.{c.name} bounds its rows with <aggregate>(CASE WHEN ... ELSE 0 END), which is 0 when rows "
                f"exist only outside the window and NULL when there is none: read it as COALESCE(..., 0), or drop "
                f"the ELSE 0")
        leaves = self.resolve_expr(src, proj, path + (label,), role)
        if not context:
            leaves += self.silent_sources(src, proj, path + (label,))
            leaves += self.context_leaves(src, path + (label,))
        compared = post.get((c.table, c.name))
        pushed = self.pushed_bounds(s, c.table, src)
        for leaf in leaves:
            if pushed and leaf.cte == label:   # a read of the filter's table: the reading scope bounds its rows
                leaf.window = intersect(window_bounds(leaf.window) + pushed)
            if leaf.cte_expr is None and leaf.cte == label:
                leaf.cte_expr = proj.sql(DIALECT)
            if role != "VALUE" and leaf.role == "VALUE":
                leaf.role = role
            if compared and leaf.role in ("VALUE", "ANCHOR", "WINDOW_BOUND"):
                leaf.post_cmp = tuple(sorted(set(leaf.post_cmp) | set(compared)))
            if matched is not None and leaf.window is None and leaf.matched is None:
                leaf.matched = matched
        return leaves

    # ------------------------------------------------------------------ public
    def leaves_of(self, p) -> list[Leaf]:
        """The scope walk of one projection of the final SELECT: every read behind it, merged per
        (table, column, role, CTE, window). A column read through two different windows stays two leaves."""
        raw = self.resolve_expr(self.root, p, ("final_select",), "VALUE")
        # the final SELECT's own joins / WHERE decide which rows every column is computed from
        raw += self.silent_sources(self.root, p, ("final_select",))
        raw += self.context_leaves(self.root, ("final_select",))
        # the renewals grain (row selection, join keys) is recorded once, at dataset level
        raw = [x for x in raw if not (x.cte == "renewals" and x.role in ("ROW_SELECTION", "JOIN_KEY"))]
        value_keys = {(x.table, x.column, x.cte, x.window, x.matched) for x in raw if x.role == "VALUE"}
        merged: dict[tuple, Leaf] = {}
        for leaf in raw:
            read = (leaf.table, leaf.column, leaf.cte, leaf.window, leaf.matched)
            if leaf.role == "PREDICATE" and read in value_keys:
                continue   # the same read, already there as a value
            k = (leaf.table, leaf.column, leaf.role, leaf.cte, leaf.window, leaf.matched)
            if k in merged:
                merged[k].post_cmp = tuple(sorted(set(merged[k].post_cmp) | set(leaf.post_cmp)))
                merged[k].path = min(merged[k].path, leaf.path, key=len)
            else:
                merged[k] = leaf
        return list(merged.values())

    def columns(self) -> list[ColumnLineage]:
        """Lineage of every output column of the final SELECT, in select order."""
        try:
            nodes = sqlglot_lineage(None, self.sql, schema=self.schema, dialect=DIALECT)
        except SqlglotError as e:
            raise LineageExtractError(f"{self.source}: sqlglot lineage() failed ({e})") from e
        out = []
        for i, p in enumerate(self.root.expression.selects, 1):
            col = ColumnLineage(p.alias_or_name, i, p.sql(DIALECT))
            col.leaves = self.leaves_of(p)
            node = nodes.get(col.name)
            if node is None:
                raise LineageExtractError(f"{self.source}: sqlglot lineage() has no node for output column "
                                          f"'{col.name}'")
            for n in node.walk():   # the library's own projection lineage (cross-check)
                if n.downstream:
                    continue
                if isinstance(n.source, exp.Table):
                    col.sqlglot_leaves.append((table_name(n.source), n.name.split(".")[-1]))
                else:
                    col.sqlglot_unresolved.append(f"{type(n.source).__name__}:{n.name}")
            out.append(col)
        return out

    def ctes(self) -> list[dict]:
        """name, base tables read (alias, table, window), CTE dependencies and SQL of every CTE."""
        res = []
        for cs in self.root.cte_scopes:
            tables, deps = [], []
            for alias, (_node, src) in cs.selected_sources.items():
                if isinstance(src, exp.Table):
                    tables.append((alias, table_name(src), self.window_for(cs, alias)))
                else:
                    deps.append(self.scope_label(src))
            for sub in cs.subquery_scopes + cs.derived_table_scopes:
                for alias, (_node, src) in sub.selected_sources.items():
                    if isinstance(src, exp.Table):
                        tables.append((alias, table_name(src), self.window_for(sub, alias)))
            res.append({"name": self.cte_name[id(cs)], "tables": tables, "depends_on": sorted(set(deps)),
                        "sql": cs.expression.sql(DIALECT)})
        return res

    def final_selects(self) -> dict[str, exp.Expression]:
        return {p.alias_or_name: p for p in self.qualified.selects}

    def cte_projection(self, cte: str, column: str):
        if cte not in self.cte_scopes:
            raise LineageExtractError(f"{self.source}: CTE '{cte}' is missing")
        return self._projection(self.cte_scopes[cte], column)
