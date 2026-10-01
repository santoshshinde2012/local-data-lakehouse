"""Assemble the lineage graph (spec ``metadata-graph/0.1``) from the extracted facts.

``assemble(repo, profile)`` runs the extractor (``extract.py``), the gold SQL scope walk
(``scope_walk.py``) and turns the facts into typed nodes and edges in a ``LineageGraph``:

  data        source CSVs -> bronze -> silver -> gold.churn_renewal_features -> exports
              (DataColumn + DERIVED_FROM / COUNTS_ROWS_OF with roles, CTE and window), the
              retail companion SQL, Window and PointInTimeRule nodes, Parameter nodes
  code        Job, SqlFile, Cte, EnvVar, Dag / DagTask, MakeTarget, ShellScript, CiStep
  contracts   Contract / Assertion for check_churn_export, check_gold_parity, the README
              numbers, the graph and lineage contracts; Metric nodes
  bridge      one GraphElement per business graph node label and edge type (all 21), each
              SOURCED_FROM its silver / gold Dataset and DataColumn, the graph Parquet
              tables and their Iceberg twins as Dataset nodes (one twin per table, or the
              union twins gold.graph_nodes / gold.graph_edges with one partition per table;
              MIRRORS carries the partition), the Spark graph job as PARITY_TWIN_OF the builder
  full only   FileSnapshot nodes for the data files that exist, README / metric values
              observed in the exports, the retention-radar interface (RADAR_DIR)

Nothing is dropped silently. A shape the assembler does not understand raises
``LineageExtractError``; a name it recognises but cannot resolve is appended to
``graph.unresolved`` (the core contract fails on any entry). ``graph.warnings`` are about
the repo and fail the contract under ``--strict``; ``graph.environment_warnings`` are about
the machine the full profile ran on (no RADAR_DIR, no export files) and never gate.

Point-in-time status of a gold feature: the loosest upper bound over all its reads outside
the as-of snapshot row (``column_upper``). At or before as_of it is ``compliant``; after
as_of it is a ``declared_exception`` when lakehouse_graph.spec.FEATURE_CARDS declares one,
else an ``undeclared_exception`` (a contract error). A read with no time bound counts as
unbounded unless its table is a declared reference table (``spec.GLOBAL_DIMENSION_TABLES``)
*and* the scope walk sees its rows matched to bounded event rows (``leaf_upper``).
"""
from __future__ import annotations

import ast
import base64
import csv
import io
import json
import re
import string
import subprocess
from collections import Counter
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage

from .. import manifest as mf
from .. import spec as gspec
from . import extract as ex
from . import spec
from .scope_walk import INF, ColumnLineage, GoldLineage, Leaf, format_bounds, format_window
from .spec import LineageExtractError, column_id, column_ref, dataset_ref

# Roles of the non-feature gold columns (display only; features come from the train contract).
GOLD_ROLES = {"user_id": "key", "user_name": "display_name", "outcome": "outcome", "route": "routing",
              "feature_as_of": "time_anchor", "renewal_date": "time_anchor"}
# The pandas twin mirrors these Spark jobs; the parity contract checks the first two.
PARITY_TWINS = ((spec.SILVER_JOB, spec.PARITY_CONTRACT), (spec.GOLD_JOB, spec.PARITY_CONTRACT), (spec.EXPORT_JOB, None))
JOB_CONSTS = ("ALLOWANCE", "CAP_CUT", "DUNNING_DAYS", "HERO_ID", "SEED", "N_USERS", "PLANS", "PRICE")


class LineageGraph:
    """Typed nodes and edges; every write is checked against ``spec.NODE_SCHEMA`` / ``EDGE_SCHEMA``."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict] = {}
        self.edges: list[dict] = []
        self._merged: dict[tuple[str, str, str], dict] = {}
        self.unresolved: list[dict] = []
        self.warnings: list[str] = []               # about the repo: fail the contract with --strict
        self.environment_warnings: list[str] = []   # about this machine (no RADAR_DIR, no exports): never gate
        self.inputs: dict[str, str] = {}
        self.profile = "core"
        self.overlays: dict[str, dict] = {}   # Tier-1 / Tier-2 overlay name -> its summary (lineage manifest)

    def node(self, label: str, node_id: str, **props) -> str:
        ns = spec.NODE_SCHEMA.get(label)
        if ns is None:
            raise LineageExtractError(f"schema: unknown node label {label!r}")
        known = {c for c, _ in ns.columns}
        bad = set(props) - known
        if bad:
            raise LineageExtractError(f"schema: {label} has no property {sorted(bad)} (declare it in lineage/spec.py)")
        if node_id in self.nodes:
            if self.nodes[node_id]["label"] != label:
                raise LineageExtractError(f"node id {node_id} is used for {self.nodes[node_id]['label']} and {label}")
            self.nodes[node_id]["props"].update({k: v for k, v in props.items() if v is not None})
        else:
            self.nodes[node_id] = {"label": label, "props": dict(props)}
        return node_id

    def edge(self, rel: str, src: str, dst: str, **props) -> None:
        es = spec.EDGE_SCHEMA.get(rel)
        if es is None:
            raise LineageExtractError(f"schema: unknown edge type {rel!r}")
        for end, nid in (("source", src), ("target", dst)):
            if nid not in self.nodes:
                raise LineageExtractError(f"edge {rel} {src} -> {dst}: the {end} node {nid} does not exist "
                                          f"(an unresolved name)")
        pair = (self.nodes[src]["label"], self.nodes[dst]["label"])
        if pair not in es.pairs:
            raise LineageExtractError(f"schema: {rel} does not allow {pair[0]} -> {pair[1]} ({src} -> {dst})")
        bad = set(props) - {c for c, _ in es.columns}
        if bad:
            raise LineageExtractError(f"schema: {rel} has no property {sorted(bad)} (declare it in lineage/spec.py)")
        if rel in spec.MERGED_EDGES:
            old = self._merged.get((rel, src, dst))
            if old is not None:
                for k, v in props.items():
                    cur = old["props"].get(k)
                    if v is None:
                        continue
                    if cur is None:
                        old["props"][k] = v
                    elif isinstance(cur, str) and str(v) not in cur.split(","):
                        old["props"][k] = f"{cur},{v}"
                return
        e = {"rel": rel, "src": src, "dst": dst, "props": dict(props)}
        self.edges.append(e)
        if rel in spec.MERGED_EDGES:
            self._merged[(rel, src, dst)] = e

    def has(self, node_id: str) -> bool:
        return node_id in self.nodes

    def label(self, node_id: str) -> str:
        return self.nodes[node_id]["label"]

    def props(self, node_id: str) -> dict:
        return self.nodes[node_id]["props"]

    def ids(self, label: str) -> list[str]:
        return sorted(i for i, n in self.nodes.items() if n["label"] == label)

    def out(self, node_id: str, rel: str) -> list[dict]:
        return [e for e in self.edges if e["rel"] == rel and e["src"] == node_id]

    def counts(self) -> dict:
        nodes = Counter(n["label"] for n in self.nodes.values())
        edges = Counter(e["rel"] for e in self.edges)
        return {"nodes": {label: nodes.get(label, 0) for label in spec.NODE_SCHEMA},
                "edges": {rel: edges.get(rel, 0) for rel in spec.EDGE_SCHEMA},
                "total_nodes": len(self.nodes), "total_edges": len(self.edges)}

    def unresolve(self, where: str, what: str) -> None:
        item = {"where": where, "what": what}
        if item not in self.unresolved:
            self.unresolved.append(item)


# --------------------------------------------------------------------------- assembly
class Assembler:
    def __init__(self, repo: str | Path, profile: str = "core", *, export_dir: str | Path | None = None,
                 sample_dir: str | Path | None = None, radar_dir: str | Path | None = None,
                 radar_public: bool = False):
        if profile not in spec.PROFILES:
            raise ValueError(f"invalid lineage profile {profile!r}: expected one of {', '.join(spec.PROFILES)}")
        self.repo = Path(repo)
        self.profile = profile
        self.export_dir = Path(export_dir) if export_dir else None
        self.sample_dir = Path(sample_dir) if sample_dir else None
        self.radar_dir = Path(radar_dir) if radar_dir else None
        self.radar_public = radar_public
        self.g = LineageGraph()
        self.g.profile = profile
        self.facts = ex.extract_all(self.repo)
        self.files: ex.SourceFiles = self.facts["files"]
        self.jobs: dict[str, dict] = self.facts["jobs"]
        self.g.unresolved.extend(self.facts["unresolved"])
        self.gold_columns: list[str] = []
        self.gold_role: dict[str, str] = {}
        self.exports: dict[str, str] = {}      # file name -> repo-relative path
        self.export_columns: dict[str, list[str]] = {}
        self.source_files: dict[str, str] = {}  # dataset / export node id -> file name (FileSnapshot lookup)

    # ------------------------------------------------------------------ small helpers
    def dataset(self, name: str, *, kind: str, layer: str, domain: str, **props) -> str:
        return self.g.node("Dataset", f"ds:{name}", name=name, ref=dataset_ref(name), kind=kind, layer=layer,
                           domain=domain, **props)

    def iceberg(self, fq: str, domain: str, **props) -> str:
        parts = fq.split(".")
        return self.dataset(fq, kind="iceberg_table", layer=parts[1] if len(parts) > 2 else "unknown",
                            domain=domain, format="iceberg", **props)

    def column(self, dataset: str, name: str, ordinal: int, *, layer: str, domain: str, container: str | None = None,
               **props) -> str:
        cid = self.g.node("DataColumn", column_id(dataset, name), ref=column_ref(dataset, name), name=name,
                          dataset=dataset, layer=layer, domain=domain, ordinal=ordinal, **props)
        self.g.edge("HAS_COLUMN", container or f"ds:{dataset}", cid, ordinal=ordinal)
        return cid

    def need_column(self, dataset: str, name: str, context: str) -> str:
        cid = column_id(dataset, name)
        if not self.g.has(cid):
            raise LineageExtractError(f"{context}: column {name!r} is not a column of {dataset} "
                                      f"(an unresolved column name)")
        return cid

    def ensure_job(self, rel: str, where: str) -> str | None:
        """Job node for a Python file a runner references; unresolved when the file is missing."""
        jid = f"job:{rel}"
        if self.g.has(jid):
            return jid
        if not self.files.exists(rel):
            self.g.unresolve(where, f"runs {rel}, which does not exist")
            return None
        tree = self.files.tree(rel)
        self.g.node("Job", jid, path=rel, name=Path(rel).stem, domain=ex.job_domain(rel), kind=ex.job_kind(rel),
                    doc=(ast.get_docstring(tree) or "").split("\n")[0])
        return jid

    # ------------------------------------------------------------------ 1. jobs
    def add_jobs(self) -> None:
        g = self.g
        defaults: dict[str, set] = {}
        for rel, j in self.jobs.items():
            domain, kind, stem, app = ex.job_domain(rel), ex.job_kind(rel), Path(rel).stem, j["app_name"]
            consts = {k: v for k, v in j["consts"].items() if k in JOB_CONSTS}
            g.node("Job", f"job:{rel}", path=rel, name=stem, domain=domain, kind=kind, app_name=app,
                   app_name_matches_stem=None if app is None else app.endswith(stem),
                   app_name_has_domain_prefix=None if app is None else app.startswith(f"{domain}_"),
                   doc=j["doc"], consts=ex.dumps(consts) if consts else None)
            for var, default in j["knobs"].items():
                defaults.setdefault(var, set()).add(ex.canonical_path(default) if "/" in default else default)
        for rel in self.files.glob(spec.DAG_GLOB):   # knobs of the DAG helper modules (LDL_SPARK_CONTAINER)
            env = ex.Env(self.files.tree(rel))
            for var, default in env.knobs.items():
                defaults.setdefault(var, set()).add(default)
        for var, values in sorted(defaults.items()):
            g.node("EnvVar", f"env:{var}", name=var, defaults=",".join(sorted(values)))
        for rel, j in self.jobs.items():
            for var, default in sorted(j["knobs"].items()):
                g.edge("CONFIGURES", f"env:{var}", f"job:{rel}",
                       default_value=ex.canonical_path(default) if "/" in default else default)

    # ------------------------------------------------------------------ 2. churn source / bronze / silver
    def add_churn_tables(self) -> None:
        g = self.g
        j01, j02 = self.jobs[spec.BRONZE_JOB], self.jobs[spec.SILVER_JOB]
        self.bronze = ex.bronze_tables(j01)
        self.silver = ex.silver_tables(j02, self.bronze)
        csv_paths = {p.rsplit("/", 1)[-1]: p for p in j01["csv_in"]}
        meta_cols = [(name, "timestamp" if "timestamp" in expr else "string") for name, expr in j01["columns_added"]
                     if name.startswith("_")]
        dropped = set(j02["consts"].get("LINEAGE") or [])
        for table, info in self.bronze.items():
            if info["csv"] not in csv_paths:
                raise LineageExtractError(f"{spec.BRONZE_JOB}: BRONZE[{table!r}] reads {info['csv']}, but the job "
                                          f"has no spark.read.csv() of that file")
            template = csv_paths[info["csv"]]
            rel = ex.canonical_path(template)
            did = self.dataset(rel, kind="csv", layer="source", domain="churn", path=rel, format="csv",
                               path_template=re.sub(r"\$\{(\w+):-[^}]*\}", r"${\1}", template))
            self.source_files[did] = info["csv"]
            fq = f"lakehouse.bronze.{table}"
            self.iceberg(fq, "churn", grain="one row per source CSV row")
            for i, (name, typ) in enumerate(info["columns"], 1):
                self.column(rel, name, i, layer="source", domain="churn", data_type=typ, role="raw")
                cid = self.column(fq, name, i, layer="bronze", domain="churn", data_type=typ, role="raw")
                g.edge("DERIVED_FROM", cid, column_id(rel, name), roles="VALUE", transform=f"CAST({typ.upper()})",
                       derived_by=f"ast:{spec.BRONZE_JOB} BRONZE")
            for i, (name, typ) in enumerate(meta_cols, len(info["columns"]) + 1):
                self.column(fq, name, i, layer="bronze", domain="churn", data_type=typ, role="lineage_meta")
        for table, info in self.silver.items():
            fq, bfq = f"lakehouse.silver.{table}", f"lakehouse.bronze.{info['bronze']}"
            keys = info["dedupe_keys"] or []
            self.iceberg(fq, "churn", dedupe_keys=",".join(keys) or None,
                         row_filter=" AND ".join(info["filter"]) or None,
                         grain=("one row per " + ", ".join(keys)) if keys else "one row per bronze row")
            for i, (name, typ) in enumerate([c for c in info["columns"] if c[0] not in dropped], 1):
                cid = self.column(fq, name, i, layer="silver", domain="churn", data_type=typ, role="raw")
                expr, sources = info["derived"].get(name, (None, [name]))
                for src in sources:
                    g.edge("DERIVED_FROM", cid, self.need_column(bfq, src, spec.SILVER_JOB), roles="VALUE",
                           transform=expr or "IDENTITY", derived_by=f"ast:{spec.SILVER_JOB} silver_tables")

    # ------------------------------------------------------------------ 3. retail (companion SQL)
    def add_retail_tables(self) -> None:
        g = self.g
        schema: dict = {"lakehouse": {"bronze": {}, "silver": {}, "gold": {}}}
        csv_cols: dict[str, list[str]] = {}
        for rel_job, j in self.jobs.items():
            if not rel_job.startswith("src/jobs/retail/"):
                continue
            for template in j["csv_in"]:
                rel = ex.canonical_path(template)
                if "*" in rel:
                    g.unresolve(rel_job, f"CSV input pattern {template}")
                    continue
                if not self.files.exists(rel):
                    g.unresolve(rel_job, f"reads {rel}, which does not exist")
                    continue
                header = next(csv.reader(io.StringIO(self.files.text(rel))), [])
                did = self.dataset(rel, kind="csv", layer="source", domain="retail", path=rel, format="csv",
                                   path_template=re.sub(r"\$\{(\w+):-[^}]*\}", r"${\1}", template))
                self.source_files[did] = rel.rsplit("/", 1)[-1]
                csv_cols[rel] = header
                for i, name in enumerate(header, 1):
                    self.column(rel, name, i, layer="source", domain="retail", data_type="string", role="raw")
        for rel, mirrors in spec.RETAIL_SQL.items():
            if not self.files.exists(rel):
                continue
            try:
                statements = [s for s in sqlglot.parse(self.files.text(rel), read="spark") if s is not None]
            except SqlglotError as e:
                raise LineageExtractError(f"{rel}: sqlglot cannot parse it ({e})") from e
            sid = g.node("SqlFile", f"sql:{rel}", path=rel, dialect="spark", executed=False,
                         parser=f"sqlglot {sqlglot.__version__}",
                         note="companion SQL contract; the DataFrame job is what runs")
            if g.has(f"job:{mirrors}"):
                g.edge("MIRRORS", sid, f"job:{mirrors}", verified_by="none (no parity check for retail)")
            for st in statements:
                if isinstance(st, exp.Create) and isinstance(st.this, exp.Schema):
                    self._retail_ddl(st, rel, schema, csv_cols, sid)
                elif isinstance(st, exp.Create) and isinstance(st.expression, exp.Query):
                    self._retail_ctas(st, rel, schema, sid)

    def _retail_ddl(self, st: exp.Create, rel: str, schema: dict, csv_cols: dict, sid: str) -> None:
        g = self.g
        t = st.this.this
        fq = f"lakehouse.{t.db}.{t.name}"
        cols = [(c.name, c.args["kind"].sql("spark").lower()) for c in st.this.expressions
                if isinstance(c, exp.ColumnDef)]
        schema["lakehouse"].setdefault(t.db, {})[t.name] = dict(cols)
        self.iceberg(fq, "retail", grain="one row per source CSV row")
        g.edge("DESCRIBES", sid, f"ds:{fq}")
        sources = [p for p in csv_cols if p.rsplit("/", 1)[-1].startswith(t.name.removesuffix("_raw"))]
        if not sources:
            g.warnings.append(f"{rel}: no source CSV matches bronze table {t.name}")
        for i, (name, typ) in enumerate(cols, 1):
            cid = self.column(fq, name, i, layer=t.db, domain="retail", data_type=typ,
                              role="lineage_meta" if name.startswith("_") else "raw")
            for src in sources:
                if name in csv_cols[src]:
                    g.edge("DERIVED_FROM", cid, column_id(src, name), roles="VALUE", transform=f"CAST({typ.upper()})",
                           derived_by=f"sqlglot:{rel}")

    def _retail_ctas(self, st: exp.Create, rel: str, schema: dict, sid: str) -> None:
        g = self.g
        t, sel = st.this, st.expression
        fq = f"lakehouse.{t.db}.{t.name}"
        window = sel.find(exp.Window)
        group = sel.args.get("group")
        if group is not None:
            grain = "one row per " + ", ".join(e.sql("spark") for e in group.expressions)
        elif window is not None and window.args.get("partition_by"):
            grain = "one row per " + ", ".join(e.sql("spark") for e in window.args["partition_by"])
        else:
            grain = None
        self.iceberg(fq, "retail", grain=grain)
        g.edge("DESCRIBES", sid, f"ds:{fq}")
        try:
            nodes = sqlglot_lineage(None, sel, schema=schema, dialect="spark")
        except SqlglotError as e:
            raise LineageExtractError(f"{rel}: sqlglot lineage() failed for {fq} ({e})") from e
        names = list(sel.named_selects)
        for i, col in enumerate(names, 1):
            role = "lineage_meta" if col.startswith("_") else (
                "metric" if t.db == "gold" and group is not None and i > len(group.expressions) else "raw")
            cid = self.column(fq, col, i, layer=t.db, domain="retail", role=role)
            expr = next(p for p in sel.selects if p.alias_or_name == col).sql("spark")
            leaves = set()
            for n in nodes[col].walk():
                if not n.downstream and isinstance(n.source, exp.Table):
                    leaves.add((f"lakehouse.{n.source.db}.{n.source.name}", n.name.split(".")[-1]))
            if not leaves:
                if "COUNT(*)" not in expr.upper():
                    raise LineageExtractError(f"{rel}: output column {fq}.{col} has no source column")
                src = next(sel.find_all(exp.Table))
                g.edge("COUNTS_ROWS_OF", cid, f"ds:lakehouse.{src.db}.{src.name}", window="unbounded",
                       derived_by=f"sqlglot:{rel}", note="sqlglot lineage() returns a sourceless node for COUNT(*)")
            for table, c in sorted(leaves):
                g.edge("DERIVED_FROM", cid, self.need_column(table, c, rel), roles="VALUE", transform=expr[:300],
                       derived_by=f"sqlglot:{rel} (companion SQL, not executed)")
        schema["lakehouse"].setdefault(t.db, {})[t.name] = {c: "string" for c in names}

    # ------------------------------------------------------------------ 4. churn gold (scope walk)
    def add_gold(self) -> None:
        g = self.g
        sql_text = self.files.text(spec.GOLD_SQL)
        j03 = self.jobs[spec.GOLD_JOB]
        substitution = j03.get("sql_substitution") or {}
        try:
            sql = string.Template(sql_text).substitute(**substitution)
        except (KeyError, ValueError) as e:
            raise LineageExtractError(f"{spec.GOLD_SQL}: placeholder {e} is not given a default by "
                                      f"{spec.GOLD_JOB} (gold_sql(<name>='lakehouse.<namespace>'))") from e
        gl = GoldLineage(sql, ex.silver_schema(self.silver), source=spec.GOLD_SQL)
        self.gl = gl
        rule = gl.pit_rule(sql_text)
        self.pit = rule
        fq = spec.GOLD_TABLE
        written = [w for w in j03["writes"] if w[0] == fq]
        if not written:
            raise LineageExtractError(f"{spec.GOLD_JOB}: does not write {fq}")
        self.iceberg(fq, "churn", grain="one row per renewal (subscription) at its T-"
                                        f"{-rule['as_of_offset_days']} snapshot")
        ctes = gl.ctes()
        sid = g.node("SqlFile", f"sql:{spec.GOLD_SQL}", path=spec.GOLD_SQL, dialect="spark", executed=True,
                     executed_by=spec.GOLD_JOB, placeholders=",".join(sorted(substitution)) or None,
                     parser=f"sqlglot {sqlglot.__version__}", qualify_ok=True, n_ctes=len(ctes))
        g.edge("DESCRIBES", sid, f"ds:{fq}")
        features_rule = g.node(
            "PointInTimeRule", "pit:features_le_as_of", name="features read events dated on or before as_of",
            as_of_expr=rule["as_of_expr"], renewal_expr=rule["renewal_expr"],
            as_of_offset_days=rule["as_of_offset_days"], predicate=rule["predicate"],
            source=f"{spec.GOLD_SQL}:{rule['source_line']}",
            statement=f"as_of = {rule['renewal_expr']} - {-rule['as_of_offset_days']} days (the T-"
                      f"{-rule['as_of_offset_days']} snapshot); every feature window's upper bound must be <= as_of")
        g.node("PointInTimeRule", "pit:outcome_after_renewal", name="outcome / label read billing events around "
                                                                    "the renewal date",
               as_of_offset_days=rule["as_of_offset_days"], source=f"{spec.GOLD_SQL} (billing CTE)",
               statement="outcome, route and churned read subscription events and invoices after as_of; they must "
                         "never feed a feature or the serve record")
        for c in ctes:
            cid = g.node("Cte", f"cte:{spec.GOLD_SQL}#{c['name']}", name=c["name"], sql_file=spec.GOLD_SQL,
                         sql=c["sql"][:4000])
            g.edge("DEFINES", sid, cid)
        tables_read = set()
        for c in ctes:
            for dep in c["depends_on"]:
                if not dep.startswith(("derived:", "subquery:")):
                    g.edge("DEPENDS_ON", f"cte:{spec.GOLD_SQL}#{c['name']}", f"cte:{spec.GOLD_SQL}#{dep}")
            for alias, table, w in c["tables"]:
                tables_read.add(table)
                g.edge("READS", f"cte:{spec.GOLD_SQL}#{c['name']}", f"ds:{table}", alias=alias,
                       window=format_window(w))
        for table in sorted(tables_read):
            g.edge("READS", sid, f"ds:{table}")
            g.edge("READS", f"job:{spec.GOLD_JOB}", f"ds:{table}", via="sql_file", purpose="input")
        g.edge("EXECUTES_SQL", f"job:{spec.GOLD_JOB}", sid, substitution=ex.dumps(substitution),
               path_template=j03["sql_file"])
        self._gold_columns(gl, features_rule)
        g.edge("ROW_GRAIN_FROM", f"ds:{fq}", f"ds:{spec.SILVER_NAMESPACE}.churn_subscription_snapshots",
               predicate=rule["predicate"], rule="pit:features_le_as_of")
        for wid in g.ids("Window"):
            g.edge("RELATIVE_TO", wid, "pit:features_le_as_of")

    def _window(self, w) -> str:
        g = self.g
        display = format_window(w)
        if w is None:
            return g.node("Window", "win:unbounded", display="all history (no time filter)", anchor="none",
                          reads_after_as_of=True)
        lo, lo_incl, hi, hi_incl = w

        def days(v):
            return None if v in (INF, -INF) else int(v)
        return g.node("Window", "win:" + display.replace(" ", ""), display=display, anchor="as_of",
                      lower_offset_days=days(lo), lower_inclusive=lo_incl, upper_offset_days=days(hi),
                      upper_inclusive=hi_incl, length_days=None if INF in (abs(lo), abs(hi)) else int(hi - lo),
                      reads_after_as_of=hi > 0)

    @staticmethod
    def _clamp(e) -> tuple[float, float] | None:
        """least(greatest(x, lo), hi) -> (lo, hi)."""
        for least in e.find_all(exp.Least):
            greatest = least.this if isinstance(least.this, exp.Greatest) else None
            if greatest is None or not least.expressions or not greatest.expressions:
                continue
            lo, hi = ex_number(greatest.expressions[0]), ex_number(least.expressions[0])
            if lo is not None and hi is not None:
                return lo, hi
        return None

    @staticmethod
    def _window_length(col: ColumnLineage, final) -> int | None:
        """COUNT(DISTINCT <date>) inside one bounded window of L days is structurally in [0, L]."""
        inner = final.this if isinstance(final, exp.Alias) else final
        if not isinstance(inner, exp.Cast):
            return None
        values = [x for x in col.leaves if x.role == "VALUE"]
        if len(values) == 1 and values[0].column.endswith("_date") and values[0].window and values[0].cte_expr \
                and "COUNT(DISTINCT" in values[0].cte_expr.upper():
            lo, _, hi, _ = values[0].window
            if INF not in (abs(lo), abs(hi)):
                return int(hi - lo)
        return None

    def _gold_columns(self, gl: GoldLineage, features_rule: str) -> None:
        g = self.g
        fq = spec.GOLD_TABLE
        contract = self.facts["export_contract"]
        train = contract["train_columns"]
        serve = self.jobs[spec.EXPORT_JOB]["records"]
        serve_keys = next(iter(serve.values())) if len(serve) == 1 else None
        if serve_keys is None:
            raise LineageExtractError(f"{spec.EXPORT_JOB}: the serve record ({{c: ... for c in TRAIN_COLUMNS if "
                                      f"c != <label>}}) was not found")
        self.serve_keys = serve_keys
        labels = [c for c in train if c not in serve_keys]
        features = [c for c in serve_keys if c not in GOLD_ROLES]
        if features != list(gspec.GOLD_FEATURES):
            raise LineageExtractError(
                f"the feature list of {spec.EXPORT_CONTRACT} / {spec.EXPORT_JOB} ({len(features)}) differs from "
                f"lakehouse_graph.spec.GOLD_FEATURES ({len(gspec.GOLD_FEATURES)}): update spec.py")
        ranges = {c: (lo, hi) for c, lo, hi in contract["ranges"]}
        leaky = set(contract["leaky"])
        finals = gl.final_selects()
        columns = gl.columns()
        for col in columns:
            name = col.name
            role = "feature" if name in features else "label" if name in labels else GOLD_ROLES.get(name, "attribute")
            label_side = any((x.cte or "").startswith("subquery:") or x.cte == "today" for x in col.leaves)
            max_hi = column_upper(col.leaves)
            if role == "feature":
                declared = gspec.FEATURE_CARDS.get(name, {}).get("pit_status")
                status = spec.PIT_COMPLIANT if max_hi <= 0 else (
                    spec.PIT_DECLARED_EXCEPTION if declared == spec.PIT_DECLARED_EXCEPTION
                    else spec.PIT_UNDECLARED_EXCEPTION)
            elif label_side:
                status = spec.PIT_LABEL_SIDE
            else:
                status = spec.PIT_AS_OF_ROW
            clamp = self._clamp(finals[name])
            rng = ranges.get(name)
            guarantee = None
            if rng is not None:
                length = self._window_length(col, finals[name])
                if clamp and clamp[0] >= rng[0] and clamp[1] <= rng[1]:
                    guarantee = "sql_clamp"
                elif length is not None and rng[0] <= 0 and length <= rng[1]:
                    guarantee = f"window_length({length}d)"
                else:
                    guarantee = "data_dependent"
            enum = None
            inner = finals[name].this if isinstance(finals[name], exp.Alias) else finals[name]
            if isinstance(inner, exp.Column) and inner.table in gl.cte_scopes:   # outcome: defined in a CTE
                inner = gl.cte_projection(inner.table, inner.name)
                inner = inner.this if isinstance(inner, exp.Alias) else inner
            if isinstance(inner, exp.Case):
                results = [i.args.get("true") for i in inner.args.get("ifs") or []] + [inner.args.get("default")]
                if all(isinstance(r, exp.Literal) and r.is_string for r in results if r is not None):
                    enum = ",".join(sorted({r.name for r in results if r is not None}))
            if name == "plan_tier":
                enum = ",".join(contract["plan_tiers"])
            cid = self.column(
                fq, name, col.ordinal, layer="gold", domain="churn", role=role, expr_sql=col.final_sql[:2000],
                pit_status=status, max_upper_vs_as_of_days=None if max_hi == INF else int(max_hi),
                reads_after_as_of=max_hi > 0, enum_values=enum,
                sql_clamp_min=clamp[0] if clamp else None, sql_clamp_max=clamp[1] if clamp else None,
                contract_min=rng[0] if rng else None, contract_max=rng[1] if rng else None,
                range_guarantee=guarantee, in_train_export=name in train, declared_leaky=name in leaky,
                sqlglot_unresolved=";".join(col.sqlglot_unresolved) or None,
                sqlglot_sources=";".join(sorted({f"{t}.{c}" for t, c in col.sqlglot_leaves})) or None)
            self.gold_columns.append(name)
            self.gold_role[name] = role
            self._gold_edges(col, cid)
            g.edge("SUBJECT_TO", cid, "pit:outcome_after_renewal" if status == spec.PIT_LABEL_SIDE else features_rule,
                   status=status, max_upper_vs_as_of_days=None if max_hi == INF else int(max_hi))
        # columns the job adds after the SQL (built_at)
        j03 = self.jobs[spec.GOLD_JOB]
        for name, expr in j03["columns_added"]:
            if name in self.gold_columns:
                continue
            cid = self.column(fq, name, len(self.gold_columns) + 1, layer="gold", domain="churn",
                              role="lake_metadata", data_type="timestamp" if "timestamp" in expr else None,
                              expr_sql=f"{expr[:200]} ({spec.GOLD_JOB})", pit_status=spec.PIT_NOT_APPLICABLE,
                              reads_after_as_of=False, in_train_export=name in train, declared_leaky=name in leaky)
            g.edge("PRODUCED_BY", cid, f"job:{spec.GOLD_JOB}", transform=expr[:300])
            self.gold_columns.append(name)
            self.gold_role[name] = "lake_metadata"

    def _gold_edges(self, col: ColumnLineage, cid: str) -> None:
        g = self.g
        derived_by = f"sqlglot {sqlglot.__version__} qualify + scope walk"
        merged: dict[tuple, dict] = {}
        for x in col.leaves:
            if x.column == "*":
                wid = self._window(x.window)
                g.edge("COUNTS_ROWS_OF", cid, f"ds:{x.table}", cte=x.cte, window=format_window(x.window),
                       derived_by="scope walk (sqlglot lineage() returns a sourceless node for COUNT(*))")
                g.edge("USES_WINDOW", cid, wid, source_dataset=x.table, via_cte=x.cte)
                continue
            # one edge per (source column, CTE, row window): the same column read through two CASEs with
            # different bounds is two reads, and the loosest decides the point-in-time status
            m = merged.setdefault((x.table, x.column, x.cte, x.window, x.matched),
                                  {"roles": set(), "post": set(), "expr": x.cte_expr, "path": x.path})
            m["roles"].add(x.role)
            m["post"] |= set(x.post_cmp)
            if x.cte_expr and not m["expr"]:
                m["expr"] = x.cte_expr
        for (table, column, cte, window, matched), m in sorted(merged.items(),
                                                              key=lambda kv: tuple(str(p) for p in kv[0])):
            dst = self.need_column(table, column, f"{spec.GOLD_SQL} column {col.name}")
            in_renewal_row = cte == "renewals"
            g.edge("DERIVED_FROM", cid, dst, roles=",".join(sorted(m["roles"])), cte=cte,
                   window=None if in_renewal_row else format_window(window),
                   matched_window=format_window(matched) if window is None and matched is not None else None,
                   post_agg_compare=format_bounds(tuple(m["post"])), transform=(m["expr"] or "")[:300] or None,
                   path=">".join(m["path"]), derived_by=derived_by)
            # a declared reference table matched to bounded event rows has no window of its own
            dimension = window is None and matched is not None and table in spec.GLOBAL_DIMENSION_TABLES
            if dimension:
                g.node("Dataset", f"ds:{table}", reference_data=spec.GLOBAL_DIMENSION_TABLES[table])
            if not in_renewal_row and cte != "today" and not dimension and m["roles"] & {"VALUE", "WINDOW_BOUND"}:
                g.edge("USES_WINDOW", cid, self._window(window), source_dataset=table,
                       event_column=column if "WINDOW_BOUND" in m["roles"] else None, via_cte=cte)
        ctes = sorted({x.cte for x in col.leaves if x.cte})
        for cte in ctes:
            if cte.startswith("subquery:"):   # scalar subqueries live in the CTE that selects them
                owner = next((p for p in reversed(next(x.path for x in col.leaves if x.cte == cte))
                              if g.has(f"cte:{spec.GOLD_SQL}#{p}")), None)
                if owner:
                    g.edge("COMPUTED_IN", cid, f"cte:{spec.GOLD_SQL}#{owner}", subquery=cte.split(":", 1)[1])
            elif g.has(f"cte:{spec.GOLD_SQL}#{cte}"):
                g.edge("COMPUTED_IN", cid, f"cte:{spec.GOLD_SQL}#{cte}")

    # ------------------------------------------------------------------ 5. parameters
    def add_parameters(self) -> None:
        g = self.g
        gold = spec.GOLD_TABLE
        p = self.facts["parameters"]
        sql_allow, sql_cut, used_by = None, None, None
        for name, e in self.gl.final_selects().items():
            for case in e.find_all(exp.Case):
                ifs = case.args.get("ifs") or []
                if case.this is not None and ifs and all(
                        isinstance(i.this, exp.Literal) and ex_number(i.args.get("true")) is not None for i in ifs):
                    sql_allow, used_by = {i.this.name: ex_number(i.args["true"]) for i in ifs}, name
            for power in e.find_all(exp.Pow):
                if ex_number(power.this) is not None:
                    sql_cut = ex_number(power.this)
        if not sql_allow or sql_cut is None:
            raise LineageExtractError(f"{spec.GOLD_SQL}: the plan allowance CASE (CASE plan_tier WHEN '<tier>' THEN n) "
                                      f"or the cap-cut pow(<x>, cuts_so_far) was not found in the final SELECT")
        twin, gen = p["twin"], p["generator"]
        sources = f"{spec.GOLD_SQL}; {spec.PANDAS_TWIN}; {spec.GENERATOR}"

        def consistent(*values):
            known = [float(v) for v in values if v is not None]
            return len(known) == len(values) and len(set(known)) == 1

        for tier in sorted(sql_allow):
            values = (sql_allow[tier], (twin["allowance"] or {}).get(tier), (gen["allowance"] or {}).get(tier))
            pid = g.node("Parameter", f"param:allowance.{tier}", name=f"allowance.{tier}", sql_value=values[0],
                         pandas_value=values[1], generator_value=values[2], consistent=consistent(*values),
                         sources=sources)
            g.edge("USED_BY", pid, column_id(gold, used_by))
        values = (sql_cut, twin["cap_cut"], gen["cap_cut"])
        pid = g.node("Parameter", "param:cap_cut", name="cap_cut", sql_value=values[0], pandas_value=values[1],
                     generator_value=values[2], consistent=consistent(*values), sources=sources)
        g.edge("USED_BY", pid, column_id(gold, used_by))
        values = (-self.pit["as_of_offset_days"], twin["as_of_offset_days"], gen["as_of_offset_days"])
        pid = g.node("Parameter", "param:as_of_offset_days", name="as_of_offset_days", sql_value=values[0],
                     pandas_value=values[1], generator_value=values[2], consistent=consistent(*values),
                     sources=sources)
        g.edge("USED_BY", pid, "pit:features_le_as_of")
        if twin["dunning_days"] is not None:
            g.node("Parameter", "param:dunning_days", name="dunning_days", pandas_value=twin["dunning_days"],
                   sources=f"{spec.PANDAS_TWIN} (defined{'' if twin['dunning_days_reads'] else ', never read'})",
                   unused=twin["dunning_days_reads"] == 0)

    # ------------------------------------------------------------------ 6. exports
    def add_exports(self) -> None:
        g = self.g
        j04 = self.jobs[spec.EXPORT_JOB]
        gold = spec.GOLD_TABLE
        contract = self.facts["export_contract"]
        guarded = self._guarded_exports()
        if not j04["files_out"]:
            raise LineageExtractError(f"{spec.EXPORT_JOB}: writes no export file the extractor recognises")
        for template, _how, rows_var, cols in j04["files_out"]:
            rel = ex.canonical_path(template)
            name = rel.rsplit("/", 1)[-1]
            columns = list(cols) if cols else (self.serve_keys if name.endswith(".json") else None)
            if columns is None:
                raise LineageExtractError(f"{spec.EXPORT_JOB}: the column list of {name} is not a literal")
            eid = g.node("Export", f"export:{rel}", name=name, ref=dataset_ref(rel), path=rel,
                         path_template=re.sub(r"\$\{(\w+):-[^}]*\}", r"${\1}", template),
                         format=name.rsplit(".", 1)[-1],
                         row_filter=j04["row_filters"].get(rows_var) if rows_var else (
                             j04["row_filters"].get("hero") if name.endswith(".json") else None),
                         n_columns=len(columns), column_order=",".join(columns),
                         contract=spec.EXPORT_CONTRACT if name in guarded else None,
                         consumed_by_radar=name in guarded)
            self.exports[name] = rel
            self.export_columns[name] = columns
            self.source_files[eid] = name
            for i, c in enumerate(columns, 1):
                src = self.need_column(gold, c, f"{spec.EXPORT_JOB} export {name}")
                cid = self.column(rel, c, i, layer="export", domain="churn", container=eid, role=self.gold_role[c])
                g.edge("DERIVED_FROM", cid, src, roles="VALUE", transform="IDENTITY (dates and timestamps as ISO text)",
                       derived_by=f"ast:{spec.EXPORT_JOB}")
            for c in self.gold_columns:
                if c in columns:
                    continue
                role = self.gold_role[c]
                if role == "label":
                    reason = "label: read from billing events after as_of; the serve record's renewal has not happened"
                elif role in ("outcome", "routing"):
                    reason = "label-derived: read from billing events after as_of"
                elif role == "time_anchor":
                    reason = "time anchor: identifies the as-of date, not a behaviour; absent from the radar schema"
                elif role == "lake_metadata":
                    reason = f"lake metadata: added by {spec.GOLD_JOB}"
                else:
                    reason = "not a radar feature; a static attribute of the snapshot row"
                g.edge("EXCLUDED_FROM", column_id(gold, c), eid, reason=reason, declared_leaky=c in contract["leaky"])

    def _guarded_exports(self) -> set[str]:
        """File names the export contract requires to exist (its files-exist loop)."""
        contract = self.facts["export_contract"]
        names = set()
        for chk in contract["checks"]:
            if ".exists()" in chk["test"] and chk["guards"] and chk["guards"][0].startswith("for "):
                for var in re.findall(r"\b(\w+_path)\b", chk["guards"][0]):
                    pattern = contract["path_vars"].get(var)
                    if pattern is None:
                        self.g.unresolve(f"{spec.EXPORT_CONTRACT}:{chk['lineno']}", f"path variable {var}")
                    else:
                        names.add(pattern.rsplit("/", 1)[-1])
        return names

    # ------------------------------------------------------------------ 7. business-graph bridge
    def add_graph_bridge(self) -> None:
        g = self.g
        self.files.note(spec.GRAPH_SPEC, gspec.__file__)
        bridge = spec.graph_bridge(gspec)
        want = {("node", label) for label in gspec.NODE_SCHEMA} | {("edge", rel) for rel in gspec.EDGE_SCHEMA}
        if set(bridge) != want:
            raise LineageExtractError(
                f"lineage/spec.graph_bridge() covers {len(bridge)} graph elements but lakehouse_graph.spec has "
                f"{len(want)}: missing {sorted(want - set(bridge))}, unknown {sorted(set(bridge) - want)}")
        refs = {n["props"]["ref"]: nid for nid, n in g.nodes.items()
                if n["label"] == "DataColumn" and n["props"].get("ref")}
        verified = {c["backing_edge"]: f for f, c in gspec.FEATURE_CARDS.items()
                    if c.get("verification") == "graph-verified" and c.get("backing_edge")}
        self.graph_datasets: list[str] = []
        twins = self._published_twins()
        for (kind, name), decl in bridge.items():
            es = gspec.NODE_SCHEMA[name] if kind == "node" else gspec.EDGE_SCHEMA[name]
            window = gspec.PIT_WINDOWS.get(name) if kind == "edge" else None
            gid = g.node(
                "GraphElement", f"ge:{kind}:{name}", kind=kind, name=name, spec_version=gspec.GRAPH_SPEC_VERSION,
                key=es.key if kind == "node" else None, src_label=getattr(es, "src", None),
                dst_label=getattr(es, "dst", None), n_properties=len(es.columns),
                feeds_feature=window.feature if window else None, pit_window_days=window.days if window else None,
                pit_note=window.note if window else None,
                verification="graph-verified" if name in verified else None, note=decl["note"])
            datasets = []
            for ref in decl["sources"]:
                if ref not in refs:
                    raise LineageExtractError(f"lineage/spec.graph_bridge(): {kind} {name} is sourced from {ref}, "
                                              f"which is not a column in the lineage graph (an unresolved name)")
                g.edge("SOURCED_FROM", gid, refs[ref], via="pandas twin silver() / gold()")
                ds = "ds:" + g.props(refs[ref])["dataset"]
                if ds not in datasets:
                    datasets.append(ds)
            for ds in datasets:
                g.edge("SOURCED_FROM", gid, ds, via="pandas twin silver() / gold()")
            table = self.dataset(spec.graph_dataset(es.file), kind="parquet", layer="graph", domain="graph",
                                 format="parquet", path=f"{spec.GRAPH_BUILD_PATH}/parquet/{es.file}",
                                 path_template=f"${{GRAPH_ROOT}}/<profile>/builds/<id>/parquet/{es.file}",
                                 declared_by="lakehouse_graph.spec", grain=f"one row per {name}")
            g.edge("MATERIALIZED_AS", gid, table)
            self.graph_datasets.append(table)
            self._graph_twin(es.file, table, twins)
        for file, grain in ((gspec.SCALER_FILE, "one row per SIMILAR_TO feature (persisted z-score scaler)"),
                            ("graph.lbdb", "Ladybug projection of the Parquet graph (rebuildable)")):
            self.graph_datasets.append(self.dataset(
                f"graph/{file}", kind="parquet" if file.endswith(".parquet") else "lbdb", layer="graph",
                domain="graph", format=file.rsplit(".", 1)[-1], declared_by="lakehouse_graph.spec", grain=grain,
                path=f"{spec.GRAPH_BUILD_PATH}/{file}", path_template=f"${{GRAPH_ROOT}}/<profile>/builds/<id>/{file}"))
        # the scaler's twin is optional (the Spark job may refit it): mirrored only when a job publishes one
        self._graph_twin(gspec.SCALER_FILE, f"ds:graph/{gspec.SCALER_FILE}", twins, required=False)
        for fq in sorted(twins["unmatched"]):   # published graph_* tables that mirror no graph Parquet table
            self.iceberg(fq, "graph", declared_by="job reference", status="published")
        self.lineage_datasets = [
            self.dataset(f"graph/{spec.LINEAGE_DIR}", kind="parquet_dir", layer="graph", domain="graph",
                         format="parquet", declared_by="lakehouse_graph.lineage.spec",
                         grain="lineage node and edge tables (this graph)",
                         path_template=f"${{GRAPH_ROOT}}/<profile>/builds/<id>/{spec.LINEAGE_DIR}/"),
            self.dataset(f"graph/{spec.DB_FILE}", kind="lbdb", layer="graph", domain="graph", format="lbdb",
                         declared_by="lakehouse_graph.lineage.spec", grain="Ladybug projection of the lineage Parquet",
                         path_template=f"${{GRAPH_ROOT}}/<profile>/builds/<id>/{spec.DB_FILE}")]

    def _published_twins(self) -> dict:
        """Iceberg ``lakehouse.gold.graph_*`` tables the jobs write by exact name, keyed by a
        normalised table name (so graph_renewal, graph_nodes_renewal and graph_node_renewal all
        match nodes_Renewal). ``graph_nodes`` / ``graph_edges`` are union twins: one table for all
        node (edge) tables, partitioned by the label (edge type) column the writing job names in
        ``partitionedBy``. No exact name (no Spark graph job yet, or names built in a loop): the
        twins are declared by this spec's naming convention and marked planned."""
        names = sorted({name for j in self.jobs.values() for name, _mode in j["writes"]
                        if name.startswith(spec.GRAPH_TWIN_PREFIX) and "*" not in name})
        partitions = {name: cols for j in self.jobs.values() for name, cols in j.get("partitions", {}).items()}
        by_key = {_twin_key(n[len(spec.GRAPH_TWIN_PREFIX):]): n for n in names}
        union = {spec.GRAPH_UNION_TWINS[k]: by_key.pop(k) for k in sorted(by_key) if k in spec.GRAPH_UNION_TWINS}
        parity = f"job:{spec.GRAPH_PARITY}"
        return {"by_key": by_key, "union": union, "partitions": partitions, "unmatched": set(names),
                "verified_by": spec.GRAPH_PARITY if self.g.has(parity) else
                "graph parity check (when the Spark graph job runs)"}

    def _graph_twin(self, file: str, table: str, twins: dict, required: bool = True) -> None:
        g = self.g
        stem = file.rsplit(".", 1)[0]
        actual, partition = twins["by_key"].get(_twin_key(stem)), None
        kind, _, value = stem.partition("_")
        if actual is None and required and kind in twins["union"]:
            actual = twins["union"][kind]
            cols = twins["partitions"].get(actual) or []
            if len(cols) != 1:
                g.warnings.append(f"{actual} holds every {kind[:-1]} table but its writer names no single partition "
                                  f"column (partitionedBy: {cols or 'none'}): the rows of {file} are not identified")
            else:
                partition = f"{cols[0]} = '{value}'"
        if actual is None and (twins["by_key"] or twins["union"]):
            if required:
                g.warnings.append(f"the Spark graph job publishes no Iceberg twin for {file}")
            return
        if actual is None and not required:
            return
        twin = self.iceberg(actual or spec.graph_twin_table(file), "graph",
                            declared_by="job reference" if actual else "lineage/spec.graph_twin_table",
                            status="published" if actual else "planned",
                            note="Iceberg twin of the graph Parquet table (Spark graph job)" if partition is None
                            else f"Iceberg twin of every graph {kind[:-1]} table, one partition per table")
        twins["unmatched"].discard(actual)
        g.edge("MIRRORS", twin, table, verified_by=twins["verified_by"], partition=partition)

    def add_other_sql(self) -> None:
        """SQL files the extractor has no column model for (sql/graph/*.sql, ...): a SqlFile node
        with the lakehouse tables it names, so a new file is never invisible. ``$name`` placeholders
        follow the repo convention ``$silver`` -> ``lakehouse.silver``."""
        g = self.g
        for rel in self.files.glob("sql/**/*.sql"):
            if g.has(f"sql:{rel}"):
                continue
            sid = g.node("SqlFile", f"sql:{rel}", path=rel, dialect="spark", parser=f"sqlglot {sqlglot.__version__}",
                         note="not modelled column by column: only the tables it names")
            tables = ex.sql_tables(re.sub(r"\$\{?(\w+)\}?", r"lakehouse.\1", self.files.text(rel)))
            if tables is None:
                g.warnings.append(f"{rel}: sqlglot cannot parse it; the file is in the graph without table edges")
                continue
            domain = ex.job_domain(rel)
            for name in sorted(tables[0]):
                for d in self._table_ids(name.partition("#")[0], rel, domain):
                    g.edge("READS", sid, d)
            for name in sorted({n for n, _kind in tables[1] if n.startswith("lakehouse.")}):
                for d in self._table_ids(name, rel, domain):
                    g.edge("DESCRIBES", sid, d)
        for rel, j in self.jobs.items():   # SQL_DIR + SQL_FILES (the Spark graph job runs sql/graph/*.sql)
            for path in j.get("sql_files", []):
                sql_rel = ex.canonical_path(path)
                if g.has(f"sql:{sql_rel}"):
                    g.edge("EXECUTES_SQL", f"job:{rel}", f"sql:{sql_rel}", path_template=path)
                else:
                    g.unresolve(rel, f"executes {path} ({sql_rel}), which is not a SQL file of the repo")

    def add_graph_scripts(self) -> None:
        """READS / WRITES of the graph and lineage scripts (they work through the library, so
        their I/O is declared here; the twin import is read from lakehouse_graph/build.py)."""
        g = self.g
        sources = [i for i in g.ids("Dataset") if g.props(i)["layer"] == "source" and g.props(i)["domain"] == "churn"]
        build_job, check_job = f"job:{spec.GRAPH_BUILD_SCRIPT}", f"job:{spec.GRAPH_CONTRACT}"
        if g.has(build_job):
            from .. import build as gbuild  # noqa: I001 (lazy: pandas; the value the builder really uses)

            self.files.note("src/lakehouse_graph/build.py", gbuild.__file__)
            twin = getattr(gbuild, "GOLD_SCRIPT", None)
            if twin and g.has(f"job:{twin}"):
                g.edge("IMPORTS", build_job, f"job:{twin}", via="lakehouse_graph.build.load_gold_twin")
            else:
                g.unresolve("src/lakehouse_graph/build.py", "GOLD_SCRIPT (the gold twin the graph builder imports)")
            for ds in sources:
                g.edge("READS", build_job, ds, via="pandas twin silver()", purpose="input")
            for ds in self.graph_datasets:
                g.edge("WRITES", build_job, ds, mode="atomic build dir swap")
        if g.has(check_job):
            for ds in self.graph_datasets:
                g.edge("READS", check_job, ds, via="pyarrow / Ladybug read-only", purpose="validate")
            for name in ("churn_renewals_audit.csv", "churn_user_features.csv", "hero_inference_record.json"):
                if name in self.exports:
                    g.edge("READS", check_job, f"export:{self.exports[name]}", via="pandas", purpose="validate")
        if g.has(f"job:{spec.LINEAGE_BUILD_SCRIPT}"):
            for ds in self.lineage_datasets:
                g.edge("WRITES", f"job:{spec.LINEAGE_BUILD_SCRIPT}", ds, mode="atomic swap inside the build dir")
        if g.has(f"job:{spec.LINEAGE_CONTRACT}"):
            for ds in self.lineage_datasets:
                g.edge("READS", f"job:{spec.LINEAGE_CONTRACT}", ds, via="pyarrow / Ladybug read-only",
                       purpose="validate")

    # ------------------------------------------------------------------ 8. job I/O
    def _table_ids(self, name: str, where: str, domain: str) -> list[str]:
        """Dataset ids of a (possibly ``*``-patterned) lakehouse table name; unknown exact names
        become Dataset nodes (declared_by 'job reference'), unknown patterns are unresolved."""
        g = self.g
        known = {g.props(i)["name"]: i for i in g.ids("Dataset") if g.props(i)["kind"] == "iceberg_table"}
        hits = ex.match_names(name, known)
        if hits:
            return [known[h] for h in hits]
        if "*" in name:
            g.unresolve(where, f"table name pattern {name} matches no dataset")
            return []
        if not name.startswith("lakehouse.") or name.count(".") != 2:
            g.unresolve(where, f"table name {name} is not lakehouse.<namespace>.<table>")
            return []
        g.warnings.append(f"{where}: {name} is referenced by a job but no schema source declares it "
                          f"(Dataset added without columns)")
        return [self.iceberg(name, domain, declared_by="job reference")]

    def _file_ids(self, path: str) -> list[str]:
        g = self.g
        rel = ex.canonical_path(path)
        known = {g.props(i)["path"]: i for i in g.ids("Dataset") + g.ids("Export") if g.props(i).get("path")}
        return [known[h] for h in ex.match_names(rel, known)]

    def add_job_io(self) -> None:
        g = self.g
        for rel, j in self.jobs.items():
            jid, domain, strict = f"job:{rel}", ex.job_domain(rel), rel.startswith("src/jobs/")
            for table, cols in j["ddl"].items():   # tables only a job's own DDL declares (smoke_demo)
                if not g.has(f"ds:{table}"):
                    self.iceberg(table, domain, declared_by=f"DDL in {rel}", is_demo="demo" in table or None)
                    for i, (name, typ) in enumerate(cols, 1):
                        self.column(table, name, i, layer=table.split(".")[1], domain=domain, data_type=typ,
                                    role="raw")
            for name, via, purpose in j["reads"]:
                base, _, meta = name.partition("#")
                for d in self._table_ids(base, rel, domain):
                    g.edge("READS", jid, d, via=via, purpose=purpose, iceberg_metadata=meta or None)
            for path in j["csv_in"]:
                ids = self._file_ids(path)
                if not ids and strict:
                    g.unresolve(rel, f"input file {path} is not a known dataset")
                for d in ids:
                    g.edge("READS", jid, d, via="csv", purpose="input", path_template=path)
            for name, mode in j["writes"]:
                for d in self._table_ids(name, rel, domain):
                    g.edge("WRITES", jid, d, mode=mode)
            written = set()
            for path, how, rows_var, _cols in j["files_out"]:
                for d in self._file_ids(path):
                    written.add(d)
                    g.edge("WRITES", jid, d, mode=how, path_template=path,
                           row_filter=j["row_filters"].get(rows_var) if rows_var else None)
            for ref in j["file_refs"]:
                for d in self._file_ids(ref):
                    if d not in written:
                        g.edge("READS", jid, d, via="pandas / json",
                               purpose="validate" if ex.job_kind(rel) == "check" else "input")
            for imp in j["imports"]:
                if g.has(f"job:{imp}"):
                    g.edge("IMPORTS", jid, f"job:{imp}", via="importlib (by file path)")
                else:
                    g.unresolve(rel, f"imports {imp}, which is not a job")
            for attr, kws in j["kw_calls"]:   # parity: gold_job.gold_sql(silver="global_temp")
                for imp in j["imports"]:
                    other = self.jobs.get(imp, {})
                    if other.get("sql_file") and attr in ex.Env(self.files.tree(imp)).funcs:
                        sql_rel = ex.canonical_path(other["sql_file"])
                        if g.has(f"sql:{sql_rel}"):
                            g.edge("EXECUTES_SQL", jid, f"sql:{sql_rel}", substitution=ex.dumps(kws),
                                   path_template=other["sql_file"])
        # dataset write modes, from the jobs that write them
        modes: dict[str, list[str]] = {}
        for e in g.edges:
            if e["rel"] == "WRITES" and g.label(e["dst"]) == "Dataset" and e["props"].get("mode"):
                seen = modes.setdefault(e["dst"], [])
                for m in e["props"]["mode"].split(","):
                    if m not in seen:
                        seen.append(m)
        for ds, seen in modes.items():
            g.node("Dataset", ds, write_mode="+".join(seen))
        twin = f"job:{spec.PANDAS_TWIN}"
        for rel, checked_by in PARITY_TWINS:
            g.edge("PARITY_TWIN_OF", twin, f"job:{rel}", checked_by=checked_by or "none (not parity-checked)")
        builder, spark_job = f"job:{spec.GRAPH_BUILD_SCRIPT}", f"job:{spec.GRAPH_SPARK_JOB}"
        if g.has(builder) and g.has(spark_job):   # the graph builder and its lakehouse-native twin
            g.edge("PARITY_TWIN_OF", builder, spark_job, checked_by=spec.GRAPH_PARITY
                   if g.has(f"job:{spec.GRAPH_PARITY}") else "none (not parity-checked)")

    # ------------------------------------------------------------------ 9. orchestration
    def add_orchestration(self) -> None:
        g = self.g
        make, shell = self.facts["make"], self.facts["shell"]
        for d in self.facts["dags"]:
            did = g.node("Dag", f"dag:{d['dag_id']}", dag_id=d["dag_id"], file=d["file"], tags=",".join(d["tags"]),
                         schedule="None", operator=",".join(sorted({t["factory"] for t in d["tasks"].values()})))
            for t in d["tasks"].values():
                where = f"{d['file']}:{t['lineno']}"
                tid = g.node("DagTask", f"task:{d['dag_id']}.{t['task_id']}", dag_id=d["dag_id"],
                             task_id=t["task_id"], operator=t["factory"])
                g.edge("HAS_TASK", did, tid)
                recognised = 0   # references found, resolved or not (a missing file is unresolved, not silent)
                job_args = (t["params"].get("job_args") or "").split()
                for name, arg in t["params"].items():
                    runs = ex.command_runs(arg)
                    candidates = list(runs["py"])
                    candidates += [(f"src/{m.replace('.', '/')}.py", "-m") for m in runs["modules"]
                                   if self.files.exists(f"src/{m.replace('.', '/')}.py")]
                    candidates += [(ex.spark_job_path(j), None) for j in runs["jobs"]]   # /opt/jobs/x/y.py
                    job_param = name in spec.DAG_JOB_PARAMS or not t["factory_params"]   # unknown factory: any
                    if not candidates and job_param and arg.endswith(".py") and " " not in arg:
                        # spark_submit_task(id, "churn/01_x.py"), spark_submit_args_task(id, job_path, job_args)
                        candidates = [(ex.spark_job_path(arg), job_args[0] if job_args else None)]
                    for rel, rel_arg in dict.fromkeys(candidates):
                        recognised += 1
                        jid = self.ensure_job(rel, where)
                        if jid:
                            g.node("DagTask", tid, job_path=rel)
                            g.edge("RUNS", tid, jid, via=t["factory"], args=rel_arg)
                    for sh, sh_arg in runs["sh"]:
                        recognised += 1
                        if g.has(f"sh:{sh}") or sh in shell:
                            self._shell_node(sh)
                            g.edge("RUNS", tid, f"sh:{sh}", via=t["factory"], args=sh_arg)
                        else:
                            g.unresolve(where, f"runs {sh}, which does not exist")
                if not recognised:
                    g.warnings.append(f"{where}: DAG task {t['task_id']} ({t['factory']}) runs nothing the "
                                      f"extractor recognises")
            for a, b in d["edges"]:
                g.edge("UPSTREAM_OF", f"task:{d['dag_id']}.{a}", f"task:{d['dag_id']}.{b}")

        def runs_something(name: str, seen: tuple = ()) -> bool:
            d = make[name]
            if d["py"] or d["sh"] or d["jobs"] or any(
                    self.files.exists(f"src/{m.replace('.', '/')}.py") for m in d["modules"]):
                return True
            return any(runs_something(x, seen + (name,)) for x in d["prereqs"] + d["calls_make"]
                       if x in make and x not in seen)

        relevant = {t for t in make if runs_something(t)}
        relevant |= {x for t in list(relevant) for x in make[t]["prereqs"] + make[t]["calls_make"] if x in make}
        relevant |= {m for s in self.facts["ci"] for m in s["make"] if m in make}
        relevant |= {m for s in shell.values() for m in s["make"] if m in make}
        for name in sorted(relevant):
            d = make[name]
            g.node("MakeTarget", f"make:{name}", name=name, help=d["help"] or None, phony=d["phony"],
                   recipe=" ; ".join(d["recipe"])[:500] or None, domain=ex.job_domain(name))
        for name in sorted(relevant):
            d, mid, where = make[name], f"make:{name}", f"{spec.MAKEFILE}:{name}"
            for x in d["prereqs"]:
                if x in relevant:
                    g.edge("DEPENDS_ON", mid, f"make:{x}", kind="prerequisite")
                elif x not in make and not x.startswith(("$", ".")):
                    g.unresolve(where, f"prerequisite {x} is not a target")
            for x in d["calls_make"]:
                if x in relevant:
                    g.edge("DEPENDS_ON", mid, f"make:{x}", kind="$(MAKE) call")
                elif x not in make:
                    g.unresolve(where, f"$(MAKE) {x} is not a target")
            self._runner_edges(mid, d, where, env=",".join(f"{k}={v}" for k, v in d["env"]) or None)
            for sh, arg in d["sh"]:
                if sh.endswith("airflow_trigger.sh") and arg:
                    if g.has(f"dag:{arg}"):
                        g.edge("TRIGGERS", mid, f"dag:{arg}", via=sh)
                    else:
                        g.unresolve(where, f"triggers DAG {arg}, which no file in airflow/dags defines")
        for rel in sorted(shell):
            self._shell_node(rel)
        for rel, s in sorted(shell.items()):
            sid, where = f"sh:{rel}", rel
            for i, job in enumerate(s["jobs"], 1):
                jid = self.ensure_job(f"src/jobs/{job}", where)
                if jid:
                    g.edge("RUNS", sid, jid, ordinal=i, via="spark-submit")
            for py, arg in s["py"]:
                jid = self.ensure_job(py, where)
                if jid:
                    g.edge("RUNS", sid, jid, args=arg)
            for call in s["calls"]:
                if call in shell:
                    g.edge("CALLS", sid, f"sh:{call}")
                else:
                    g.unresolve(where, f"calls {call}, which does not exist")
            for target in s["make"]:
                if g.has(f"make:{target}"):
                    g.edge("RUNS", sid, f"make:{target}")
                else:
                    g.unresolve(where, f"runs make {target}, which is not a target")
        self._ci_steps()

    def _shell_node(self, rel: str) -> str:
        s = self.facts["shell"].get(rel, {})
        return self.g.node("ShellScript", f"sh:{rel}", path=rel, docker_exec=s.get("docker_exec"),
                           jobs=",".join(s.get("jobs", [])) or None)

    def _runner_edges(self, runner: str, d: dict, where: str, env: str | None = None,
                      foreign_ok: bool = False) -> None:
        """RUNS edges of a Make target / CI step to the scripts, modules and pipelines it runs."""
        g = self.g
        for py, arg in d["py"]:
            if foreign_ok and not self.files.exists(py):
                continue
            jid = self.ensure_job(py, where)
            if jid:
                g.edge("RUNS", runner, jid, args=arg, env=env)
        for module in d.get("modules", []):
            rel = f"src/{module.replace('.', '/')}.py"
            if self.files.exists(rel):
                g.edge("RUNS", runner, self.ensure_job(rel, where), args="-m", env=env)
        for sh, arg in d["sh"]:
            if sh in self.facts["shell"]:
                g.edge("RUNS", runner, self._shell_node(sh), args=arg)
            elif not foreign_ok:
                g.unresolve(where, f"runs {sh}, which does not exist")
        for job in d.get("jobs", []):
            jid = self.ensure_job(f"src/jobs/{job}", where)
            if jid:
                g.edge("RUNS", runner, jid, via="spark-submit")

    def _ci_steps(self) -> None:
        g = self.g
        seen: Counter = Counter()
        for s in self.facts["ci"]:
            slug = re.sub(r"[^a-z0-9]+", "-", s["name"].lower()).strip("-") or f"line-{s['line']}"
            seen[(s["job"], slug)] += 1
            if seen[(s["job"], slug)] > 1:
                slug = f"{slug}-{seen[(s['job'], slug)]}"
            where = f"{spec.CI_WORKFLOW}:{s['line']}"
            env = ex.dumps(s["env"]) if s["env"] else None
            sid = g.node("CiStep", f"ci:{s['job']}#{slug}", job=s["job"], name=s["name"], workflow=spec.CI_WORKFLOW,
                         env=env)
            # a step that clones another repo runs that repo's scripts too: not ours to resolve
            self._runner_edges(sid, s, where, env=env, foreign_ok=bool(s["clones"]))
            for target in s["make"]:
                if target in self.facts["make"]:
                    g.edge("RUNS", sid, f"make:{target}")
                else:
                    g.unresolve(where, f"runs make {target}, which is not a target")
            # one CLONES edge per candidate ref (a step that picks the same-named branch when it exists,
            # else main, clones one of two refs); an unresolved repository is in graph.unresolved already
            for clone in s["clones"]:
                for note in clone["notes"]:
                    g.warnings.append(f"{spec.CI_WORKFLOW}:{clone['line']}: {note}")
                for url in clone["urls"]:
                    key = ex.repo_key(url)
                    rid = g.node("DownstreamRepo", f"repo:{key}", name=key.rsplit("/", 1)[-1], url=url)
                    for ref in clone["refs"]:
                        g.edge("CLONES", sid, rid, ref=ref, ref_expr=clone["ref_expr"], n_refs=len(clone["refs"]),
                               depth=clone["depth"], url_expr=clone["url_expr"],
                               source=f"{spec.CI_WORKFLOW}:{clone['line']}")
                    if "data/export" in s["body"]:
                        for fname, rel in sorted(self.exports.items()):
                            if g.props(f"export:{rel}").get("consumed_by_radar"):
                                g.edge("CONSUMES", rid, f"export:{rel}", via=f"{spec.CI_WORKFLOW}: {s['name']}",
                                       as_path=f"data/external/{fname}")

    # ------------------------------------------------------------------ 10. contracts
    def add_contracts(self) -> None:
        g = self.g
        for rel in self.jobs:
            if ex.job_kind(rel) == "check":
                cid = g.node("Contract", f"contract:{Path(rel).stem.removeprefix('check_')}", name=Path(rel).stem,
                             implemented_by=rel, modelled=False, source=rel)
                g.edge("IMPLEMENTS", f"job:{rel}", cid)
        self._export_contract()
        self._parity_contract()
        self._readme_contracts()
        self._graph_contracts()

    def _assertion(self, contract: str, key: str, targets: list[str], **props) -> str:
        g = self.g
        aid = f"assert:{contract}#{key}"
        if g.has(aid):
            aid = f"{aid}@L{props.get('source_line')}"
        g.node("Assertion", aid, contract=contract, **props)
        g.edge("HAS_ASSERTION", f"contract:{contract}", aid)
        for t in targets:
            g.edge("CHECKS", aid, t)
        return aid

    def _export_contract(self) -> None:
        g = self.g
        c = self.facts["export_contract"]
        rel = spec.EXPORT_CONTRACT
        gold = spec.GOLD_TABLE
        train, serve, audit = (self.exports.get(n) for n in ("churn_user_features.csv", "hero_inference_record.json",
                                                             "churn_renewals_audit.csv"))
        if train is None or serve is None:
            raise LineageExtractError(f"{spec.EXPORT_JOB}: the train CSV / serve JSON exports the contract checks "
                                      f"are not written by the export job")
        known = {n["props"]["name"] for n in g.nodes.values() if n["label"] == "DataColumn"}
        dangling = sorted(x for x in c["leaky"] if x not in known)
        g.node("Contract", "contract:churn_export", name="churn export contract", implemented_by=rel,
               severity_policy="structural = error; ranges = warn (--strict: error)",
               mirrors="retention-radar configs/schemas/user_record.schema.json" if "retention-radar" in c["doc"]
               else None, dangling_refs=",".join(dangling) or None, modelled=True)
        guarded = self._guarded_exports()
        for name in sorted(guarded):
            if name in self.exports:
                g.edge("GUARDS", "contract:churn_export", f"export:{self.exports[name]}")
            else:
                g.unresolve(rel, f"requires {name}, which the export job does not write")

        def tcol(name: str, line: int) -> str:
            return self.need_column(train, name, f"{rel}:{line}")

        for chk in c["checks"]:
            test, msg, line, guards = chk["test"], chk["message"], chk["lineno"], chk["guards"]
            base = {"severity": chk["severity"], "source": f"{rel}:{line}", "source_line": line,
                    "expr": test[:300], "message": msg}
            in_test = [x for x in re.findall(r"df\[['\"](\w+)['\"]\]", test) if x in c["train_columns"]]
            if any(gd.replace("(", "").replace(")", "").startswith("for col, lo, hi in RANGES") for gd in guards):
                for col, lo, hi in c["ranges"]:
                    self._assertion("churn_export", f"range:{col}", [tcol(col, line)], kind="range", min=lo, max=hi,
                                    **{**base, "severity": "warn (error with --strict)" if chk["severity"] == "warn"
                                       else chk["severity"], "expr": f"{lo:g} <= {col} <= {hi:g}"})
            elif ".exists()" in test and "missing" in msg:
                self._assertion("churn_export", "files_exist",
                                [f"export:{self.exports[n]}" for n in sorted(guarded) if n in self.exports],
                                kind="files_exist", **base)
            elif "TRAIN_COLUMNS" in test and "columns" in test:
                self._assertion("churn_export", "columns_exact", [f"export:{train}"], kind="columns_exact", **base)
            elif "nulls" in test:
                self._assertion("churn_export", "not_null", [tcol(x, line) for x in c["train_columns"]],
                                kind="not_null", n_columns=len(c["train_columns"]), **base)
            elif "duplicated" in test and in_test:
                self._assertion("churn_export", f"unique:{in_test[0]}", [tcol(in_test[0], line)], kind="unique",
                                **base)
            elif "PLAN_TIERS" in " ".join(guards) or "unknown plan_tier" in msg:
                self._assertion("churn_export", "accepted_values:plan_tier", [tcol("plan_tier", line)],
                                kind="accepted_values", accepted_values=",".join(c["plan_tiers"]), **base)
            elif "must be 0/1" in msg and any(gd.startswith("for col in BINARY") for gd in guards):
                self._assertion("churn_export", "binary:flags", [tcol(x, line) for x in c["binary"]], kind="binary",
                                n_columns=len(c["binary"]), **base)
            elif "must be 0/1" in msg and in_test:
                self._assertion("churn_export", f"binary:{in_test[0]}", [tcol(in_test[0], line)], kind="binary",
                                **base)
            elif len(set(in_test)) == 2 and ">" in test:
                self._assertion("churn_export", "consistency:" + "_vs_".join(dict.fromkeys(in_test)),
                                [tcol(x, line) for x in dict.fromkeys(in_test)], kind="consistency", **base)
            elif "leaked" in test and audit is not None:
                self._assertion("churn_export", "route_exclusion",
                                [self.need_column(audit, "route", rel), tcol("user_id", line)],
                                kind="route_exclusion", **base)
            elif "sorted(record)" in test:
                self._assertion("churn_export", "serve_keys_exact", [f"export:{serve}"], kind="serve_keys_exact",
                                **base)
            elif "LEAKY" in test:
                leaks = sorted(set(c["leaky"]) | {x for x in c["train_columns"] if x not in self.serve_keys})
                self._assertion("churn_export", "no_leak",
                                [f"export:{serve}"] + [column_id(gold, x) for x in leaks
                                                       if g.has(column_id(gold, x))],
                                kind="no_leak", accepted_values=",".join(c["leaky"]), **base)
            elif "record.get" in test and "user_id" in test:
                self._assertion("churn_export", "serve_not_in_train",
                                [self.need_column(serve, "user_id", rel), tcol("user_id", line)],
                                kind="serve_not_in_train", **base)
            else:
                g.unresolve(f"{rel}:{line}", f"check not classified: {test[:80]} (teach lineage/graph.py)")

    def _parity_contract(self) -> None:
        g = self.g
        p = self.facts["parity_contract"]
        rel = spec.PARITY_CONTRACT
        gold = spec.GOLD_TABLE
        tol = f"np.isclose atol={p['atol']:g} (numeric), exact string otherwise" if p["atol"] is not None else None
        g.node("Contract", "contract:gold_parity", name="Spark SQL vs pandas gold parity", implemented_by=rel,
               severity_policy="error", tolerance=tol, modelled=True)
        g.edge("GUARDS", "contract:gold_parity", f"ds:{gold}")
        for chk in p["checks"]:
            base = {"severity": chk["severity"], "source": f"{rel}:{chk['lineno']}", "source_line": chk["lineno"],
                    "expr": chk["test"][:300], "message": chk["message"]}
            if any(gd.startswith("for c in cols") for gd in chk["guards"]):
                self._assertion("gold_parity", "values", [self.need_column(gold, c, rel) for c in p["columns"]],
                                kind="parity_values", n_columns=len(p["columns"]), **base)
            elif "len(a) != len(b)" in chk["test"]:
                self._assertion("gold_parity", "rowcount", [f"ds:{gold}"], kind="parity_rowcount", **base)
            else:
                g.unresolve(f"{rel}:{chk['lineno']}", f"check not classified: {chk['test'][:80]}")

    def _readme_contracts(self) -> None:
        g = self.g
        r = self.facts["readme"]
        gold = spec.GOLD_TABLE
        policy = "documentation (not executed)"
        if r["churn"]:
            g.node("Contract", "contract:readme_churn", name="README expected results (renewal features)",
                   severity_policy=policy, source=spec.README, modelled=True)
            for k, v in r["churn"].items():
                if k == "n_subscriptions" or v is None:
                    continue
                target = "route" if k.startswith("route") else "churned"
                self._assertion("readme_churn", k, [column_id(gold, target)], kind="expected_value", severity="doc",
                                expected=f"{float(v):g}", source=f"{spec.README} (seed 42, make churn-sample)")
        else:
            g.warnings.append(f"{spec.README}: the churn contract sentence (renewals routed to the model ...) was not "
                              f"found; no README assertions for the renewal features")
        daily_table = "lakehouse.gold.daily_order_metrics"
        if (r["retail_daily"] or r["retail_orders"]) and g.has(f"ds:{daily_table}"):
            g.node("Contract", "contract:readme_retail", name="README expected results (retail)",
                   severity_policy=policy, source=spec.README, modelled=True)
            for day, orders, revenue, aov in r["retail_daily"]:
                cols = [c for c in ("orders", "revenue", "avg_order_value") if g.has(column_id(daily_table, c))]
                self._assertion("readme_retail", day, [column_id(daily_table, c) for c in cols],
                                kind="expected_value", severity="doc", source=f"{spec.README} (retail table)",
                                expected=ex.dumps({"orders": int(orders), "revenue": float(revenue),
                                                   "avg_order_value": float(aov)}))
            if r["retail_orders"] and g.has("ds:lakehouse.silver.orders"):
                bronze_n, silver_n = r["retail_orders"]
                self._assertion("readme_retail", "bronze_to_silver_orders", ["ds:lakehouse.silver.orders"],
                                kind="expected_value", severity="doc", source=spec.README,
                                expected=ex.dumps({"bronze": int(bronze_n), "silver": int(silver_n)}))

    def _graph_contracts(self) -> None:
        g = self.g
        gold = spec.GOLD_TABLE
        if g.has(f"job:{spec.GRAPH_CONTRACT}"):
            cid = g.node("Contract", "contract:graph_contract", name=f"graph contract {gspec.CONTRACT_VERSION}",
                         severity_policy="structural = error; warnings fail with --strict", modelled=True)
            for ds in self.graph_datasets:
                g.edge("GUARDS", cid, ds)
            for feature, card in gspec.FEATURE_CARDS.items():
                if card.get("verification") != "graph-verified":
                    continue
                edge = f"ge:edge:{card['backing_edge']}"
                self._assertion(
                    "graph_contract", f"pit_parity:{feature}", [self.need_column(gold, feature, spec.GRAPH_SPEC), edge],
                    kind="pit_parity", severity="error", source=spec.GRAPH_CONTRACT,
                    expr=f"{feature} re-derived from {card['backing_edge']} edges for every renewal = gold",
                    message=gspec.PIT_WINDOWS[card["backing_edge"]].note)
            audit = self.exports.get("churn_renewals_audit.csv")
            if audit:
                self._assertion("graph_contract", "export_crosscheck", [f"export:{audit}", "ge:node:Renewal"],
                                kind="graph_equals_export",
                                severity="warn when exports are missing (error with --strict)",
                                source=spec.GRAPH_CONTRACT, expr="Renewal features = churn_renewals_audit.csv rows")
        if g.has(f"job:{spec.LINEAGE_CONTRACT}"):
            cid = g.node("Contract", "contract:lineage_contract", name=f"lineage contract {spec.CONTRACT_VERSION}",
                         severity_policy="structural = error; warnings fail with --strict", modelled=True)
            for ds in self.lineage_datasets:
                g.edge("GUARDS", cid, ds)

    # ------------------------------------------------------------------ 11. metrics
    def add_metrics(self) -> None:
        g = self.g
        gold = spec.GOLD_TABLE
        churn = {
            "churn.voluntary_lapse_rate": ("voluntary_lapse_rate", "AVG(churned)", "route = 'model'",
                                           "all model-routed renewals", ["churned", "route"]),
            "churn.voluntary_lapse_rate_by_plan": ("voluntary_lapse_rate by plan_tier", "AVG(churned)",
                                                   "route = 'model'", "plan_tier", ["churned", "route", "plan_tier"]),
            "churn.renewals_by_route": ("renewals by route, outcome", "COUNT(*)", None, "route, outcome",
                                        ["route", "outcome"]),
        }
        for mid, (name, expr, flt, grain, uses) in churn.items():
            m = g.node("Metric", f"metric:{mid}", name=name, dataset=gold, expr=expr, filter=flt, grain=grain,
                       source=f"{spec.GOLD_JOB} route summary")
            g.edge("DEFINED_ON", m, f"ds:{gold}")
            for c in uses:
                g.edge("USES", m, self.need_column(gold, c, "metrics"))
        daily = "lakehouse.gold.daily_order_metrics"
        if g.has(f"ds:{daily}"):
            for e in g.out(f"ds:{daily}", "HAS_COLUMN"):
                col = g.props(e["dst"])
                if col["role"] != "metric":
                    continue
                m = g.node("Metric", f"metric:retail.{col['name']}", name=col["name"], dataset=daily,
                           grain="order_date", source="sql/retail/gold_daily_metrics.sql",
                           expr=next((x["props"].get("transform") for x in g.out(e["dst"], "DERIVED_FROM")),
                                     "COUNT(*)"))
                g.edge("DEFINED_ON", m, f"ds:{daily}")
                g.edge("MATERIALIZED_AS", m, e["dst"])

    # ------------------------------------------------------------------ 12. full profile
    def add_file_snapshots(self) -> None:
        g = self.g
        sample = self.sample_dir or self.repo / "data/sample/churn"
        export = self.export_dir or self.repo / "data/export"
        found = 0
        missing: list[tuple[str, str]] = []   # (label, file name) of the data files this machine does not have
        for nid, fname in sorted(self.source_files.items()):
            props = g.props(nid)
            if g.label(nid) == "Export":
                path = export / fname
            elif props["domain"] == "churn":
                path = sample / fname
            else:
                path = self.repo / props["path"]
            if not path.is_file():
                missing.append((g.label(nid), fname))
                continue
            found += 1
            digest = mf.sha256_file(path)
            data = path.read_bytes()
            rows = (data.count(b"\n") - 1) if path.suffix == ".csv" else 1
            shown = mf.display_path(path)
            volatile = fname.startswith("churn_renewals_audit")
            fid = g.node("FileSnapshot", f"fsnap:{shown}@{digest[:12]}", path=shown, sha256=digest, n_bytes=len(data),
                         n_rows=rows, deterministic=not volatile,
                         note="embeds built_at, so its hash changes per build" if volatile else None)
            g.edge("HAS_FILE_SNAPSHOT", nid, fid)
            g.inputs[f"data:{shown}"] = digest
        if not found:
            g.environment_warnings.append(f"full profile: no data file found under {sample} or {export}; no "
                                          f"FileSnapshot nodes")
        elif missing:   # some present, some not: say which, rather than silently snapshot fewer files
            names = ", ".join(sorted({name for _label, name in missing}))
            unobserved = "; without the exports the README route counts and metric goldens are not observed " \
                         "(generate them first: make graph-sample PROFILE=<profile>, or make churn-gold-local for " \
                         "data/export)" if any(label == "Export" for label, _name in missing) else ""
            g.environment_warnings.append(f"full profile: {len(missing)} data file(s) not found under "
                                          f"{mf.display_path(sample)} / {mf.display_path(export)} ({names}): no "
                                          f"FileSnapshot for them{unobserved}")
        self._observed(export)

    def _observed(self, export: Path) -> None:
        """README assertions and metric goldens observed in the exports (when they exist)."""
        import pandas as pd

        g = self.g
        audit_path, train_path = export / "churn_renewals_audit.csv", export / "churn_user_features.csv"
        if not (audit_path.is_file() and train_path.is_file()):
            return
        audit, train = pd.read_csv(audit_path), pd.read_csv(train_path)
        observed = {f"route_{r}": int((audit["route"] == r).sum()) for r in ("model", "dunning", "cancel_flow",
                                                                                "score_today")}
        observed["lapse_pct"] = round(100 * float(train["churned"].mean()), 1)
        for k, v in observed.items():
            aid = f"assert:readme_churn#{k}"
            if g.has(aid):
                expected = float(g.props(aid)["expected"])
                g.node("Assertion", aid, observed=f"{float(v):g}", holds=expected == float(v))
        by_plan = train.groupby("plan_tier")["churned"].agg(["mean", "sum", "count"])
        routes = audit.groupby(["route", "outcome"]).size().rename("n").reset_index().to_dict("records")
        goldens = {
            "churn.voluntary_lapse_rate": (f"{float(train['churned'].mean()):.4f}",
                                           f"{int(train['churned'].sum())}/{len(train)}"),
            "churn.voluntary_lapse_rate_by_plan": (ex.dumps({k: {"rate": round(float(v["mean"]), 4),
                                                                 "lapses": int(v["sum"]), "n": int(v["count"])}
                                                             for k, v in by_plan.iterrows()}), None),
            "churn.renewals_by_route": (ex.dumps(routes), None),
        }
        for mid, (golden, detail) in goldens.items():
            if g.has(f"metric:{mid}"):
                g.node("Metric", f"metric:{mid}", golden=golden, golden_detail=detail)

    def add_radar(self) -> None:
        """The retention-radar consumer interface: the local checkout at RADAR_DIR and, only with
        ``radar_public``, the public main branch through ``gh api`` (the one network path)."""
        g = self.g
        refs = []
        if self.radar_dir is not None:
            local = self._radar_local()
            if local:
                refs.append(local)
        else:
            g.environment_warnings.append("full profile: RADAR_DIR is not set; no retention-radar interface nodes "
                                          "(the severity comparison needs a local radar checkout)")
        if self.radar_public:
            public = self._radar_public()
            if public:
                refs.append(public)
        if not refs:
            return
        contract = self.facts["export_contract"]
        train, serve = self.exports["churn_user_features.csv"], self.exports["hero_inference_record.json"]
        rid = g.node("DownstreamRepo", f"repo:{spec.RADAR_URL}", name="retention-radar",
                     url=f"https://{spec.RADAR_URL}")
        ranges = {c: (lo, hi) for c, lo, hi in contract["ranges"]}
        for r in refs:
            schema = r["schema"]
            required = schema.get("required", [])
            missing = [c for c in self.serve_keys if c not in required]
            extra = [c for c in required if c not in self.serve_keys]
            tiers = schema.get("properties", {}).get("plan_tier", {}).get("enum", [])
            serve_name = serve.rsplit("/", 1)[-1]
            compatible = (not missing and not extra and r["serve_file"] in (None, serve_name)
                          and set(tiers) >= set(contract["plan_tiers"]))
            cid = g.node("Contract", f"contract:radar_user_record@{r['ref']}", modelled=True,
                         name=f"retention-radar user_record schema ({r['ref']})",
                         implemented_by=f"{spec.RADAR_INGEST} validate_users + {spec.RADAR_SCHEMA}", source=r["source"],
                         severity_policy="ranges are hard errors in validate_users" if r["ranges"] else None,
                         detail=ex.dumps({"additional_properties": schema.get("additionalProperties"),
                                          "n_required": len(required), "plan_tier_enum": tiers,
                                          "expected_serve_file": r["serve_file"], "keys_missing": missing,
                                          "keys_extra": extra, "compatible": compatible}))
            g.edge("PUBLISHES", rid, cid, ref=r["ref"])
            g.edge("CONSUMES_VIA", cid, f"export:{train}", compatible=compatible)
            g.edge("CONSUMES_VIA", cid, f"export:{serve}", compatible=compatible, expected_file=r["serve_file"])
            if r["ranges"]:
                g.edge("MIRRORED_BY", cid, "contract:churn_export")
            for col, (lo, hi) in sorted(r["ranges"].items()):
                if not g.has(column_id(train, col)):
                    g.unresolve(f"radar {spec.RADAR_CONFIG}", f"FEATURE_RANGES names {col}, not a train column")
                    continue
                aid = self._assertion(f"radar_user_record@{r['ref']}", f"range:{col}", [column_id(train, col)],
                                      kind="range", severity="error", min=float(lo), max=float(hi),
                                      source=f"{spec.RADAR_CONFIG} FEATURE_RANGES")
                if col in ranges and g.has(f"assert:churn_export#range:{col}"):
                    g.edge("SAME_RULE_AS", aid, f"assert:churn_export#range:{col}", severity_differs=True,
                           bounds_equal=(float(lo), float(hi)) == ranges[col])
            g.node("DownstreamRepo", rid, **r["repo_props"])
        for fname in ("churn_user_features.csv", "hero_inference_record.json"):
            g.edge("CONSUMES", rid, f"export:{self.exports[fname]}", as_path=f"data/external/{fname}",
                   via="scripts/sync_lakehouse_exports.sh (retention-radar)")

    def _radar_local(self) -> dict | None:
        g = self.g
        d = self.radar_dir
        schema_path, config_path, ingest_path = d / spec.RADAR_SCHEMA, d / spec.RADAR_CONFIG, d / spec.RADAR_INGEST
        if not schema_path.is_file():
            g.warnings.append(f"full profile: RADAR_DIR={d} has no {spec.RADAR_SCHEMA}; no radar interface nodes")
            return None
        try:
            schema = json.loads(schema_path.read_text())
        except ValueError as e:
            raise LineageExtractError(f"radar {spec.RADAR_SCHEMA}: not JSON ({e})") from e
        g.inputs[f"radar:{spec.RADAR_SCHEMA}"] = mf.sha256_file(schema_path)
        ranges = {}
        if config_path.is_file():
            g.inputs[f"radar:{spec.RADAR_CONFIG}"] = mf.sha256_file(config_path)
            ranges = ex.module_const(ast.parse(config_path.read_text()), "FEATURE_RANGES") or {}
        serve_file = None
        if ingest_path.is_file():
            g.inputs[f"radar:{spec.RADAR_INGEST}"] = mf.sha256_file(ingest_path)
            found = re.findall(r'"(\w+_inference_record\.json)"', ingest_path.read_text())
            serve_file = found[0] if found else None

        def git(*args: str) -> str | None:
            try:
                p = subprocess.run(["git", "-C", str(d), *args], capture_output=True, text=True, timeout=20,
                                   check=False)
            except (OSError, subprocess.SubprocessError):
                return None
            return p.stdout.strip() if p.returncode == 0 else None
        branch, commit = git("branch", "--show-current"), git("rev-parse", "--short=7", "HEAD")
        return {"ref": "local", "schema": schema, "ranges": ranges, "serve_file": serve_file,
                "source": f"local checkout {branch or '?'} @ {commit or '?'}",
                "repo_props": {"local_branch": branch, "local_commit": commit}}

    def _radar_public(self) -> dict | None:
        g = self.g
        repo = spec.RADAR_URL.split("/", 1)[1]

        def gh(path: str):
            try:
                p = subprocess.run(["gh", "api", f"repos/{repo}/{path}"], capture_output=True, text=True, timeout=30,
                                   check=False)
            except (OSError, subprocess.SubprocessError):
                return None
            try:
                return json.loads(p.stdout) if p.returncode == 0 else None
            except ValueError:
                return None
        head = (gh("commits/main") or {}).get("sha", "")[:7]
        schema_doc = gh(f"contents/{spec.RADAR_SCHEMA}")
        if not head or not schema_doc:
            g.warnings.append("--radar-public: gh api could not read the public retention-radar main branch")
            return None
        schema = json.loads(base64.b64decode(schema_doc["content"]))
        ingest_doc = gh(f"contents/{spec.RADAR_INGEST}")
        ingest = base64.b64decode(ingest_doc["content"]).decode() if ingest_doc else ""
        found = re.findall(r'"(\w+_inference_record\.json)"', ingest)
        g.inputs["radar-public:main"] = head
        return {"ref": "public-main", "schema": schema, "ranges": {}, "serve_file": found[0] if found else None,
                "source": f"github main @ {head}", "repo_props": {"public_main_head": head}}

    # ------------------------------------------------------------------ run
    def run(self, overlays=()) -> LineageGraph:
        self.add_jobs()
        self.add_churn_tables()
        self.add_retail_tables()
        self.add_gold()
        self.add_parameters()
        self.add_exports()
        self.add_graph_bridge()
        self.add_job_io()
        self.add_other_sql()
        self.add_graph_scripts()
        self.add_orchestration()
        self.add_contracts()
        self.add_metrics()
        if self.profile == "full":
            self.add_file_snapshots()
            self.add_radar()
        for overlay in overlays:
            overlay(self.g)
        self.finalize()
        return self.g

    def finalize(self) -> None:
        """Column degrees (n_sources / n_readers) and the list of files read, after every edge exists."""
        g = self.g
        sources: Counter = Counter()
        readers: Counter = Counter()
        for e in g.edges:
            if e["rel"] in ("DERIVED_FROM", "COUNTS_ROWS_OF"):
                sources[e["src"]] += 1
            if e["rel"] == "DERIVED_FROM":
                readers[e["dst"]] += 1
        for nid in g.ids("DataColumn"):
            g.node("DataColumn", nid, n_sources=sources[nid], n_readers=readers[nid])
        g.inputs = {**self.files.read, **g.inputs}


def _twin_key(name: str) -> str:
    """nodes_Renewal / node_renewal / renewal -> 'renewal'; edges_SIMILAR_TO / similar_to -> 'similarto'
    (no node label collides with an edge type once normalised)."""
    low = name.lower()
    for prefix in ("nodes_", "node_", "edges_", "edge_"):
        if low.startswith(prefix):
            low = low[len(prefix):]
            break
    return low.replace("_", "")


def leaf_upper(x: Leaf) -> float:
    """Upper bound (days after as_of) of one read: its own row window's; for a declared reference
    table with no window of its own, the event window its rows are matched to; else unbounded."""
    if x.window is not None:
        return x.window[2]
    if x.matched is not None and x.table in spec.GLOBAL_DIMENSION_TABLES:
        return x.matched[2]
    return INF


def column_upper(leaves: list[Leaf]) -> float:
    """The loosest upper bound (days after as_of) over every read of a gold column outside the
    as-of snapshot row (the renewals CTE). A feature is point-in-time compliant when it is <= 0.
    The exemption is sound only because GoldLineage stops on a renewals CTE that is more than a
    plain row filter (a window function or aggregate there would read other renewals' rows)."""
    return max((leaf_upper(x) for x in leaves if x.cte != "renewals"), default=0.0)


def ex_number(e) -> float | None:
    """Numeric value of a sqlglot literal expression (None when it is not one)."""
    from .scope_walk import literal_number

    return None if e is None else literal_number(e)


def assemble(repo: str | Path | None = None, profile: str = "core", *, export_dir: str | Path | None = None,
             sample_dir: str | Path | None = None, radar_dir: str | Path | None = None, radar_public: bool = False,
             overlays=()) -> LineageGraph:
    """Extract and assemble the lineage graph of the repo at ``repo`` (default: this repo).

    ``core`` is code-derived only: ``export_dir``, ``sample_dir``, ``radar_dir`` and
    ``radar_public`` are ignored. ``overlays`` are callables ``f(graph)`` run last, in order:
    the Tier-1 Iceberg facts (``iceberg_facts.overlay``) and the Tier-2 OpenLineage runs
    (``openlineage.overlay``) plug in here, on any profile.
    """
    spec.check_schema()
    return Assembler(repo or gspec.repo_root(), profile, export_dir=export_dir, sample_dir=sample_dir,
                     radar_dir=radar_dir, radar_public=radar_public).run(overlays)
