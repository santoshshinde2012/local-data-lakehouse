"""Metadata / lineage graph specification (spec ``metadata-graph/0.1``).

Everything that shapes the lineage graph's *content* is declared here once and imported
by the extractor (``extract.py`` / ``scope_walk.py``), the assembler (``graph.py``), the
Parquet writer and Ladybug loader (``build.py``), the pure-Python oracle (``oracle.py``),
the Cypher templates (``queries.py``) and the four agent tools (``tools.py``).

* 24 node labels and 48 edge types. Three labels (Snapshot, Ref, Run) and seven edge types
  (HAS_SNAPSHOT, POINTS_TO, PRODUCED_BY_RUN, RAN_AS, PARENT, SUPERSEDES, CONSUMED_SNAPSHOT;
  ``placeholder=True``) are Tier 1 / Tier 2: empty in a Tier-0 build, filled by the overlays
  ``iceberg_facts`` (Iceberg ``.snapshots`` + ``.refs``, ``--iceberg``) and ``openlineage``
  (OpenLineage JSONL, ``--openlineage``). The column label is ``DataColumn``: ``Column`` is
  reserved in Ladybug Cypher. Property names avoid Ladybug keywords (order, default, table,
  column, desc, group, on, in, profile, end, exists).
* Node ids are stable strings (``col:lakehouse.gold.churn_renewal_features#limit_hits_14d``).
  Agents address columns with a ``ColumnRef`` (``gold.churn_renewal_features.limit_hits_14d``),
  validated by ``is_column_ref`` (a full match of ``COLUMN_REF_RE``) and then checked against
  the refs that exist in the build.
* Two build profiles: ``core`` (offline, code-derived only; the CI contract) and ``full``
  (adds FileSnapshot nodes for the data files that exist and the retention-radar interface
  when RADAR_DIR points at a local checkout).
* ``graph_bridge()`` declares which silver / gold columns feed each of the 21 business
  graph node and edge types (``lakehouse_graph.spec``); the assembler fails if it does not
  cover the business spec exactly.

Bump ``SPEC_VERSION`` when the meaning of the content changes; goldens regenerate with
``python -m lakehouse_graph.lineage.oracle --build <dir> --print-golden``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import pyarrow as pa

SPEC_VERSION = "metadata-graph/0.1"
CONTRACT_VERSION = "metadata-graph/0.1"
PROFILES = ("core", "full")

# --------------------------------------------------------------------------- build layout
LINEAGE_DIR = "lineage"            # <build_dir>/lineage/{nodes_<Label>,edges_<TYPE>}.parquet
DB_FILE = "lineage.lbdb"           # <build_dir>/lineage.lbdb
MANIFEST_FILE = "manifest.json"    # <build_dir>/lineage/manifest.json (also manifest.json["lineage"])
CONTRACT_FILE = "contract.json"    # <build_dir>/lineage/contract.json (written by the contract script)
GOLDEN_DIR = "goldens"

# --------------------------------------------------------------------------- the repo surface
GOLD_SQL = "sql/churn/gold_renewal_features.sql"
GOLD_TABLE = "lakehouse.gold.churn_renewal_features"
SILVER_NAMESPACE = "lakehouse.silver"   # $silver in the gold SQL
# The grain of the gold SQL: one renewal per value of this column of the ``renewals`` CTE. Every
# feature window is relative to that one row's as_of, so CTEs may only be grouped by / joined on it.
RENEWAL_KEY = "subscription_id"
BRONZE_JOB = "src/jobs/churn/01_ingest_bronze.py"
SILVER_JOB = "src/jobs/churn/02_transform_silver.py"
GOLD_JOB = "src/jobs/churn/03_publish_gold_features.py"
EXPORT_JOB = "src/jobs/churn/04_export_features.py"
PANDAS_TWIN = "scripts/build_churn_gold_local.py"
GENERATOR = "scripts/generate_churn_sample.py"
EXPORT_CONTRACT = "scripts/check_churn_export.py"
PARITY_CONTRACT = "scripts/check_gold_parity.py"
GRAPH_BUILD_SCRIPT = "scripts/build_graph_local.py"
GRAPH_CONTRACT = "scripts/check_graph_contract.py"
LINEAGE_BUILD_SCRIPT = "scripts/build_lineage_local.py"
LINEAGE_CONTRACT = "scripts/check_lineage_contract.py"
GRAPH_SPARK_JOB = "src/jobs/graph/01_publish_gold_graph.py"   # the lakehouse-native twin of the graph builder
GRAPH_PARITY = "scripts/check_graph_parity.py"                # its SQL = the pandas builder, cell for cell
GRAPH_SPEC = "src/lakehouse_graph/spec.py"
MAKEFILE = "Makefile"
CI_WORKFLOW = ".github/workflows/ci.yml"
README = "README.md"
RETAIL_SQL = {   # companion SQL (not executed) -> the DataFrame job it mirrors
    "sql/retail/bronze_ddl.sql": "src/jobs/retail/02_ingest_bronze.py",
    "sql/retail/silver_orders.sql": "src/jobs/retail/03_transform_silver.py",
    "sql/retail/gold_daily_metrics.sql": "src/jobs/retail/04_publish_gold.py",
}
RETAIL_CSVS = ("orders_day1.csv", "orders_day2.csv", "customers.csv")   # data/sample/<name>, committed
# Files whose absence or unreadable shape stops the extraction (LineageExtractError).
REQUIRED_FILES = (GOLD_SQL, BRONZE_JOB, SILVER_JOB, GOLD_JOB, EXPORT_JOB, PANDAS_TWIN, GENERATOR, EXPORT_CONTRACT,
                  PARITY_CONTRACT, MAKEFILE)
# Globs the extractor walks (every match is read, hashed into lineage_build_id and becomes a node).
JOB_GLOBS = ("src/jobs/**/*.py", "scripts/*.py")
DAG_GLOB = "airflow/dags/*.py"
# Parameters of a DAG task factory that name a Spark job under /opt/jobs (lakehouse_operators.spark_submit_task
# and lakehouse_graph_operators.spark_submit_args_task: "churn/01_x.py" -> src/jobs/churn/01_x.py). Any other
# argument of a task is read as a command (scripts/x.py, python -m pkg.mod, pipelines/x.sh, /opt/jobs/x.py).
DAG_JOB_PARAMS = ("job_path",)
SHELL_GLOBS = ("pipelines/*.sh", "scripts/*.sh")
# Container mount points -> repo-relative paths (docker-compose volumes; used to canonicalise
# "${CHURN_EXPORT_DIR:-/opt/data/export}/x.csv" to "data/export/x.csv").
MOUNTS = {"/opt/data/sample": "data/sample", "/opt/data/export": "data/export", "/opt/data/graph": "data/graph",
          "/opt/sql": "sql", "/opt/jobs": "src/jobs"}
# The code that shapes lineage content besides the files it reads (hashed into lineage_build_id).
CONTENT_CODE = (
    "src/lakehouse_graph/lineage/__init__.py", "src/lakehouse_graph/lineage/spec.py",
    "src/lakehouse_graph/lineage/scope_walk.py", "src/lakehouse_graph/lineage/extract.py",
    "src/lakehouse_graph/lineage/graph.py", "src/lakehouse_graph/lineage/build.py",
    "src/lakehouse_graph/lineage/iceberg_facts.py", "src/lakehouse_graph/lineage/openlineage.py",
)
# Where the optional OpenLineage run writes its events (pipelines/run_graph_e2e.sh and the lakehouse_graph DAG:
# spark.openlineage.transport.location=/opt/data/graph/lineage/openlineage.jsonl), relative to the graph root.
OPENLINEAGE_FILE = "lineage/openlineage.jsonl"
# Spark appName conventions of the repo's jobs (check_repo_contracts.py): <domain>_<stem> -> src/jobs/<domain>/<stem>.py
APP_NAME_DOMAINS = ("churn", "graph", "retail")
# Files whose content decides the *semantic* golden answers (goldens/<name>.json is keyed by
# their sha256): a change here is expected to move a golden; a Makefile edit is not.
SEMANTIC_FILES = (
    GOLD_SQL, BRONZE_JOB, SILVER_JOB, GOLD_JOB, EXPORT_JOB, PANDAS_TWIN, GENERATOR, EXPORT_CONTRACT, PARITY_CONTRACT,
    GRAPH_SPEC, *RETAIL_SQL, *RETAIL_SQL.values(),
)

RADAR_URL = "github.com/santoshshinde2012/retention-radar"
RADAR_SCHEMA = "configs/schemas/user_record.schema.json"
RADAR_CONFIG = "src/retention_radar/config.py"
RADAR_INGEST = "src/retention_radar/data/ingest.py"

# --------------------------------------------------------------------------- ColumnRef
LAYERS = ("source", "bronze", "silver", "gold", "export")
COLUMN_REF_RE = re.compile(r"^(source|bronze|silver|gold|export)\.[a-z_]+\.[a-z0-9_]+$")   # use is_column_ref()
DOMAINS = ("churn", "retail")
TRACE_DIRECTIONS = ("upstream", "downstream")
MAX_TRACE_DEPTH = 6
TRACE_LARGE_EDGES = 200   # a trace with more edge rows says so in a caveat (the tool envelope caps rows at 200)
GOLD_DATASET_REF = "gold.churn_renewal_features"

# Roles a gold column reads a silver column in (DERIVED_FROM.roles, comma separated, sorted).
ROLES = ("ANCHOR", "FILTER", "JOIN_KEY", "PREDICATE", "TEMPORAL_JOIN", "VALUE", "WINDOW_BOUND")
# pit_status of a gold column. Features are compliant or a declared exception; an
# undeclared exception (reads after as_of, not declared in FEATURE_CARDS) fails the contract.
PIT_COMPLIANT = "compliant"
PIT_DECLARED_EXCEPTION = "declared_exception"
PIT_UNDECLARED_EXCEPTION = "undeclared_exception"
PIT_LABEL_SIDE = "label_side"        # label / outcome / route: read billing events after as_of on purpose
PIT_AS_OF_ROW = "as_of_row"          # attributes of the T-7 snapshot row itself
PIT_NOT_APPLICABLE = "not_applicable"  # lake metadata (built_at)
# Global reference tables: read without a time bound on purpose. The exemption holds only
# while the scope walk also sees the rows matched to event rows that are themselves bounded
# (``EXISTS(i.windows, w -> u.activity_date BETWEEN w.starts_on AND w.ends_on)``): the
# feature's upper bound is then the matched event window's. Read in any other way, or by a
# table that is not declared here, an unbounded read is a point-in-time exception.
GLOBAL_DIMENSION_TABLES = {
    "lakehouse.silver.churn_incidents": "declared incident windows: reference data with no knowledge-time column",
}
FEATURE_PIT_STATUSES = (PIT_COMPLIANT, PIT_DECLARED_EXCEPTION)
# Assertion kinds that do not check a column's values (absence checks, documentation).
NON_VALUE_ASSERTION_KINDS = ("no_leak", "expected_value")

# PLAN 6.6 invariant 9 (keeps Q15 and Q17 consistent): silver hit_at has no gold descendant,
# and bronze hit_at reaches gold only through silver hit_date.
INVARIANT_9 = {"silver": "silver.churn_limit_events.hit_at", "bronze": "bronze.churn_limit_events_raw.hit_at",
               "via": "silver.churn_limit_events.hit_date", "gold": "gold.churn_renewal_features.limit_hits_14d"}
# The lineage questions of the plan (section 3 Q13-Q17, plus impact and severity), as tool calls.
# Their answers are golden, are computed twice (Cypher and the Python oracle) and are timed.
QUESTIONS = (
    ("Q13_feature_pit", "lineage_pit", {"feature": "limit_hits_14d"}),
    ("Q14_pit_exceptions", "lineage_pit", {}),
    ("Q15_downstream_bronze_hit_at", "lineage_trace", {"target": "bronze.churn_limit_events_raw.hit_at",
                                                       "direction": "downstream"}),
    ("Q16_unguarded_gold", "lineage_guards", {}),
    ("Q17_unused_silver", "lineage_unused", {"layer": "silver", "domain": "churn"}),
    ("impact_silver_agent_requests", "lineage_trace", {"target": "silver.churn_usage_daily.agent_requests",
                                                       "direction": "downstream"}),
    ("severity_limit_hits_guards", "lineage_guards", {"column": "gold.churn_renewal_features.limit_hits_14d"}),
)
# More golden tool answers (not among the seven timed questions).
EXTRA_QUESTIONS = (
    ("upstream_allowance_used_pct", "lineage_trace", {"target": "gold.churn_renewal_features.allowance_used_pct",
                                                      "direction": "upstream"}),
    ("downstream_silver_hit_at", "lineage_trace", {"target": "silver.churn_limit_events.hit_at",
                                                   "direction": "downstream"}),
    ("unused_bronze_churn", "lineage_unused", {"layer": "bronze", "domain": "churn"}),
    ("unused_silver_retail", "lineage_unused", {"layer": "silver", "domain": "retail"}),
)

STR, I64, F64, BOOL = pa.string(), pa.int64(), pa.float64(), pa.bool_()


class LineageExtractError(RuntimeError):
    """The repo no longer has the shape the extractor understands (a required file, a name
    that does not resolve, a construct it cannot evaluate). Raised instead of dropping the
    fact silently: fix the code or teach the extractor, never ignore it."""


def dataset_ref(name: str) -> str:
    """``lakehouse.silver.x`` -> ``silver.x``; ``data/sample/churn/x.csv`` -> ``source.x``;
    ``data/export/x.csv`` -> ``export.x``; ``graph/parquet/nodes_X.parquet`` -> ``graph.nodes_X``."""
    if name.startswith("lakehouse."):
        return name[len("lakehouse."):]
    stem = name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    if name.startswith("data/export/"):
        return f"export.{stem}"
    if name.startswith("graph/"):
        return f"graph.{stem}"
    return f"source.{stem}"


def is_column_ref(ref) -> bool:
    """True when ``ref`` is a well-formed ColumnRef. ``fullmatch``: with ``match`` the pattern's
    ``$`` would also accept a trailing newline."""
    return isinstance(ref, str) and COLUMN_REF_RE.fullmatch(ref) is not None


def column_ref(dataset: str, column: str) -> str | None:
    """The ColumnRef of a column, or None when it does not fit COLUMN_REF_RE (not addressable)."""
    ref = f"{dataset_ref(dataset)}.{column}"
    return ref if is_column_ref(ref) else None


def column_id(dataset: str, column: str) -> str:
    return f"col:{dataset}#{column}"


# --------------------------------------------------------------------------- schema
@dataclass(frozen=True)
class NodeSpec:
    label: str
    columns: tuple[tuple[str, pa.DataType], ...]   # properties; the key ``id`` is implicit and first
    placeholder: bool = False

    @property
    def file(self) -> str:
        return f"nodes_{self.label}.parquet"

    @property
    def all_columns(self) -> tuple[tuple[str, pa.DataType], ...]:
        return (("id", STR), *self.columns)

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([pa.field(c, t, nullable=(c != "id")) for c, t in self.all_columns])


@dataclass(frozen=True)
class EdgeSpec:
    rel: str
    pairs: tuple[tuple[str, str], ...]             # allowed (FROM label, TO label) combinations
    columns: tuple[tuple[str, pa.DataType], ...] = ()
    placeholder: bool = False

    @property
    def file(self) -> str:
        return f"edges_{self.rel}.parquet"

    @property
    def all_columns(self) -> tuple[tuple[str, pa.DataType], ...]:
        return (("src", STR), ("dst", STR), ("src_label", STR), ("dst_label", STR), *self.columns)

    @property
    def schema(self) -> pa.Schema:
        fixed = ("src", "dst", "src_label", "dst_label")
        return pa.schema([pa.field(c, t, nullable=c not in fixed) for c, t in self.all_columns])


def _n(label: str, *cols: tuple[str, pa.DataType], placeholder: bool = False) -> NodeSpec:
    return NodeSpec(label, tuple(cols), placeholder)


def _s(*names: str) -> tuple[tuple[str, pa.DataType], ...]:
    return tuple((n, STR) for n in names)


NODE_SCHEMA: dict[str, NodeSpec] = {n.label: n for n in [
    _n("Dataset", *_s("name", "ref", "kind", "layer", "domain", "path", "path_template", "format", "write_mode",
                      "grain", "row_filter", "dedupe_keys"),
       ("is_demo", BOOL), *_s("declared_by", "status", "note",
                              # set for GLOBAL_DIMENSION_TABLES: why a read of it needs no time bound of its own
                              "reference_data")),
    _n("DataColumn", *_s("ref", "name", "dataset", "layer", "domain"), ("ordinal", I64),
       *_s("data_type", "role", "expr_sql", "pit_status"), ("max_upper_vs_as_of_days", I64),
       ("reads_after_as_of", BOOL), ("enum_values", STR), ("sql_clamp_min", F64), ("sql_clamp_max", F64),
       ("contract_min", F64), ("contract_max", F64), ("range_guarantee", STR), ("in_train_export", BOOL),
       ("declared_leaky", BOOL), *_s("sqlglot_sources", "sqlglot_unresolved"),
       # degrees, stored so a trace never queries past a leaf: n_sources = DERIVED_FROM + COUNTS_ROWS_OF
       # edges out of the column, n_readers = DERIVED_FROM edges into it (the contract re-counts both)
       ("n_sources", I64), ("n_readers", I64)),
    _n("Export", *_s("name", "ref", "path", "path_template", "format", "row_filter"), ("n_columns", I64),
       *_s("column_order", "contract"), ("consumed_by_radar", BOOL)),
    _n("Assertion", *_s("contract", "kind", "severity"), ("min", F64), ("max", F64),
       *_s("expr", "message", "source"), ("source_line", I64), *_s("accepted_values", "expected", "observed"),
       ("holds", BOOL), ("n_columns", I64)),
    _n("Contract", *_s("name", "implemented_by", "severity_policy", "source", "mirrors", "tolerance", "dangling_refs"),
       ("modelled", BOOL), ("detail", STR)),
    _n("Cte", *_s("name", "sql_file", "sql")),
    _n("SqlFile", *_s("path", "dialect"), ("executed", BOOL), *_s("executed_by", "placeholders", "parser"),
       ("qualify_ok", BOOL), ("n_ctes", I64), ("note", STR)),
    _n("Job", *_s("path", "name", "domain", "kind", "app_name"), ("app_name_matches_stem", BOOL),
       ("app_name_has_domain_prefix", BOOL), *_s("doc", "consts")),
    _n("EnvVar", *_s("name", "defaults")),
    _n("Dag", *_s("dag_id", "file", "tags", "schedule", "operator")),
    _n("DagTask", *_s("dag_id", "task_id", "operator", "job_path")),
    _n("MakeTarget", *_s("name", "help"), ("phony", BOOL), *_s("recipe", "domain")),
    _n("ShellScript", ("path", STR), ("docker_exec", BOOL), ("jobs", STR)),
    _n("CiStep", *_s("job", "name", "workflow", "env")),
    _n("Window", *_s("display", "anchor"), ("lower_offset_days", I64), ("lower_inclusive", BOOL),
       ("upper_offset_days", I64), ("upper_inclusive", BOOL), ("length_days", I64), ("reads_after_as_of", BOOL)),
    _n("PointInTimeRule", *_s("name", "statement", "predicate", "source", "as_of_expr", "renewal_expr"),
       ("as_of_offset_days", I64)),
    _n("Parameter", ("name", STR), ("sql_value", F64), ("pandas_value", F64), ("generator_value", F64),
       ("consistent", BOOL), ("sources", STR), ("unused", BOOL)),
    _n("Metric", *_s("name", "dataset", "expr", "filter", "grain", "golden", "golden_detail", "source")),
    _n("GraphElement", *_s("kind", "name", "spec_version", "key", "src_label", "dst_label"), ("n_properties", I64),
       ("feeds_feature", STR), ("pit_window_days", I64), *_s("pit_note", "verification", "note")),
    _n("FileSnapshot", *_s("path", "sha256"), ("n_bytes", I64), ("n_rows", I64), ("deterministic", BOOL),
       ("note", STR)),
    _n("DownstreamRepo", *_s("name", "url", "license", "public_main_head", "local_branch", "local_commit")),
    # Tier 1 (Iceberg metadata, lineage/iceberg_facts.py, --iceberg) and Tier 2 (OpenLineage runs,
    # lineage/openlineage.py, --openlineage): empty in a Tier-0 build, filled by those overlays.
    # Snapshot ids are strings (Iceberg snapshot ids are 64-bit and are only ever compared).
    _n("Snapshot", *_s("table_name", "snapshot_id", "parent_id"), ("sequence_number", I64),
       *_s("committed_at", "operation", "spark_app_id"), ("added_records", I64), ("total_records", I64),
       ("summary", STR), ("table_uuid", STR), ("timestamp_ms", I64),
       ("is_current", BOOL),     # the table's current snapshot when the catalog was read
       ("in_catalog", BOOL),     # false: known only from a build's pins (expired since, or another catalog)
       placeholder=True),
    _n("Ref", *_s("table_name", "name", "kind", "snapshot_id"), ("max_ref_age_ms", I64),
       ("min_snapshots_to_keep", I64), ("max_snapshot_age_ms", I64), placeholder=True),
    # kind: spark_application (an application run; its action runs are collapsed into it),
    # graph_build (a business build that read Iceberg snapshots), parent_run / root_run (the orchestrator
    # runs an OpenLineage ParentRunFacet names). actions: JSON {plan node kind: action runs};
    # datasets: JSON {"inputs": [...], "outputs": [...]} as OpenLineage reported them; env: JSON context
    # (OpenLineage environment-properties; for a graph build the tag and lakehouse build it read).
    _n("Run", *_s("job", "engine", "spark_app_id", "ol_run_id", "airflow_run_id", "started_at", "ended_at", "state",
                  "env"), *_s("kind", "namespace", "app_name", "engine_version"), ("n_actions", I64),
       ("n_events", I64), *_s("actions", "datasets", "source"), placeholder=True),
]}


def _e(rel: str, pairs: list[tuple[str, str]], *cols: tuple[str, pa.DataType], placeholder: bool = False) -> EdgeSpec:
    return EdgeSpec(rel, tuple(pairs), tuple(cols), placeholder)


EDGE_SCHEMA: dict[str, EdgeSpec] = {e.rel: e for e in [
    # ---- column lineage
    # window: the row window of the read relative to as_of ("unbounded" = no time filter; null = a column of
    # the as-of snapshot row). matched_window: for an unbounded read, the event window its rows are matched to.
    _e("DERIVED_FROM", [("DataColumn", "DataColumn")],
       *_s("roles", "cte", "window", "matched_window", "post_agg_compare", "transform", "path", "derived_by")),
    _e("COUNTS_ROWS_OF", [("DataColumn", "Dataset")], *_s("cte", "window", "derived_by", "note")),
    _e("USES_WINDOW", [("DataColumn", "Window")], *_s("source_dataset", "event_column", "via_cte")),
    _e("SUBJECT_TO", [("DataColumn", "PointInTimeRule")], ("status", STR), ("max_upper_vs_as_of_days", I64)),
    _e("COMPUTED_IN", [("DataColumn", "Cte")], ("subquery", STR)),
    _e("HAS_COLUMN", [("Dataset", "DataColumn"), ("Export", "DataColumn")], ("ordinal", I64)),
    _e("PRODUCED_BY", [("DataColumn", "Job")], ("transform", STR)),
    _e("EXCLUDED_FROM", [("DataColumn", "Export")], ("reason", STR), ("declared_leaky", BOOL)),
    _e("RELATIVE_TO", [("Window", "PointInTimeRule")]),
    _e("ROW_GRAIN_FROM", [("Dataset", "Dataset")], *_s("predicate", "rule")),
    _e("USED_BY", [("Parameter", "DataColumn"), ("Parameter", "PointInTimeRule")]),
    # ---- code, jobs, orchestration
    _e("READS", [("Job", "Dataset"), ("Job", "Export"), ("Cte", "Dataset"), ("SqlFile", "Dataset")],
       *_s("via", "purpose", "alias", "window", "iceberg_metadata", "path_template")),
    _e("WRITES", [("Job", "Dataset"), ("Job", "Export")], *_s("mode", "row_filter", "path_template")),
    _e("DEPENDS_ON", [("Cte", "Cte"), ("MakeTarget", "MakeTarget")], ("kind", STR)),
    # RUNS.args is the mode a command runs the target in: the first token after the script when it is a
    # subcommand or an option ("build", "promote", "--strict", "--profile"; an option's value is not kept),
    # "-m" for a module run, a spark_submit_args_task's first job argument, the first argument of a shell
    # script; null otherwise. Merged runs join theirs ("build,promote"). The same rule holds for every runner.
    _e("RUNS",[("CiStep", "Job"), ("CiStep", "MakeTarget"), ("CiStep", "ShellScript"), ("DagTask", "Job"),
                ("DagTask", "ShellScript"), ("MakeTarget", "Job"), ("MakeTarget", "ShellScript"),
                ("ShellScript", "Job"), ("ShellScript", "MakeTarget")],
       *_s("args", "env", "via"), ("ordinal", I64)),
    _e("CALLS", [("ShellScript", "ShellScript")]),
    _e("CONFIGURES", [("EnvVar", "Job")], ("default_value", STR)),
    _e("DEFINES", [("SqlFile", "Cte")]),
    _e("DESCRIBES", [("SqlFile", "Dataset")]),
    _e("EXECUTES_SQL", [("Job", "SqlFile")], *_s("substitution", "path_template")),
    _e("HAS_TASK", [("Dag", "DagTask")]),
    _e("UPSTREAM_OF", [("DagTask", "DagTask")]),
    _e("TRIGGERS", [("MakeTarget", "Dag")], ("via", STR)),
    _e("IMPORTS", [("Job", "Job")], ("via", STR)),
    # partition: the rows of a union twin (lakehouse.gold.graph_nodes) that mirror one table ("label = 'Renewal'")
    _e("MIRRORS", [("SqlFile", "Job"), ("Dataset", "Dataset")], ("verified_by", STR), ("partition", STR)),
    _e("PARITY_TWIN_OF", [("Job", "Job")], ("checked_by", STR)),
    _e("IMPLEMENTS", [("Job", "Contract")]),
    _e("HAS_FILE_SNAPSHOT", [("Dataset", "FileSnapshot"), ("Export", "FileSnapshot")]),
    # ---- contracts and consumers
    _e("CHECKS", [("Assertion", "DataColumn"), ("Assertion", "Dataset"), ("Assertion", "Export"),
                  ("Assertion", "GraphElement")]),
    _e("HAS_ASSERTION", [("Contract", "Assertion")]),
    _e("GUARDS", [("Contract", "Dataset"), ("Contract", "Export")]),
    _e("SAME_RULE_AS", [("Assertion", "Assertion")], ("severity_differs", BOOL), ("bounds_equal", BOOL)),
    _e("MIRRORED_BY", [("Contract", "Contract")]),
    _e("PUBLISHES", [("DownstreamRepo", "Contract")], ("ref", STR)),
    _e("CONSUMES_VIA", [("Contract", "Export")], ("compatible", BOOL), ("expected_file", STR)),
    _e("CONSUMES", [("DownstreamRepo", "Export")], *_s("as_path", "via")),
    # one edge per candidate ref of a `git clone`: ref is "default branch" without --branch; ref_expr is the
    # --branch word as written ("$REF"), n_refs how many refs that clone can take, url_expr the repository word
    _e("CLONES", [("CiStep", "DownstreamRepo")], *_s("ref", "ref_expr"), ("n_refs", I64), ("depth", I64),
       *_s("url_expr", "source")),
    # ---- metrics and the bridge to the business graph
    _e("DEFINED_ON", [("Metric", "Dataset")]),
    _e("USES", [("Metric", "DataColumn")]),
    _e("MATERIALIZED_AS", [("Metric", "DataColumn"), ("GraphElement", "Dataset")]),
    _e("SOURCED_FROM", [("GraphElement", "Dataset"), ("GraphElement", "DataColumn")], ("via", STR)),
    # ---- Tier 1 (Iceberg: --iceberg) and Tier 2 (OpenLineage: --openlineage); empty in a Tier-0 build
    _e("HAS_SNAPSHOT", [("Dataset", "Snapshot")], placeholder=True),
    _e("POINTS_TO", [("Ref", "Snapshot")], placeholder=True),
    _e("PRODUCED_BY_RUN", [("Snapshot", "Run")], ("via", STR), placeholder=True),
    # Run -> the Job it ran as: an application's appName = the Job's extracted appName (or the
    # <domain>_<stem> convention); a graph build -> scripts/build_graph_local.py
    _e("RAN_AS", [("Run", "Job")], *_s("via", "app_name"), placeholder=True),
    # child run -> the run its OpenLineage ParentRunFacet names (kind parent) / that run's root (kind root)
    _e("PARENT", [("Run", "Run")], ("kind", STR), placeholder=True),
    # newer -> the previous snapshot of the same table by sequence number (createOrReplace cuts parent_id;
    # parent_matches says whether parent_id agrees, sequence_gap counts numbers skipped by expiry)
    _e("SUPERSEDES", [("Snapshot", "Snapshot")], ("ordered_by", STR), ("parent_matches", BOOL),
       ("sequence_gap", I64), placeholder=True),
    # a graph build -> every snapshot it read, from manifest.json["iceberg"] (role input / output, the tag)
    _e("CONSUMED_SNAPSHOT", [("Run", "Snapshot")], *_s("role", "tag"), ("n_rows", I64), placeholder=True),
]}
# Edge types where a second edge between the same two nodes is merged into the first
# (property values are joined with ","), so a job that reads a table twice has one READS edge.
# RAN_AS / PARENT: the Iceberg and the OpenLineage overlays may both state the same run -> job (or parent) fact.
MERGED_EDGES = frozenset({"READS", "WRITES", "DEPENDS_ON", "COMPUTED_IN", "RUNS", "CONSUMES", "RAN_AS", "PARENT"})
RESERVED_PROPERTY_NAMES = frozenset({"order", "default", "table", "column", "desc", "group", "on", "in", "profile",
                                     "end", "exists", "from", "to", "limit"})


def check_schema() -> None:
    """Static sanity of the declaration itself (raises AssertionError on a typo)."""
    for n in NODE_SCHEMA.values():
        names = [c for c, _ in n.all_columns]
        assert len(names) == len(set(names)), f"{n.label}: duplicate property"
        assert not set(names) & RESERVED_PROPERTY_NAMES, f"{n.label}: reserved property name"
    for e in EDGE_SCHEMA.values():
        names = [c for c, _ in e.all_columns]
        assert len(names) == len(set(names)), f"{e.rel}: duplicate property"
        assert not set(names) & RESERVED_PROPERTY_NAMES, f"{e.rel}: reserved property name"
        for a, b in e.pairs:
            assert a in NODE_SCHEMA and b in NODE_SCHEMA, f"{e.rel}: unknown endpoint label {a}->{b}"


# --------------------------------------------------------------------------- business-graph bridge
GRAPH_PARQUET_DIR = "graph/parquet"          # Dataset ids of the business graph tables: ds:graph/parquet/<file>
GRAPH_TWIN_PREFIX = "lakehouse.gold.graph_"  # Iceberg twins published by the Spark graph job
# Union twins: one Iceberg table for every node (edge) table, partitioned by label (edge type).
# Normalised name after the prefix -> the Parquet file prefix it covers.
GRAPH_UNION_TWINS = {"nodes": "nodes", "node": "nodes", "edges": "edges", "edge": "edges"}
GRAPH_BUILD_PATH = "data/graph/<profile>/builds/<id>"   # canonical path of a build dir (Dataset.path prefix)


def graph_dataset(file: str) -> str:
    """Dataset name of a business-graph Parquet table (``nodes_Renewal.parquet``)."""
    return f"{GRAPH_PARQUET_DIR}/{file}"


def graph_twin_table(file: str) -> str:
    """Iceberg twin of a business-graph Parquet table: ``nodes_Renewal.parquet`` ->
    ``lakehouse.gold.graph_nodes_renewal`` (naming convention of this spec)."""
    return GRAPH_TWIN_PREFIX + file.rsplit(".", 1)[0].lower()


_SNAP = "silver.churn_subscription_snapshots"
_USAGE = "silver.churn_usage_daily"
_INV = "silver.churn_invoices"
_EVENTS = "silver.churn_subscription_events"
_LIMITS = "silver.churn_limit_events"
_SETTINGS = "silver.churn_overage_settings"
_CHARGES = "silver.churn_overage_charges"
_INCIDENTS = "silver.churn_incidents"
_TICKETS = "silver.churn_support_tickets"
_PRICING = "silver.churn_pricing_changes"


def _cols(dataset: str, *names: str) -> list[str]:
    return [f"{dataset}.{n}" for n in names]


def graph_bridge(gspec) -> dict[tuple[str, str], dict]:
    """(kind, name) -> {"sources": [ColumnRef, ...], "note": str} for every business graph
    node label and edge type (``gspec`` is ``lakehouse_graph.spec``).

    Declared knowledge about ``lakehouse_graph.build.build_tables``: which silver / gold
    columns each graph table is built from (through the pandas twin's ``silver()`` and
    ``gold()``). The assembler checks that the keys are exactly the business spec's node
    labels and edge types and that every ref resolves to a DataColumn.
    """
    gold = GOLD_DATASET_REF
    renewal_key = _cols(gold, "user_id", "renewal_date")
    billing = (_cols(_EVENTS, "subscription_id", "event_date", "event_type")
               + _cols(_INV, "subscription_id", "invoice_date", "status", "amount_usd", "attempt")
               + _cols(gold, "renewal_date", "feature_as_of"))
    return {
        ("node", "Subscription"): {
            "sources": _cols(_SNAP, "subscription_id", "user_name", "city", "plan_tier", "started_at"),
            "note": "one per subscription (its T-7 snapshot row); city stays a property"},
        ("node", "Renewal"): {
            "sources": (_cols(gold, "user_id", "plan_tier", "feature_as_of", "renewal_date",
                              *gspec.NUMERIC_FEATURES, "churned", "outcome", "route")
                        + _cols(_EVENTS, "subscription_id", "event_date", "event_type")
                        + _cols(_PRICING, "effective_date")),
            "note": "one per gold row; as_of = feature_as_of; outcome_observed_on from the canceled event; "
                    "cuts_so_far from pricing changes effective on or before as_of"},
        ("node", "Plan"): {
            "sources": _cols(gold, "plan_tier"),
            "note": "lookup dimension: tiers from gold, base allowance from the gold constants "
                    "(Parameter allowance.*), price from spec.PLAN_PRICE_USD"},
        ("node", "Incident"): {"sources": _cols(_INCIDENTS, "incident_id", "starts_on", "ends_on"),
                               "note": "global hub: declared incident windows"},
        ("node", "PricingChange"): {"sources": _cols(_PRICING, "change_id", "effective_date", "description"),
                                    "note": "global hub: cap cuts; cap_multiplier is Parameter cap_cut"},
        ("node", "LimitHit"): {
            "sources": _cols(_LIMITS, "subscription_id", "hit_at", "hit_date", "limit_type"),
            "note": "one per cap hit; event_date = hit_date; hit_at only orders the deterministic ids"},
        ("node", "OverageChange"): {"sources": _cols(_SETTINGS, "subscription_id", "changed_at", "overage"),
                                    "note": "one per overage setting change"},
        ("node", "OverageCharge"): {"sources": _cols(_CHARGES, "subscription_id", "charged_at", "amount_usd"),
                                    "note": "one per overage charge"},
        ("node", "Ticket"): {"sources": _cols(_TICKETS, "ticket_id", "subscription_id", "created_date"),
                             "note": "one per support ticket (no text)"},
        ("node", "BillingEvent"): {
            "sources": billing,
            "note": "cancel events + renewal-cycle invoices (invoice_date >= renewal_date); history invoices "
                    "stay aggregated in renewals_completed"},
        ("edge", "HAS_RENEWAL"): {"sources": renewal_key + _cols(gold, "feature_as_of"),
                                  "note": "Subscription -> its renewal (exactly one in renewal-graph/v1)"},
        ("edge", "ON_PLAN"): {"sources": renewal_key + _cols(gold, "plan_tier", "feature_as_of"),
                              "note": "Renewal -> Plan; tools never traverse through Plan between renewals"},
        ("edge", "HIT_LIMIT"): {"sources": _cols(_LIMITS, "subscription_id", "hit_at", "hit_date"),
                                "note": "event_date = hit_date; contains hits after as_of (the leak surface)"},
        ("edge", "CHANGED_OVERAGE"): {"sources": _cols(_SETTINGS, "subscription_id", "changed_at", "overage"),
                                      "note": "event_date = changed_at"},
        ("edge", "CHARGED_OVERAGE"): {"sources": _cols(_CHARGES, "subscription_id", "charged_at", "amount_usd"),
                                      "note": "event_date = charged_at"},
        ("edge", "OPENED"): {"sources": _cols(_TICKETS, "subscription_id", "ticket_id", "created_date"),
                             "note": "event_date = created_date"},
        ("edge", "BILLED"): {"sources": billing,
                             "note": "outcome_evidence unless cancel_scheduled on or before as_of"},
        ("edge", "EXPOSED_TO"): {
            "sources": (_cols(_USAGE, "subscription_id", "activity_date")
                        + _cols(_INCIDENTS, "incident_id", "starts_on", "ends_on")),
            "note": "one edge per active usage day inside a declared incident window"},
        ("edge", "FIRST_RENEWAL_AFTER"): {
            "sources": (_cols(_PRICING, "change_id", "effective_date") + renewal_key
                        + _cols(gold, "feature_as_of") + _cols(_SNAP, "started_at")),
            "note": "the gold rule (declared PIT exception), flagged known_by_as_of"},
        ("edge", "CUT_CAP"): {"sources": _cols(_PRICING, "change_id", "effective_date") + _cols(gold, "plan_tier"),
                              "note": "every pricing change cuts every plan's cap"},
        ("edge", "SIMILAR_TO"): {
            "sources": renewal_key + _cols(gold, gspec.BLOCK, "route", *gspec.FEATURES),
            "note": f"{gspec.SIMILAR_TO_SPEC_VERSION}: blocked kNN over {len(gspec.FEATURES)} gold features; "
                    f"candidates route = {gspec.REFERENCE_ROUTE}"},
    }
