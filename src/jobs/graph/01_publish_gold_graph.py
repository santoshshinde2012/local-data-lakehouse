"""Publish the renewal graph (renewal-graph/v1) into the lakehouse: gold.graph_* + tags.

The lakehouse-native twin of scripts/build_graph_local.py. It reads silver.churn_* and
gold.churn_renewal_features, each pinned to ONE snapshot id, computes every node and edge table
with sql/graph/{nodes,edges,similar_to}.sql (the Spark SQL twin of lakehouse_graph.build) and
writes four Iceberg tables:

  lakehouse.gold.graph_nodes              PARTITIONED BY (label): one typed column set per label
  lakehouse.gold.graph_edges              PARTITIONED BY (rel_type): every edge type incl. SIMILAR_TO
  lakehouse.gold.graph_similar_to_scaler  the z-score scaler fitted in SQL (population AVG /
                                          STDDEV_POP over route = 'model'); the kNN reads it back
  lakehouse.gold.graph_build_manifest     one row per input and output table of every publish (append)

Right after the writes it tags gold.churn_renewal_features, the ten silver inputs and every graph
table ``graph_<build_id> AS OF VERSION <the snapshot it read or wrote>``. Tags are Iceberg refs:
they survive createOrReplace and snapshot expiry, so every publish stays readable by its tag.

build_id = first 12 hex characters of sha256 over the input snapshot ids, the sha256 of this job
and of the SQL files, and the spec versions. The same inputs and code give the same build_id: a
second run finds the publish in the manifest, checks (or completes) its tags and exits 0 without
writing. A tag is never moved: a tag that points at another snapshot fails the run.

Readers pin by tag + snapshot id, never by timestamp: `scripts/build_graph_local.py --source
iceberg` (src/lakehouse_graph/iceberg_source.py, PyIceberg) re-reads the inputs at the tag,
rebuilds the graph with the canonical builder, checks this twin against it (counts exact,
SIMILAR_TO differences only at quantised ties) and records table uuid, tag and snapshot ids.

Self-contained for the ldl-spark image (pyspark + stdlib only: no numpy / pandas / pyarrow and no
repo package; it reads /opt/sql/graph and writes only Iceberg tables):
  docker compose -f docker-compose.yml -f docker-compose.graph.yml exec -T spark \\
    /opt/spark/bin/spark-submit --master 'local[*]' /opt/jobs/graph/01_publish_gold_graph.py
Needs gold.churn_renewal_features: run the churn pipeline first (make churn-e2e).
Environment: GRAPH_SQL_DIR (default /opt/sql/graph).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from string import Template

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SQL_DIR = Path(os.environ.get("GRAPH_SQL_DIR", "/opt/sql/graph"))
SQL_FILES = ("nodes.sql", "edges.sql", "similar_to.sql")
SPEC_GRAPH = "renewal-graph/v1"
SPEC_SIMILAR_TO = "similar_to/renewal-v1"
TAG_PREFIX = "graph_"

# Inputs, read at one snapshot each. Key = the name the SQL uses after $silver / $gold.
INPUT_TABLES = {
    "churn_subscription_snapshots": "lakehouse.silver.churn_subscription_snapshots",
    "churn_usage_daily": "lakehouse.silver.churn_usage_daily",
    "churn_invoices": "lakehouse.silver.churn_invoices",
    "churn_subscription_events": "lakehouse.silver.churn_subscription_events",
    "churn_limit_events": "lakehouse.silver.churn_limit_events",
    "churn_overage_settings": "lakehouse.silver.churn_overage_settings",
    "churn_overage_charges": "lakehouse.silver.churn_overage_charges",
    "churn_incidents": "lakehouse.silver.churn_incidents",
    "churn_support_tickets": "lakehouse.silver.churn_support_tickets",
    "churn_pricing_changes": "lakehouse.silver.churn_pricing_changes",
    "churn_renewal_features": "lakehouse.gold.churn_renewal_features",
}
NODES_TABLE = "lakehouse.gold.graph_nodes"
EDGES_TABLE = "lakehouse.gold.graph_edges"
SCALER_TABLE = "lakehouse.gold.graph_similar_to_scaler"
MANIFEST_TABLE = "lakehouse.gold.graph_build_manifest"
OUTPUT_TABLES = (NODES_TABLE, EDGES_TABLE, SCALER_TABLE)
# Label / type order of lakehouse_graph.spec.NODE_SCHEMA / EDGE_SCHEMA (tests/graph checks it).
NODE_LABELS = ("Subscription", "Renewal", "Plan", "Incident", "PricingChange", "LimitHit", "OverageChange",
               "OverageCharge", "Ticket", "BillingEvent")
EDGE_TYPES = ("HAS_RENEWAL", "ON_PLAN", "HIT_LIMIT", "CHANGED_OVERAGE", "CHARGED_OVERAGE", "OPENED", "BILLED",
              "EXPOSED_TO", "FIRST_RENEWAL_AFTER", "CUT_CAP", "SIMILAR_TO")
SCALER_COLUMNS = ("feature", "mean", "std", "n_ref", "spec_version")
SCALER_KIND = "sql: AVG / STDDEV_POP (population) over route = 'model', persisted, read back for the kNN"
SECTION = re.compile(r"^-- (view|table): (\w+)\s*$", re.M)


class PublishError(RuntimeError):
    """A precondition of the publish does not hold (missing input, moved tag, ...)."""


# --------------------------------------------------------------------------- SQL sections
def sections(path: Path) -> list[tuple[str, str, str]]:
    """[(kind, name, sql)] of one sql/graph file: '-- view: x' / '-- table: X' markers, in order."""
    text = path.read_text(encoding="utf-8")
    marks = list(SECTION.finditer(text))
    out = []
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        body = text[m.end():end].strip().rstrip(";").strip()
        out.append((m.group(1), m.group(2), body))
    if not out:
        raise PublishError(f"{path}: no '-- view:' / '-- table:' sections")
    return out


def render(sql: str, silver: str, gold: str) -> str:
    return Template(sql).substitute(silver=silver, gold=gold)


def q(session, statement: str):
    """Run one statement (metadata queries, tags, rendered sections)."""
    return session.sql(statement)


def run_sections(session, path: Path, silver: str, gold: str, only: tuple[str, ...] | None = None) -> dict:
    """Execute the sections of one file in order; every section is registered as a temp view
    (a table section as g_<name lower>); returns {name: DataFrame} for the table sections."""
    tables = {}
    for kind, name, sql in sections(path):
        if only is not None and name not in only:
            continue
        df = q(session, render(sql, silver, gold))
        df.createOrReplaceTempView(name if kind == "view" else f"g_{name.lower()}")
        if kind == "table":
            tables[name] = df
    return tables


def graph_tables(session, silver: str, gold: str, sql_dir: Path | None = None) -> tuple[dict, dict]:
    """(nodes, edges) DataFrames of every label / type except SIMILAR_TO, keyed by label / type."""
    d = sql_dir or SQL_DIR
    nodes = run_sections(session, d / "nodes.sql", silver, gold)
    edges = run_sections(session, d / "edges.sql", silver, gold)
    missing = [x for x in NODE_LABELS if x not in nodes] + [x for x in EDGE_TYPES[:-1] if x not in edges]
    if missing:
        raise PublishError(f"sql/graph defines no section for {missing}")
    return nodes, edges


def fit_scaler(session, sql_dir: Path | None = None):
    """The scaler fitted in SQL over the reference rows of the g_renewal view (not yet persisted)."""
    d = sql_dir or SQL_DIR
    run_sections(session, d / "similar_to.sql", "", "", only=("g_similar_to_scaler_fit",))
    return session.table("g_similar_to_scaler_fit").select(*SCALER_COLUMNS)


def similar_to(session, scaler, features: list[str] | None = None, sql_dir: Path | None = None):
    """SIMILAR_TO edges from the g_renewal view and ``scaler`` (bound as g_similar_to_scaler).

    The z-scores and the top-k are cached: each is read more than once (dst side, mutual join).
    ``features`` (optional) must equal the scaler's feature set.
    """
    d = sql_dir or SQL_DIR
    rows = scaler.select("feature", "std").collect()
    names = [r["feature"] for r in rows]
    if len(names) != len(set(names)) or (features is not None and sorted(names) != sorted(features)):
        raise PublishError(f"the scaler must hold each SIMILAR_TO feature exactly once, got {sorted(names)}")
    scaler.createOrReplaceTempView("g_similar_to_scaler")
    secs = {name: sql for _kind, name, sql in sections(d / "similar_to.sql")}
    z = q(session, secs["g_similar_to_z"]).persist()
    z.count()
    z.createOrReplaceTempView("g_similar_to_z")
    topk = q(session, secs["g_similar_to_topk"]).persist()
    topk.count()
    topk.createOrReplaceTempView("g_similar_to_topk")
    return q(session, secs["SIMILAR_TO"])


def union_wide(frames: dict, order: tuple[str, ...], kind_col: str, node_id: bool = False):
    """One wide table: kind_col (+ node_id = the key, the first column of a node section) + every
    property column in first-occurrence order; a column a label / type does not have is null there.
    Types come from the SQL casts, so a column shared by two labels must have one type in both."""
    out = None
    for name in order:
        df = frames[name]
        head = [F.lit(name).alias(kind_col)] + ([F.col(df.columns[0]).alias("node_id")] if node_id else [])
        df = df.select(*head, *df.columns)
        out = df if out is None else out.unionByName(df, allowMissingColumns=True)
    return out


# --------------------------------------------------------------------------- Iceberg metadata
def main_snapshot(session, table: str) -> int:
    rows = q(session, f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    if not rows:
        raise PublishError(f"{table} has no snapshot (never written)")
    return int(rows[0]["snapshot_id"])


def total_records(session, table: str, snapshot_id: int) -> int:
    rows = q(session, f"SELECT summary['total-records'] AS n FROM {table}.snapshots "
                      f"WHERE snapshot_id = {int(snapshot_id)}").collect()
    if not rows or rows[0]["n"] is None:
        raise PublishError(f"{table}: snapshot {snapshot_id} not found (expired?)")
    return int(rows[0]["n"])


def tag_target(session, table: str, tag: str) -> int | None:
    rows = q(session, f"SELECT snapshot_id, type FROM {table}.refs WHERE name = '{tag}'").collect()
    if not rows:
        return None
    if str(rows[0]["type"]).upper() != "TAG":
        raise PublishError(f"{table}: ref {tag} exists and is a {rows[0]['type']}, not a tag")
    return int(rows[0]["snapshot_id"])


def ensure_tag(session, table: str, tag: str, snapshot_id: int) -> str:
    """Create tag -> snapshot_id; 'created' or 'exists'. Never moves an existing tag."""
    have = tag_target(session, table, tag)
    if have is None:
        q(session, f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS {tag} AS OF VERSION {int(snapshot_id)}")
        have, how = tag_target(session, table, tag), "created"
    else:
        how = "exists"
    if have != snapshot_id:
        raise PublishError(f"{table}: tag {tag} points at snapshot {have}, not {snapshot_id}; refusing to move it")
    return how


def table_exists(session, table: str) -> bool:
    ns, name = table.rsplit(".", 1)
    return bool(q(session, f"SHOW TABLES IN {ns} LIKE '{name}'").collect())


# --------------------------------------------------------------------------- publish
def code_sha256(sql_dir: Path | None = None) -> dict[str, str]:
    d = sql_dir or SQL_DIR
    out = {"src/jobs/graph/01_publish_gold_graph.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    for name in SQL_FILES:
        p = d / name
        if not p.is_file():
            raise PublishError(f"{p} is missing (mount ./sql at /opt/sql, or set GRAPH_SQL_DIR)")
        out[f"sql/graph/{name}"] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def build_id_of(pins: dict[str, int], code: dict[str, str]) -> str:
    payload = {"inputs": {t: int(s) for t, s in sorted(pins.items())}, "code": dict(sorted(code.items())),
               "spec": {"graph": SPEC_GRAPH, "similar_to": SPEC_SIMILAR_TO}}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


def pin_inputs(spark) -> dict[str, int]:
    """{table: main snapshot id} of every input; each is bound as global_temp.<name> AT that snapshot."""
    pins = {}
    for view, table in INPUT_TABLES.items():
        try:
            spark.table(table)
        except AnalysisException as e:  # table or namespace not found
            raise PublishError(f"input {table} is missing ({type(e).__name__}): run the churn pipeline first "
                               f"(make churn-e2e, or the lakehouse_churn_features DAG)") from e
        pins[table] = main_snapshot(spark, table)
        spark.read.option("snapshot-id", str(pins[table])).table(table).createOrReplaceGlobalTempView(view)
    return pins


def published(session, build_id: str) -> list:
    if not table_exists(session, MANIFEST_TABLE):
        return []
    return session.table(MANIFEST_TABLE).where(F.col("build_id") == build_id).collect()


def complete_tags(session, rows: list, tag: str) -> dict[str, str]:
    """A publish already in the manifest: make sure every table it lists carries its tag."""
    out = {r["table_name"]: ensure_tag(session, r["table_name"], tag, int(r["snapshot_id"])) for r in rows}
    manifest_tag = tag_target(session, MANIFEST_TABLE, tag)
    if manifest_tag is None:
        out[MANIFEST_TABLE] = ensure_tag(session, MANIFEST_TABLE, tag, main_snapshot(session, MANIFEST_TABLE))
    return out


def part_counts(session, table: str, snapshot_id: int, col: str) -> dict[str, int]:
    df = session.read.option("snapshot-id", str(snapshot_id)).table(table)
    return {r[col]: int(r["count"]) for r in df.groupBy(col).count().collect()}


def main() -> None:
    spark = SparkSession.builder.appName("graph_01_publish_gold_graph").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        summary = publish(spark)
    except PublishError as e:
        print(f"Graph publish FAILED: {e}", file=sys.stderr)
        spark.stop()
        raise SystemExit(1) from e
    print("GRAPH_PUBLISH " + json.dumps(summary, sort_keys=True))
    spark.stop()


def publish(spark) -> dict:
    t0 = datetime.now(timezone.utc)
    pins = pin_inputs(spark)
    code = code_sha256()
    build_id = build_id_of(pins, code)
    tag = TAG_PREFIX + build_id
    print(f"==> graph publish {build_id}: {len(pins)} inputs pinned "
          f"(gold.churn_renewal_features @ {pins[INPUT_TABLES['churn_renewal_features']]})")

    rows = published(spark, build_id)
    if rows:
        tags = complete_tags(spark, rows, tag)
        print(f"    already published: {tag} on {len(tags)} tables "
              f"({sum(v == 'created' for v in tags.values())} tag(s) completed); nothing written")
        return {"build_id": build_id, "tag": tag, "status": "already_published",
                "tables": {r["table_name"]: int(r["snapshot_id"]) for r in rows}}

    nodes, edges = graph_tables(spark, "global_temp", "global_temp")
    fit_scaler(spark).writeTo(SCALER_TABLE).using("iceberg").createOrReplace()
    scaler_sid = main_snapshot(spark, SCALER_TABLE)
    scaler = spark.read.option("snapshot-id", str(scaler_sid)).table(SCALER_TABLE)
    edges["SIMILAR_TO"] = similar_to(spark, scaler)

    (union_wide(nodes, NODE_LABELS, "label", node_id=True).sortWithinPartitions("label", "node_id")
     .writeTo(NODES_TABLE).using("iceberg").partitionedBy(F.col("label")).createOrReplace())
    (union_wide(edges, EDGE_TYPES, "rel_type").sortWithinPartitions("rel_type", "src", "dst")
     .writeTo(EDGES_TABLE).using("iceberg").partitionedBy(F.col("rel_type")).createOrReplace())
    out = {t: (scaler_sid if t == SCALER_TABLE else main_snapshot(spark, t)) for t in OUTPUT_TABLES}
    parts = {NODES_TABLE: part_counts(spark, NODES_TABLE, out[NODES_TABLE], "label"),
             EDGES_TABLE: part_counts(spark, EDGES_TABLE, out[EDGES_TABLE], "rel_type")}

    app_id = spark.sparkContext.applicationId
    common = {"build_id": build_id, "tag": tag, "spec_graph": SPEC_GRAPH, "spec_similar_to": SPEC_SIMILAR_TO,
              "scaler_kind": SCALER_KIND, "code_sha256": json.dumps(code, sort_keys=True),
              "spark_version": spark.version, "spark_app_id": app_id}
    records = [{**common, "table_name": t, "role": "input", "snapshot_id": s,
                "row_count": total_records(spark, t, s), "parts": None} for t, s in pins.items()]
    records += [{**common, "table_name": t, "role": "output", "snapshot_id": s,
                 "row_count": total_records(spark, t, s),
                 "parts": json.dumps(parts[t], sort_keys=True) if t in parts else None} for t, s in out.items()]
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.graph_build_manifest (
          build_id STRING, tag STRING, table_name STRING, role STRING, snapshot_id BIGINT, row_count BIGINT,
          parts STRING, spec_graph STRING, spec_similar_to STRING, scaler_kind STRING, code_sha256 STRING,
          spark_version STRING, spark_app_id STRING, published_at TIMESTAMP
        ) USING iceberg
    """)
    cols = ["build_id", "tag", "table_name", "role", "snapshot_id", "row_count", "parts", "spec_graph",
            "spec_similar_to", "scaler_kind", "code_sha256", "spark_version", "spark_app_id"]
    manifest = (spark.createDataFrame([[r[c] for c in cols] for r in records],
                                      "build_id STRING, tag STRING, table_name STRING, role STRING, "
                                      "snapshot_id BIGINT, row_count BIGINT, parts STRING, spec_graph STRING, "
                                      "spec_similar_to STRING, scaler_kind STRING, code_sha256 STRING, "
                                      "spark_version STRING, spark_app_id STRING")
                .withColumn("published_at", F.lit(t0.replace(tzinfo=None)).cast("timestamp")))
    manifest.writeTo(MANIFEST_TABLE).append()

    # Tags right after the writes: inputs at the snapshot read, outputs at the snapshot written.
    tags = {t: ensure_tag(spark, t, tag, s) for t, s in {**pins, **out}.items()}
    tags[MANIFEST_TABLE] = ensure_tag(spark, MANIFEST_TABLE, tag, main_snapshot(spark, MANIFEST_TABLE))
    n_nodes, n_edges = sum(parts[NODES_TABLE].values()), sum(parts[EDGES_TABLE].values())
    n_similar = parts[EDGES_TABLE].get("SIMILAR_TO", 0)
    secs = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"    wrote {NODES_TABLE} ({n_nodes:,} rows, {len(parts[NODES_TABLE])} labels), {EDGES_TABLE} "
          f"({n_edges:,} rows, {len(parts[EDGES_TABLE])} types; SIMILAR_TO {n_similar:,}), "
          f"{SCALER_TABLE}, {MANIFEST_TABLE}")
    print(f"    tagged {tag} on {len(tags)} tables ({sum(v == 'created' for v in tags.values())} created) in "
          f"{secs:.1f} s; readers: build_graph_local.py --source iceberg [--iceberg-tag {tag}]")
    return {"build_id": build_id, "tag": tag, "status": "published", "seconds": round(secs, 1),
            "tables": {**{t: int(s) for t, s in pins.items()}, **{t: int(s) for t, s in out.items()}},
            "nodes": parts[NODES_TABLE], "edges": parts[EDGES_TABLE]}


if __name__ == "__main__":
    main()
