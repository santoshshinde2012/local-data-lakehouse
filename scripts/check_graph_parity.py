#!/usr/bin/env python3
"""Spark SQL twin of the renewal graph vs the pandas / numpy builder (local Spark, no Docker).

The lakehouse-native twin (src/jobs/graph/01_publish_gold_graph.py + sql/graph/*.sql) must build
the same node and edge tables as lakehouse_graph.build. This script runs the twin's SQL in PySpark
local mode and compares it with the canonical builder on the same inputs.

HARD GATE (exit 1 on any difference), same pandas input on both sides:
  * the pandas twin's silver frames and gold (scripts/build_churn_gold_local.py, loaded unchanged)
    are handed to Spark as the $silver / $gold views, the numpy builder runs on the same frames;
  * every node and edge table is equal cell for cell (floats bit for bit);
  * SIMILAR_TO with the PERSISTED scaler (the numpy build's similar_to_scaler): identical
    (src, dst, rank) for every edge and bit-identical d2, d2_q, dist, mutual.
REPORTED, not gated (a difference that is not a quantised tie is a warning; --strict fails):
  (a) the scaler fitted in SQL (AVG / STDDEV_POP, what the Spark job persists) on the same input:
      scaler deltas and SIMILAR_TO differences, each classified tie-only or not;
  (b) Spark gold as input (the check_gold_parity.py route: bronze CSV -> the user's
      silver_tables() -> sql/churn/gold_renewal_features.sql in Spark): the per-feature max |delta|
      of Spark gold vs pandas gold is MEASURED and recorded, then the whole twin runs on Spark silver
      + Spark gold + the SQL-fitted scaler (the lakehouse configuration) and is compared again.
Tie-only: every destination involved in a (dst, rank) difference of a source has a numpy d2_q
within 1 quantum of the others (PLAN 6.3 / 9.1).

It also owns two helpers of the twin:
  sql        sql/graph/similar_to.sql is GENERATED from lakehouse_graph.spec (FEATURES, K, QUANT,
             REFERENCE_ROUTE): `sql --write` regenerates it, `sql` checks it is current
  lakehouse  a Docker-free local lakehouse: Iceberg JdbcCatalog(SQLite) named `lakehouse` + a file
             warehouse; loads bronze -> silver -> gold with the user's churn jobs (reused
             unchanged) and publishes gold.graph_* + tags with the Spark job (in process, or its
             real file with spark-submit). Then read it back pinned by tag:
               scripts/build_graph_local.py build --source iceberg --catalog-uri sqlite:///<root>/catalog.db \\
                 --warehouse file://<root>/warehouse

Needs .venv-graph-spark (requirements-graph-spark.txt: pyspark 4.1.3 + PyIceberg) and a JDK 17 or 21
(GRAPH_JAVA_HOME / JAVA_HOME / the Zulu 17 path). The parity check needs no jar; `lakehouse` needs
iceberg-spark-runtime-4.1_2.13-1.12.0.jar and sqlite-jdbc-3.46.1.3.jar from ~/.ivy2, ~/.m2 or
$GRAPH_SPARK_JARS_DIR (never downloaded here). One JVM per process (local[2], 2 GB driver).

Usage:
  python scripts/check_graph_parity.py [parity] [--profile tiny|s42|default] [--sample-dir DIR]
                                       [--skip-spark-gold] [--strict] [--json out.json]
  python scripts/check_graph_parity.py sql [--write]
  python scripts/check_graph_parity.py lakehouse --root DIR [--profile tiny | --sample-dir DIR]
                                       [--graph-only] [--spark-submit]
"""
from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import build, spec  # noqa: E402 (after the sys.path line above)
from lakehouse_graph.iceberg_source import (  # noqa: E402 (shared with --source iceberg)
    gold_drift,
    local_arrow,
    scaler_deltas,
    similar_diff,
    table_diff,
    table_keys,
)

JOB = ROOT / "src/jobs/graph/01_publish_gold_graph.py"
SQL_DIR = ROOT / "sql/graph"
SIMILAR_TO_SQL = SQL_DIR / "similar_to.sql"
CATALOG = "lakehouse"
ICEBERG = ("org.apache.iceberg", "iceberg-spark-runtime-4.1_2.13", "1.12.0")
SQLITE_JDBC = ("org.xerial", "sqlite-jdbc", "3.46.1.3")
MAC_ZULU17 = "/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home"
JAVA_MAJORS = (17, 21)                 # the JDKs Spark 4.1 supports
# The exact DDL Iceberg 1.6.1 JdbcUtil emits for a V0 catalog (the shape of the Docker stack's
# Postgres catalog). Pre-creating it + jdbc.init-catalog-tables=false avoids the Iceberg 1.6.1
# SQLite lock leak (initializeCatalogTables() leaves a ResultSet open: SQLITE_BUSY on commit as
# soon as a second catalog instance exists, e.g. after DataFrame.cache()).
V0_CATALOG_DDL = (
    "CREATE TABLE iceberg_tables(catalog_name VARCHAR(255) NOT NULL,table_namespace VARCHAR(255) NOT NULL,"
    "table_name VARCHAR(255) NOT NULL,metadata_location VARCHAR(1000),previous_metadata_location VARCHAR(1000),"
    "PRIMARY KEY (catalog_name, table_namespace, table_name))",
    "CREATE TABLE iceberg_namespace_properties(catalog_name VARCHAR(255) NOT NULL,namespace VARCHAR(255) NOT NULL,"
    "property_key VARCHAR(255),property_value VARCHAR(1000),PRIMARY KEY (catalog_name, namespace, property_key))",
)
# pandas twin silver key -> the Spark silver table name (the SQL's $silver.<name>)
SILVER_TABLES = {
    "snapshots": "churn_subscription_snapshots", "usage": "churn_usage_daily", "invoices": "churn_invoices",
    "sub_events": "churn_subscription_events", "limits": "churn_limit_events",
    "overage_settings": "churn_overage_settings", "overage_charges": "churn_overage_charges",
    "incidents": "churn_incidents", "tickets": "churn_support_tickets", "pricing": "churn_pricing_changes",
}
GOLD_VIEW = "churn_renewal_features"


class HarnessUnavailable(RuntimeError):
    """Java 17, pyspark or a required jar is missing: tests skip, this script exits 2."""


# --------------------------------------------------------------------------- generated SQL
def render_similar_to_sql() -> str:
    """sql/graph/similar_to.sql from the spec (the file is committed; `sql` checks it is current)."""
    f, k, ref, version = list(spec.FEATURES), spec.K, spec.REFERENCE_ROUTE, spec.SIMILAR_TO_SPEC_VERSION
    quant = f"{spec.QUANT}D"
    pad = "         "
    fit_structs = (",\n" + pad).join(
        f"named_struct('feature', '{x}', 'mean', m_{x}, 'std', s_{x})" for x in f)
    fit_aggs = (",\n" + pad).join(
        f"AVG(CAST({x} AS DOUBLE)) AS m_{x}, STDDEV_POP(CAST({x} AS DOUBLE)) AS s_{x}" for x in f)
    wide = (",\n" + pad).join(
        f"MAX(CASE WHEN feature = '{x}' THEN mean END) AS m_{x}, MAX(CASE WHEN feature = '{x}' THEN std END) AS s_{x}"
        for x in f)
    z = (",\n" + pad).join(
        f"CASE WHEN s.s_{x} > 0 THEN (CAST(r.{x} AS DOUBLE) - s.m_{x}) / s.s_{x} ELSE 0D END AS z_{x}" for x in f)
    d2 = ("\n" + pad + "     + ").join(f"(a.z_{x} - b.z_{x}) * (a.z_{x} - b.z_{x})" for x in f)
    return f"""-- SIMILAR_TO (spec {version}) in Spark SQL: the lakehouse-native twin of
-- lakehouse_graph.build.similar_to_full(). GENERATED from src/lakehouse_graph/spec.py
-- (FEATURES, K = {k}, QUANT = {spec.QUANT}, REFERENCE_ROUTE = '{ref}') by
--   python scripts/check_graph_parity.py sql --write
-- Do not edit by hand: tests/graph/test_parity_sql.py fails when this file and the spec disagree.
--
-- Inputs (temporary views the caller registers; no placeholders in this file):
--   g_renewal            Renewal node rows (renewal_id, plan_tier, route and the {len(f)} features)
--   g_similar_to_scaler  the PERSISTED scaler rows (feature, mean, std, n_ref, spec_version). The
--                        Spark job fits it with the first section, writes it to
--                        lakehouse.gold.graph_similar_to_scaler and reads it back; the parity
--                        check's hard gate binds the numpy build's similar_to_scaler.parquet.
-- Rules that make the result bit-identical to the numpy builder given the same input and scaler:
--   z_f  = CASE WHEN std_f > 0 THEN (CAST(x_f AS DOUBLE) - mean_f) / std_f ELSE 0D END
--   d2   = t_1 + t_2 + ... + t_{len(f)} with t_f = (a.z_f - b.z_f) * (a.z_f - b.z_f), in feature order
--          (Spark keeps the left-to-right order of a sum of non-constant doubles)
--   d2_q = CAST(FLOOR(d2 * {quant} + 0.5D) AS BIGINT), the one rounding mode of every engine
--   rank = ROW_NUMBER() OVER (PARTITION BY src ORDER BY d2_q, dst), rank <= {k}; self excluded;
--          candidates: route = '{ref}' in the same plan_tier. rid is a dense INT assigned in
--          renewal_id order, so ordering by rid is ordering by renewal_id
--   dist = SQRT(d2); mutual = the reverse edge is also in the top {k}
-- Performance: the sources are repartitioned by rid (a small table is one input partition) and
-- the candidates broadcast; the caller caches g_similar_to_z and g_similar_to_topk.

-- view: g_similar_to_scaler_fit
-- The scaler fitted in SQL: population mean and std (ddof = 0) over the reference set.
SELECT inline(array(
         {fit_structs})),
       n_ref, '{version}' AS spec_version
FROM (
  SELECT {fit_aggs},
         COUNT(*) AS n_ref
  FROM g_renewal
  WHERE route = '{ref}'
) a;

-- view: g_similar_to_z
SELECT r.renewal_id, r.plan_tier, (r.route = '{ref}') AS is_ref,
       CAST(ROW_NUMBER() OVER (ORDER BY r.renewal_id) AS INT) AS rid,
       {z}
FROM g_renewal r
CROSS JOIN (
  SELECT {wide}
  FROM g_similar_to_scaler
) s;

-- view: g_similar_to_topk
SELECT a.renewal_id AS src, b.renewal_id AS dst, t.rank, t.d2, t.d2_q
FROM (
  SELECT src_rid, dst_rid, CAST(rank AS BIGINT) AS rank, d2, d2_q
  FROM (
    SELECT src_rid, dst_rid, d2, d2_q,
           ROW_NUMBER() OVER (PARTITION BY src_rid ORDER BY d2_q, dst_rid) AS rank
    FROM (
      SELECT src_rid, dst_rid, d2, CAST(FLOOR(d2 * {quant} + 0.5D) AS BIGINT) AS d2_q
      FROM (
        SELECT /*+ BROADCAST(b) */ a.rid AS src_rid, b.rid AS dst_rid,
               {d2} AS d2
        FROM (SELECT /*+ REPARTITION(8, rid) */ * FROM g_similar_to_z) a
        JOIN (SELECT * FROM g_similar_to_z WHERE is_ref) b
          ON a.plan_tier = b.plan_tier AND a.rid <> b.rid
      ) pairs
    ) keyed
  ) ranked
  WHERE rank <= {k}
) t
JOIN g_similar_to_z a ON a.rid = t.src_rid
JOIN g_similar_to_z b ON b.rid = t.dst_rid;

-- table: SIMILAR_TO
SELECT t.src, t.dst, t.rank, t.d2, t.d2_q, SQRT(t.d2) AS dist, (r.src IS NOT NULL) AS mutual,
       '{version}' AS spec_version
FROM g_similar_to_topk t
LEFT JOIN g_similar_to_topk r ON r.src = t.dst AND r.dst = t.src;
"""


def cmd_sql(a) -> int:
    want = render_similar_to_sql()
    if a.write:
        SIMILAR_TO_SQL.write_text(want, encoding="utf-8")
        print(f"wrote {SIMILAR_TO_SQL.relative_to(ROOT)} ({len(want.splitlines())} lines) from lakehouse_graph.spec")
        return 0
    have = SIMILAR_TO_SQL.read_text(encoding="utf-8") if SIMILAR_TO_SQL.is_file() else ""
    if have != want:
        print(f"{SIMILAR_TO_SQL.relative_to(ROOT)} is not what lakehouse_graph.spec generates: run "
              f"python scripts/check_graph_parity.py sql --write", file=sys.stderr)
        return 1
    print(f"{SIMILAR_TO_SQL.relative_to(ROOT)} is current (generated from lakehouse_graph.spec)")
    return 0


# --------------------------------------------------------------------------- local Spark harness
def _java_major(home: str) -> int | None:
    exe = Path(home) / "bin" / "java"
    if not exe.is_file():
        return None
    try:
        out = subprocess.run([str(exe), "-version"], capture_output=True, text=True, timeout=20).stderr
    except (OSError, subprocess.TimeoutExpired):
        return None
    for tok in out.split('"')[1:2]:          # openjdk version "17.0.17" 2025-10-21 LTS
        head = tok.split(".")[0]
        return int(head) if head.isdigit() else None
    return None


def find_java_home() -> str | None:
    """JDK 17 or 21 (what Spark 4.1 supports): $GRAPH_JAVA_HOME (strict when set), else $JAVA_HOME, the Zulu 17
    path, java_home -v 17 / 21."""
    override = os.environ.get("GRAPH_JAVA_HOME")
    if override:
        return override if _java_major(override) in JAVA_MAJORS else None
    cands = [os.environ.get("JAVA_HOME"), MAC_ZULU17]
    if sys.platform == "darwin" and Path("/usr/libexec/java_home").exists():
        with contextlib.suppress(OSError, subprocess.TimeoutExpired):
            for major in JAVA_MAJORS:
                r = subprocess.run(["/usr/libexec/java_home", "-v", str(major)], capture_output=True, text=True,
                                   timeout=20)
                if r.returncode == 0:
                    cands.append(r.stdout.strip())
    return next((c for c in cands if c and _java_major(c) in JAVA_MAJORS), None)


def find_jar(group: str, artifact: str, version: str) -> Path | None:
    """A jar from the local caches, never a download. $GRAPH_SPARK_JARS_DIR is strict when set."""
    name = f"{artifact}-{version}.jar"
    if os.environ.get("GRAPH_SPARK_JARS_DIR"):
        p = Path(os.environ["GRAPH_SPARK_JARS_DIR"]) / name
        return p if p.is_file() else None
    home = Path.home()
    for c in (home / ".ivy2" / "cache" / group / artifact / "jars" / name, home / ".ivy2" / "jars" / f"{group}_{name}",
              home / ".m2" / "repository" / Path(*group.split(".")) / artifact / version / name):
        if c.is_file():
            return c
    return None


def required_jars() -> list[Path]:
    out = []
    for gav in (ICEBERG, SQLITE_JDBC):
        p = find_jar(*gav)
        if p is None:
            raise HarnessUnavailable("jar not found locally: {}:{}:{} (set GRAPH_SPARK_JARS_DIR)".format(*gav))
        out.append(p)
    return out


def skip_reason(iceberg: bool = True) -> str | None:
    """None when a local Spark (+ Iceberg jars if ``iceberg``) can start here, else a one-line reason."""
    if importlib.util.find_spec("pyspark") is None:
        return "pyspark is not installed in this interpreter (use .venv-graph-spark)"
    if find_java_home() is None:
        return "no JDK 17 or 21 found (set GRAPH_JAVA_HOME or JAVA_HOME)"
    if iceberg:
        try:
            required_jars()
        except HarnessUnavailable as e:
            return str(e)
    return None


def ensure_catalog_db(catalog_db: Path) -> bool:
    """Create the SQLite catalog with Iceberg's V0 tables if it has none (True if created)."""
    catalog_db = Path(catalog_db)
    catalog_db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(catalog_db)
    try:
        have = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        want = {"iceberg_tables", "iceberg_namespace_properties"}
        if want <= have:
            return False
        if have & want:
            raise RuntimeError(f"{catalog_db} holds a partial Iceberg catalog: {sorted(have)}")
        for ddl in V0_CATALOG_DDL:
            con.execute(ddl)
        con.commit()
        return True
    finally:
        con.close()


def spark_conf(catalog_db: Path | None, warehouse: Path | None, scratch: Path, *, driver_memory: str = "2g",
               master: str = "local[2]") -> dict[str, str]:
    """spark.* settings: plain local Spark (catalog_db None) or catalog `lakehouse` = Iceberg
    JdbcCatalog(SQLite) + file warehouse (same catalog-impl and extension as config/spark-defaults.conf;
    only uri and warehouse differ). Calls ensure_catalog_db() itself (jdbc.init-catalog-tables=false)."""
    conf = {
        "spark.master": master,
        "spark.driver.memory": driver_memory,
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.sql.warehouse.dir": str(scratch / "spark-warehouse"),   # never ./spark-warehouse in the repo
        "spark.local.dir": str(scratch / "spark-local"),
    }
    if catalog_db is not None:
        ensure_catalog_db(Path(catalog_db))
        conf.update({
            "spark.jars": ",".join(str(j) for j in required_jars()),
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": CATALOG,
            f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{CATALOG}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            f"spark.sql.catalog.{CATALOG}.default-namespace": "bronze",
            f"spark.sql.catalog.{CATALOG}.uri": f"jdbc:sqlite:{Path(catalog_db).absolute()}",
            f"spark.sql.catalog.{CATALOG}.warehouse": f"file://{Path(warehouse).absolute()}",
            f"spark.sql.catalog.{CATALOG}.jdbc.init-catalog-tables": "false",
        })
    return conf


def prepare_env() -> None:
    """Environment for a deterministic local JVM; call before the first SparkSession."""
    jh = find_java_home()
    if jh is None:
        raise HarnessUnavailable("no JDK 17 or 21 found (set GRAPH_JAVA_HOME or JAVA_HOME)")
    os.environ.update({"JAVA_HOME": jh, "SPARK_LOCAL_IP": "127.0.0.1", "PYSPARK_PYTHON": sys.executable,
                       "PYSPARK_DRIVER_PYTHON": sys.executable, "TZ": "UTC", "PYTHONDONTWRITEBYTECODE": "1"})
    os.environ.pop("SPARK_HOME", None)          # a stray SPARK_HOME would pick another Spark build
    os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
    if hasattr(time, "tzset"):
        time.tzset()


def build_session(scratch: Path, catalog_db: Path | None = None, warehouse: Path | None = None, *,
                  app_name: str = "graph_parity", driver_memory: str = "2g"):
    """A local SparkSession (one per process: a second one would silently reuse the first JVM's settings)."""
    prepare_env()
    from pyspark.sql import SparkSession

    if SparkSession.getActiveSession() is not None:
        raise RuntimeError("a SparkSession is already active in this process; stop it first")
    scratch.mkdir(parents=True, exist_ok=True)
    if warehouse is not None:
        Path(warehouse).mkdir(parents=True, exist_ok=True)
    b = SparkSession.builder.appName(app_name)
    for key, value in spark_conf(catalog_db, warehouse, scratch, driver_memory=driver_memory).items():
        b = b.master(value) if key == "spark.master" else b.config(key, value)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_module(path: Path, name: str):
    """importlib load by path (the check_gold_parity.py pattern) that writes no __pycache__."""
    prev = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        mspec = importlib.util.spec_from_file_location(name, path)
        if mspec is None or mspec.loader is None:
            raise ImportError(f"cannot load {path}")
        mod = importlib.util.module_from_spec(mspec)
        mspec.loader.exec_module(mod)
        return mod
    finally:
        sys.dont_write_bytecode = prev


def graph_job():
    """The Spark job module (pyspark + stdlib only), with its SQL directory set to this checkout."""
    os.environ["GRAPH_SQL_DIR"] = str(SQL_DIR)   # read by the job at import
    return load_module(JOB, "graph_01_publish_gold_graph")


def churn_jobs(sample_dir: Path):
    """(bronze, silver, gold) modules of src/jobs/churn, unchanged; they read CHURN_* at import."""
    os.environ["CHURN_SAMPLE_DIR"] = str(sample_dir)
    os.environ["CHURN_GOLD_SQL"] = str(ROOT / "sql/churn/gold_renewal_features.sql")
    jobs = ROOT / "src/jobs/churn"
    return (load_module(jobs / "01_ingest_bronze.py", "churn_01_ingest_bronze"),
            load_module(jobs / "02_transform_silver.py", "churn_02_transform_silver"),
            load_module(jobs / "03_publish_gold_features.py", "churn_03_publish_gold_features"))


def _replace_table(df, table: str) -> None:
    """createOrReplace an Iceberg table of the LOCAL catalog (a helper, so the lineage extractor
    never attributes a write of the real lakehouse to this script)."""
    df.writeTo(table).using("iceberg").createOrReplace()


def _materialise(spark, df, path: Path, view: str) -> None:
    """Write ``df`` to Parquet and bind the re-read table as global_temp.<view>: the twin then reads
    a materialised table, as it does from Iceberg (a view over the gold SQL would be re-planned in
    every section and trips a Spark 3.5.3 exchange-reuse bug: "Couldn't find <col> in [...]")."""
    df.write.mode("overwrite").parquet(str(path))
    spark.read.parquet(str(path)).createOrReplaceGlobalTempView(view)


def spark_silver_gold(spark, sample_dir: Path, scratch: Path | None = None, *, to_iceberg: bool = False) -> dict:
    """bronze CSV -> the user's silver_tables() -> the user's gold SQL, the check_gold_parity.py route.

    to_iceberg False: silver + gold materialised under ``scratch`` and bound as global temp views
    (parity). True: real Iceberg tables lakehouse.{bronze,silver,gold}.* of the local catalog, written
    like the churn jobs write them (writeTo(...).using("iceberg").createOrReplace()).
    Returns per-stage seconds.
    """
    from pyspark.sql import functions as F

    bronze, silver, gold_job = churn_jobs(sample_dir)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    t: dict[str, float] = {}

    def read_bronze(table: str):
        csv, schema = bronze.BRONZE[table]
        return (spark.read.option("header", True).schema(schema).csv(str(sample_dir / csv))
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_ingested_at", F.lit(now).cast("timestamp")))

    t0 = time.perf_counter()
    if to_iceberg:
        for ns in ("bronze", "silver", "gold"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
        for table in bronze.BRONZE:
            _replace_table(read_bronze(table), f"{CATALOG}.bronze.{table}")
        t["bronze"] = round(time.perf_counter() - t0, 2)
        t0 = time.perf_counter()
        for name, df in silver.silver_tables(lambda x: spark.read.table(f"{CATALOG}.bronze.{x}")).items():
            _replace_table(df.drop(*[c for c in silver.LINEAGE if c in df.columns]), f"{CATALOG}.silver.{name}")
        t["silver"] = round(time.perf_counter() - t0, 2)
        t0 = time.perf_counter()
        gold = spark.sql(gold_job.gold_sql()).withColumn(
            "built_at", F.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        _replace_table(gold, f"{CATALOG}.gold.churn_renewal_features")
        t["gold"] = round(time.perf_counter() - t0, 2)
        return t
    if scratch is None:
        raise ValueError("spark_silver_gold(to_iceberg=False) needs a scratch directory")
    for name, df in silver.silver_tables(read_bronze).items():
        _materialise(spark, df.drop(*[c for c in silver.LINEAGE if c in df.columns]), scratch / f"silver_{name}", name)
    t["silver"] = round(time.perf_counter() - t0, 2)
    t0 = time.perf_counter()
    _materialise(spark, spark.sql(gold_job.gold_sql(silver="global_temp")), scratch / "gold", GOLD_VIEW)
    t["gold"] = round(time.perf_counter() - t0, 2)
    return t


# --------------------------------------------------------------------------- frames <-> Spark
def _arrow_for_spark(df: pd.DataFrame, timestamps: tuple[str, ...] = ()) -> pa.Table:
    """A pandas twin frame as Arrow with Spark-silver types: date-grained datetimes -> date32."""
    arrays, fields = [], []
    for c in df.columns:
        s = df[c]
        if s.dtype.kind == "M" and c not in timestamps:
            arrays.append(pa.array(s.to_numpy(dtype="datetime64[D]"), type=pa.date32(), from_pandas=True))
        elif s.dtype.kind == "M":
            arrays.append(pa.array(s.to_numpy(dtype="datetime64[us]"), type=pa.timestamp("us"), from_pandas=True))
        elif s.dtype.kind in "iub":
            arrays.append(pa.array(s.to_numpy(), from_pandas=True))
        elif s.dtype.kind == "f":
            arrays.append(pa.array(s.to_numpy(dtype=np.float64), type=pa.float64(), from_pandas=True))
        else:
            arrays.append(pa.array(s.astype(object).where(s.notna(), None).tolist(), type=pa.string()))
        fields.append(c)
    return pa.Table.from_arrays(arrays, names=fields)


def bind_pandas_inputs(spark, silver: dict, gold: pd.DataFrame, scratch: Path) -> None:
    """The pandas twin's silver frames + gold as global temp views ($silver / $gold = global_temp)."""
    scratch.mkdir(parents=True, exist_ok=True)
    frames = {SILVER_TABLES[k]: _arrow_for_spark(v, ("hit_at",) if k == "limits" else ()) for k, v in silver.items()}
    g = gold.copy()
    for c in ("feature_as_of", "renewal_date"):
        g[c] = pd.to_datetime(g[c])
    frames[GOLD_VIEW] = _arrow_for_spark(g.drop(columns=["built_at"], errors="ignore"))
    for name, tbl in frames.items():
        path = scratch / f"{name}.parquet"
        pq.write_table(tbl, path)
        spark.read.parquet(str(path)).createOrReplaceGlobalTempView(name)


def bind_pandas_scaler(spark, scaler: pd.DataFrame):
    rows = [(str(r.feature), float(r.mean), float(r.std), int(r.n_ref), str(r.spec_version))
            for r in scaler.itertuples(index=False)]
    return spark.createDataFrame(rows, "feature STRING, mean DOUBLE, std DOUBLE, n_ref BIGINT, spec_version STRING")


def spark_arrow(df, schema: pa.Schema, scratch: Path, name: str) -> pa.Table:
    """A Spark DataFrame as an Arrow table with the spec schema (written to Parquet: exact values)."""
    path = scratch / "out" / name
    df.select(*[f.name for f in schema]).coalesce(1).write.mode("overwrite").parquet(str(path))
    tbl = pq.read_table(str(path))
    return pa.Table.from_arrays([tbl.column(f.name).cast(f.type) for f in schema], schema=schema)


# --------------------------------------------------------------------------- parity run
def _twin_arrow(spark, nodes: dict, edges: dict, scratch: Path, prefix: str) -> dict[str, pa.Table]:
    out = {}
    for label, ns in spec.NODE_SCHEMA.items():
        out[label] = spark_arrow(nodes[label], ns.schema, scratch, f"{prefix}_nodes_{label}")
    for rel, es in spec.EDGE_SCHEMA.items():
        out[rel] = spark_arrow(edges[rel], es.schema, scratch, f"{prefix}_edges_{rel}")
    return out


def compare_all(local: dict[str, pa.Table], twin: dict[str, pa.Table], *, skip: tuple[str, ...] = ()) -> dict:
    return {name: table_diff(local[name], twin[name], table_keys(name)) for name in local if name not in skip}


def _scaler_frame(spark_df) -> pd.DataFrame:
    rows = [r.asDict() for r in spark_df.collect()]
    order = {f: i for i, f in enumerate(spec.FEATURES)}
    return pd.DataFrame(sorted(rows, key=lambda r: order.get(r["feature"], 99)))[list(spec.SCALER_SCHEMA.names)]


def resolve_sample(a) -> Path:
    if a.sample_dir:
        return Path(a.sample_dir).absolute()
    root = Path(a.graph_root).absolute() if a.graph_root else None
    return spec.sample_dir(a.profile, root)


def cmd_parity(a) -> int:
    sample = resolve_sample(a)
    build.check_bronze(sample, a.profile)
    reason = skip_reason(iceberg=False)
    if reason:
        print(f"Graph parity SKIPPED: {reason}", file=sys.stderr)
        return 2
    t_start = time.perf_counter()
    scratch = Path(tempfile.mkdtemp(prefix="graph-parity-", dir=a.scratch)) if a.scratch else \
        Path(tempfile.mkdtemp(prefix="graph-parity-"))
    report: dict = {"sample_dir": str(sample), "profile": a.profile, "spec": dict(spec.SPEC_VERSIONS),
                    "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "timings_s": {}}
    errors, warnings = [], []
    print(f"==> graph parity: Spark SQL twin (sql/graph) vs the numpy builder on {sample}")
    t0 = time.perf_counter()
    mod = build.load_gold_twin(sample)
    silver, gold, today = build.run_gold(mod)
    tables = build.build_tables(silver, gold, today, build.gold_constants(mod))
    local = local_arrow(tables)
    report["timings_s"]["numpy_build"] = round(time.perf_counter() - t0, 2)
    job = graph_job()
    spark = build_session(scratch / "spark", app_name="graph_parity", driver_memory=a.driver_memory)
    try:
        # ---- hard gate: same pandas input, persisted numpy scaler
        t0 = time.perf_counter()
        bind_pandas_inputs(spark, silver, gold, scratch / "inputs")
        nodes, edges = job.graph_tables(spark, "global_temp", "global_temp", SQL_DIR)
        edges["SIMILAR_TO"] = job.similar_to(spark, bind_pandas_scaler(spark, tables["similar_to_scaler"]),
                                             list(spec.FEATURES), SQL_DIR)
        twin = _twin_arrow(spark, nodes, edges, scratch, "gate")
        report["timings_s"]["spark_gate"] = round(time.perf_counter() - t0, 2)
        gate = compare_all(local, twin)
        report["hard_gate"] = {"tables": gate, "similar_to": similar_diff(
            tables["SIMILAR_TO"], twin["SIMILAR_TO"].to_pandas(), tables["Renewal"], tables["similar_to_scaler"])}
        bad = {k: v for k, v in gate.items() if v["columns"]}
        sim = report["hard_gate"]["similar_to"]
        n_sim = len(tables["SIMILAR_TO"])
        if bad:
            errors.append(f"hard gate: {len(bad)} table(s) differ between the Spark SQL twin and the numpy builder "
                          f"on the same input: {json.dumps(bad, default=str)[:800]}")
        else:
            print(f"  ok    hard gate: all {len(gate)} node/edge tables equal cell for cell (floats bit for bit) on "
                  f"the same pandas input; SIMILAR_TO {n_sim:,} edges: identical (src, dst, rank), bit-identical d2, "
                  f"d2_q, dist, mutual with the persisted scaler")
        if sim["only_local"] or sim["only_twin"] or sim["rank_differences"] or sim["d2_bits_differ"]:
            errors.append(f"hard gate SIMILAR_TO: {json.dumps(sim)[:600]}")

        # ---- (a) the scaler fitted in SQL, same input (what the Spark job persists): reported
        t0 = time.perf_counter()
        fitted = _scaler_frame(job.fit_scaler(spark, SQL_DIR))
        sim_fit = job.similar_to(spark, bind_pandas_scaler(spark, fitted), list(spec.FEATURES), SQL_DIR)
        twin_fit = spark_arrow(sim_fit, spec.EDGE_SCHEMA["SIMILAR_TO"].schema, scratch, "fit_SIMILAR_TO").to_pandas()
        report["timings_s"]["spark_fitted_scaler"] = round(time.perf_counter() - t0, 2)
        report["fitted_scaler"] = {"scaler": scaler_deltas(tables["similar_to_scaler"], fitted),
                                   "similar_to": similar_diff(tables["SIMILAR_TO"], twin_fit, tables["Renewal"],
                                                              tables["similar_to_scaler"])}
        fs = report["fitted_scaler"]
        msg = (f"(a) SQL-fitted scaler: {fs['scaler']['mean_bit_equal']}/{fs['scaler']['features']} means and "
               f"{fs['scaler']['std_bit_equal']}/{fs['scaler']['features']} stds bit-equal (max |d std| "
               f"{fs['scaler']['max_abs_std_delta']:.2e}); SIMILAR_TO: {fs['similar_to']['only_local']} edges only "
               f"numpy / {fs['similar_to']['only_twin']} only Spark, {fs['similar_to']['rank_differences']} rank "
               f"differences, {fs['similar_to']['d2_bits_differ']:,} d2 differ (max |d| "
               f"{fs['similar_to']['max_abs_d2_delta']:.2e}), tie-only {fs['similar_to']['tie_only']}")
        if fs["similar_to"]["tie_only"]:
            print(f"  note  {msg}")
        else:
            warnings.append(msg + " (NOT tie-only)")

        # ---- (b) Spark gold as input (the lakehouse configuration): reported
        if a.skip_spark_gold:
            report["spark_gold"] = {"skipped": True}
            print("  note  (b) Spark gold input: skipped (--skip-spark-gold)")
        else:
            t0 = time.perf_counter()
            report["spark_gold_stages_s"] = spark_silver_gold(spark, sample, scratch / "spark_inputs")
            sg = spark.table(f"global_temp.{GOLD_VIEW}")
            spark_gold = pd.DataFrame([r.asDict() for r in sg.collect()])
            drift = gold_drift(spark_gold, gold)
            nodes_b, edges_b = job.graph_tables(spark, "global_temp", "global_temp", SQL_DIR)
            fitted_b = job.fit_scaler(spark, SQL_DIR)
            edges_b["SIMILAR_TO"] = job.similar_to(spark, bind_pandas_scaler(spark, _scaler_frame(fitted_b)),
                                                   list(spec.FEATURES), SQL_DIR)
            twin_b = _twin_arrow(spark, nodes_b, edges_b, scratch, "sparkgold")
            report["timings_s"]["spark_gold_input"] = round(time.perf_counter() - t0, 2)
            diffs_b = compare_all(local, twin_b, skip=("SIMILAR_TO",))
            sim_b = similar_diff(tables["SIMILAR_TO"], twin_b["SIMILAR_TO"].to_pandas(), tables["Renewal"],
                                 tables["similar_to_scaler"])
            report["spark_gold"] = {"drift": drift, "tables": {k: v for k, v in diffs_b.items() if v["columns"]},
                                    "similar_to": sim_b}
            cells = {k: {c: x.get("cells", x) for c, x in v["columns"].items()}
                     for k, v in diffs_b.items() if v["columns"]}
            print(f"  note  (b) Spark gold vs pandas gold: {drift['cells_differ']} feature cells differ over "
                  f"{drift['rows'][0]:,} renewals, max |delta| {drift['max_abs_delta']:.3g} "
                  f"{json.dumps(drift['by_feature'])}; labels {drift['label_cells_differ'] or 'equal'}")
            msg = (f"(b) twin on Spark silver + Spark gold + SQL-fitted scaler: node/edge cells differ "
                   f"{cells or 'none'}; SIMILAR_TO {sim_b['only_local']} only numpy / {sim_b['only_twin']} only "
                   f"Spark, {sim_b['rank_differences']} rank differences, tie-only {sim_b['tie_only']}")
            if sim_b["tie_only"]:
                print(f"  note  {msg}")
            else:
                warnings.append(msg + " (NOT tie-only)")
    finally:
        spark.stop()
        if not a.keep_scratch:
            shutil.rmtree(scratch, ignore_errors=True)
    report["timings_s"]["total"] = round(time.perf_counter() - t_start, 2)
    report["errors"], report["warnings"] = errors, warnings
    if a.json:
        Path(a.json).write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n")
    for w in warnings:
        print(f"WARN: {w}", file=sys.stderr)
    if errors or (a.strict and warnings):
        print("Graph parity FAILED:", file=sys.stderr)
        for e in errors + ([f"{len(warnings)} warning(s) are fatal with --strict"] if a.strict and warnings else []):
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(f"Graph parity OK ({spec.SIMILAR_TO_SPEC_VERSION}): {len(local)} tables equal and SIMILAR_TO "
          f"{len(tables['SIMILAR_TO']):,} edges identical with the persisted scaler on the same input; reported "
          f"configurations tie-only; {report['timings_s']['total']} s")
    return 0


# --------------------------------------------------------------------------- local lakehouse
def publish_local(spark, sample_dir: Path | None, *, graph_only: bool = False) -> dict:
    """Load the medallion for ``sample_dir`` (unless graph_only) and run the Spark graph job in process."""
    out: dict = {}
    if not graph_only:
        t0 = time.perf_counter()
        out["medallion_s"] = spark_silver_gold(spark, sample_dir, to_iceberg=True)
        out["medallion_total_s"] = round(time.perf_counter() - t0, 2)
    job = graph_job()
    t0 = time.perf_counter()
    out["publish"] = job.publish(spark)
    out["publish_s"] = round(time.perf_counter() - t0, 2)
    return out


def spark_submit_local(root: Path) -> dict:
    """Run the real job file with spark-submit against the local catalog (appName and main() as in Docker)."""
    prepare_env()
    conf = spark_conf(root / "catalog.db", root / "warehouse", root / "spark")
    props = root / "spark-local.conf"
    props.write_text("".join(f"{k} {v}\n" for k, v in conf.items() if k != "spark.master"))
    submit = Path(sys.executable).parent / "spark-submit"
    env = dict(os.environ, GRAPH_SQL_DIR=str(SQL_DIR))
    t0 = time.perf_counter()
    p = subprocess.run([str(submit), "--master", conf["spark.master"], "--properties-file", str(props), str(JOB)],
                       env=env, capture_output=True, text=True, check=False, cwd=root)
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("GRAPH_PUBLISH ")), None)
    if p.returncode or line is None:
        raise RuntimeError(f"spark-submit {JOB.name} failed (exit {p.returncode}): "
                           f"{(p.stderr or p.stdout).strip()[-1500:]}")
    return {"publish": json.loads(line.split(" ", 1)[1]), "spark_submit_s": round(time.perf_counter() - t0, 2),
            "stdout": [ln for ln in p.stdout.splitlines() if ln.startswith(("==>", "    "))]}


def cmd_lakehouse(a) -> int:
    root = Path(a.root).absolute()
    reason = skip_reason(iceberg=True)
    if reason:
        print(f"local lakehouse SKIPPED: {reason}", file=sys.stderr)
        return 2
    sample = None if a.graph_only else resolve_sample(a)
    if sample is not None:
        build.check_bronze(sample, a.profile)
    root.mkdir(parents=True, exist_ok=True)

    def session():
        return build_session(root / "spark", root / "catalog.db", root / "warehouse", app_name="graph_lakehouse")

    try:
        if a.spark_submit:
            res = {}
            if not a.graph_only:
                spark = session()
                try:
                    res["medallion_s"] = spark_silver_gold(spark, sample, to_iceberg=True)
                finally:
                    spark.stop()
            res.update(spark_submit_local(root))
        else:
            spark = session()
            try:
                res = publish_local(spark, sample, graph_only=a.graph_only)
            finally:
                spark.stop()
    except Exception as e:  # noqa: BLE001 - the CLI reports every Spark / Iceberg failure in one line
        print(f"local lakehouse FAILED: {type(e).__name__}: {str(e).strip()[:1500]}", file=sys.stderr)
        return 1
    pub = res["publish"]
    print(f"local lakehouse OK: catalog {root / 'catalog.db'} (Iceberg JdbcCatalog on SQLite, catalog `lakehouse`), "
          f"warehouse {root / 'warehouse'}; graph {pub['status']} as {pub['tag']}")
    print("GRAPH_LAKEHOUSE " + json.dumps({"root": str(root), **res}, sort_keys=True, default=str))
    return 0


# --------------------------------------------------------------------------- CLI
def parse(argv: list[str]) -> argparse.Namespace:
    if not argv or argv[0] not in ("parity", "sql", "lakehouse", "-h", "--help"):
        argv = ["parity", *argv]
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("parity", help="hard gate + reported configurations (default command)")
    p.add_argument("--profile", default="tiny", help="whose bronze to use: tiny (default) | s<seed> | default")
    p.add_argument("--sample-dir", default=None, help="bronze CSV directory (overrides --profile)")
    p.add_argument("--graph-root", default=None, help="for s<seed> profiles: default $GRAPH_ROOT or data/graph")
    p.add_argument("--skip-spark-gold", action="store_true", help="skip report (b) (Spark silver + gold as input)")
    p.add_argument("--strict", action="store_true", help="reported differences that are not tie-only fail too")
    p.add_argument("--json", default=None, help="write the full report here")
    p.add_argument("--driver-memory", default="2g")
    p.add_argument("--scratch", default=None, help="parent directory for the scratch files (default: $TMPDIR)")
    p.add_argument("--keep-scratch", action="store_true")
    p.set_defaults(fn=cmd_parity)
    s = sub.add_parser("sql", help="check (or --write) the generated sql/graph/similar_to.sql")
    s.add_argument("--write", action="store_true")
    s.set_defaults(fn=cmd_sql)
    lk = sub.add_parser("lakehouse", help="local Iceberg lakehouse (SQLite JdbcCatalog + file warehouse) with "
                                          "the churn medallion and the published graph")
    lk.add_argument("--root", required=True, help="directory for catalog.db, warehouse/ and Spark scratch")
    lk.add_argument("--profile", default="tiny")
    lk.add_argument("--sample-dir", default=None)
    lk.add_argument("--graph-root", default=None)
    lk.add_argument("--graph-only", action="store_true", help="only (re)run the graph job on the existing tables")
    lk.add_argument("--spark-submit", action="store_true", help="run the job file itself with spark-submit")
    lk.set_defaults(fn=cmd_lakehouse)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse(list(sys.argv[1:] if argv is None else argv))
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
