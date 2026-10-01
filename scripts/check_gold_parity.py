#!/usr/bin/env python3
"""Check the Spark gold SQL and the pandas gold builder agree (local Spark, no Docker).

Runs bronze CSV → silver (src/jobs/churn/02_transform_silver.silver_tables) →
sql/churn/gold_renewal_features.sql in PySpark local mode, runs
scripts/build_churn_gold_local.gold on the same CSVs, and compares every
contract column plus outcome and route row by row.

Usage (needs Java 17 and `pip install pyspark==3.5.*`):
  python scripts/check_gold_parity.py
  CHURN_SAMPLE_DIR=data/sample/churn/fixtures/tiny python scripts/check_gold_parity.py
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = Path(os.environ.get("CHURN_SAMPLE_DIR", ROOT / "data/sample/churn"))
os.environ.setdefault("CHURN_GOLD_SQL", str(ROOT / "sql/churn/gold_renewal_features.sql"))


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    from pyspark.sql import SparkSession, functions as F

    bronze = _load(ROOT / "src/jobs/churn/01_ingest_bronze.py", "churn_bronze")
    silver = _load(ROOT / "src/jobs/churn/02_transform_silver.py", "churn_silver")
    gold_job = _load(ROOT / "src/jobs/churn/03_publish_gold_features.py", "churn_gold")
    local = _load(ROOT / "scripts/build_churn_gold_local.py", "churn_local")

    spark = (
        SparkSession.builder.master("local[2]").appName("gold_parity")
        .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def read_bronze(table: str):
        csv, schema = bronze.BRONZE[table]
        return (
            spark.read.option("header", True).schema(schema).csv(str(SAMPLE / csv))
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.lit(now).cast("timestamp"))
        )

    for name, df in silver.silver_tables(read_bronze).items():
        df.drop(*[c for c in silver.LINEAGE if c in df.columns]).createOrReplaceGlobalTempView(name)
    spark_gold = pd.DataFrame([r.asDict() for r in spark.sql(gold_job.gold_sql(silver="global_temp")).collect()])
    spark.stop()

    os.environ["CHURN_SAMPLE_DIR"] = str(SAMPLE)
    local.SAMPLE = SAMPLE
    s = local.silver()
    pandas_gold = local.gold(s, s["snapshots"]["snapshot_date"].max())

    cols = local.TRAIN_COLUMNS + ["outcome", "route"]
    a = spark_gold[cols].sort_values("user_id").reset_index(drop=True)
    b = pandas_gold[cols].sort_values("user_id").reset_index(drop=True)
    problems = []
    if len(a) != len(b):
        problems.append(f"row count spark={len(a)} pandas={len(b)}")
    else:
        for c in cols:
            x, y = a[c], b[c]
            if x.dtype.kind in "fi" or y.dtype.kind in "fi":
                bad = ~np.isclose(x.astype(float), y.astype(float), atol=1e-4, equal_nan=True)
            else:
                bad = x.astype(str).to_numpy() != y.astype(str).to_numpy()
            if bad.any():
                i = int(np.argmax(bad))
                problems.append(f"{c}: {int(bad.sum())} rows differ, e.g. {a.loc[i, 'user_id']}: spark={x[i]!r} pandas={y[i]!r}")
    if problems:
        print("Gold parity FAILED:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"Gold parity OK: {len(a)} renewals × {len(cols)} columns match (Spark SQL vs pandas) on {SAMPLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
