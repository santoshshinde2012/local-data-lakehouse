"""Publish gold.churn_renewal_features: one row per renewal, features as of T-7.

The logic lives in sql/churn/gold_renewal_features.sql (mounted at /opt/sql) so
the same statement can be checked in local mode against the pandas builder
(scripts/check_gold_parity.py).
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from string import Template

from pyspark.sql import SparkSession, functions as F

SQL_PATH = Path(os.environ.get("CHURN_GOLD_SQL", "/opt/sql/churn/gold_renewal_features.sql"))


def gold_sql(silver: str = "lakehouse.silver") -> str:
    return Template(SQL_PATH.read_text()).substitute(silver=silver)


def main() -> None:
    spark = SparkSession.builder.appName("churn_03_publish_gold_features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    gold = spark.sql(gold_sql()).withColumn(
        "built_at", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    )
    gold.writeTo("lakehouse.gold.churn_renewal_features").using("iceberg").createOrReplace()

    print("=== gold.churn_renewal_features: routes ===")
    spark.sql(
        """
        SELECT route, outcome, COUNT(*) AS renewals, ROUND(AVG(churned), 3) AS lapse_rate
        FROM lakehouse.gold.churn_renewal_features GROUP BY route, outcome ORDER BY route, outcome
        """
    ).show(truncate=False)
    print("Churn gold features OK.")
    spark.stop()


if __name__ == "__main__":
    main()
