"""Ingest sample retail CSVs into bronze Iceberg tables with ingest metadata."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F


SAMPLE_DIR = os.environ.get("SAMPLE_DATA_DIR", "/opt/data/sample")


def main() -> None:
    spark = SparkSession.builder.appName("02_ingest_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")


    # --- orders: day1 then day2, one Iceberg snapshot per file ---
    # Two appends instead of one: 05_query_timetravel.py reads the table as it was after day 1
    # (VERSION AS OF the snapshot tagged ldl.batch=orders_day1) and as it is now.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_raw (
          order_id     STRING,
          customer_id  STRING,
          order_ts     TIMESTAMP,
          status       STRING,
          amount       DOUBLE,
          _source_file STRING,
          _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    # Full refresh for teaching clarity (append-only production would differ)
    spark.sql("DELETE FROM lakehouse.bronze.orders_raw")
    # One Iceberg append (snapshot) per file, day 1 then day 2; the snapshot property ldl.batch names it.
    day1 = spark.read.option("header", True).csv(f"{SAMPLE_DIR}/orders_day1.csv")
    day2 = spark.read.option("header", True).csv(f"{SAMPLE_DIR}/orders_day2.csv")
    for batch, raw in (("orders_day1", day1), ("orders_day2", day2)):
        # Each batch gets its own ingest time: silver's dedupe (latest order_ts, then latest ingest)
        # must not tie when an order's status changes between days at the same order_ts (o-1006).
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        orders = (
            raw.withColumn("order_ts", F.to_timestamp("order_ts"))
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        )
        orders.select(
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "amount",
            "_source_file",
            "_ingested_at",
        ).writeTo("lakehouse.bronze.orders_raw").option(
            "snapshot-property.ldl.batch", batch
        ).append()

    # --- customers ---
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    customers = (
        spark.read.option("header", True)
        .csv(f"{SAMPLE_DIR}/customers.csv")
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers_raw (
          customer_id  STRING,
          name         STRING,
          email        STRING,
          city         STRING,
          signup_date  DATE,
          _source_file STRING,
          _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql("DELETE FROM lakehouse.bronze.customers_raw")
    customers.select(
        "customer_id",
        "name",
        "email",
        "city",
        "signup_date",
        "_source_file",
        "_ingested_at",
    ).writeTo("lakehouse.bronze.customers_raw").append()

    print("=== bronze.orders_raw ===")
    spark.sql("SELECT order_id, status, amount, _source_file FROM lakehouse.bronze.orders_raw ORDER BY order_id").show(50, truncate=False)
    print("=== bronze.customers_raw (count) ===")
    spark.sql("SELECT COUNT(*) AS n FROM lakehouse.bronze.customers_raw").show()
    print("Bronze ingest OK.")
    spark.stop()


if __name__ == "__main__":
    main()
