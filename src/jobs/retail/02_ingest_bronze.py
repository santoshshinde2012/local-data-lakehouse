"""Ingest sample retail CSVs into bronze Iceberg tables with ingest metadata."""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F


SAMPLE_DIR = os.environ.get("SAMPLE_DATA_DIR", "/opt/data/sample")


def main() -> None:
    spark = SparkSession.builder.appName("02_ingest_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

    ingested_at = datetime.utcnow()

    # --- orders (day1 + day2) ---
    order_files = [
        f"{SAMPLE_DIR}/orders_day1.csv",
        f"{SAMPLE_DIR}/orders_day2.csv",
    ]
    orders = (
        spark.read.option("header", True)
        .csv(order_files)
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )

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
    orders.select(
        "order_id",
        "customer_id",
        "order_ts",
        "status",
        "amount",
        "_source_file",
        "_ingested_at",
    ).writeTo("lakehouse.bronze.orders_raw").append()

    # --- customers ---
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
