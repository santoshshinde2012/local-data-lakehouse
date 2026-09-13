"""Ingest churn sample CSVs into bronze Iceberg tables."""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

SAMPLE_DIR = os.environ.get("CHURN_SAMPLE_DIR", "/opt/data/sample/churn")


def main() -> None:
    spark = SparkSession.builder.appName("06_ingest_churn_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    ingested_at = datetime.utcnow()

    users = (
        spark.read.option("header", True)
        .csv(f"{SAMPLE_DIR}/users.csv")
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("nps_score", F.col("nps_score").cast("double"))
        .withColumn("churned", F.col("churned").cast("int"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.churn_users_raw (
          user_id STRING, user_name STRING, plan_tier STRING, signup_date DATE,
          city STRING, nps_score DOUBLE, churned INT,
          _source_file STRING, _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql("DELETE FROM lakehouse.bronze.churn_users_raw")
    users.select(
        "user_id", "user_name", "plan_tier", "signup_date", "city", "nps_score", "churned",
        "_source_file", "_ingested_at",
    ).writeTo("lakehouse.bronze.churn_users_raw").append()

    usage = (
        spark.read.option("header", True)
        .csv(f"{SAMPLE_DIR}/daily_usage_snapshots.csv")
        .withColumn("event_date", F.to_date("event_date"))
        .withColumn("sessions", F.col("sessions").cast("int"))
        .withColumn("session_minutes", F.col("session_minutes").cast("double"))
        .withColumn("api_calls", F.col("api_calls").cast("int"))
        .withColumn("tokens", F.col("tokens").cast("long"))
        .withColumn("tools_used", F.col("tools_used").cast("int"))
        .withColumn("failed_requests", F.col("failed_requests").cast("int"))
        .withColumn("successful_requests", F.col("successful_requests").cast("int"))
        .withColumn("agent_runs", F.col("agent_runs").cast("int"))
        .withColumn("ide_plugin_sessions", F.col("ide_plugin_sessions").cast("int"))
        .withColumn("is_weekend", F.col("is_weekend").cast("int"))
        .withColumn("spend_usd", F.col("spend_usd").cast("double"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.churn_usage_raw (
          user_id STRING, event_date DATE, sessions INT, session_minutes DOUBLE,
          api_calls INT, tokens BIGINT, tools_used INT, failed_requests INT,
          successful_requests INT, agent_runs INT, ide_plugin_sessions INT,
          is_weekend INT, spend_usd DOUBLE,
          _source_file STRING, _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql("DELETE FROM lakehouse.bronze.churn_usage_raw")
    usage.select(
        "user_id", "event_date", "sessions", "session_minutes", "api_calls", "tokens",
        "tools_used", "failed_requests", "successful_requests", "agent_runs",
        "ide_plugin_sessions", "is_weekend", "spend_usd", "_source_file", "_ingested_at",
    ).writeTo("lakehouse.bronze.churn_usage_raw").append()

    tickets = (
        spark.read.option("header", True)
        .csv(f"{SAMPLE_DIR}/support_tickets.csv")
        .withColumn("created_date", F.to_date("created_date"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.churn_tickets_raw (
          ticket_id STRING, user_id STRING, created_date DATE,
          _source_file STRING, _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql("DELETE FROM lakehouse.bronze.churn_tickets_raw")
    tickets.select(
        "ticket_id", "user_id", "created_date", "_source_file", "_ingested_at"
    ).writeTo("lakehouse.bronze.churn_tickets_raw").append()

    payments = (
        spark.read.option("header", True)
        .csv(f"{SAMPLE_DIR}/payments.csv")
        .withColumn("payment_date", F.to_date("payment_date"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.churn_payments_raw (
          payment_id STRING, user_id STRING, payment_date DATE, status STRING,
          _source_file STRING, _ingested_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql("DELETE FROM lakehouse.bronze.churn_payments_raw")
    payments.select(
        "payment_id", "user_id", "payment_date", "status", "_source_file", "_ingested_at"
    ).writeTo("lakehouse.bronze.churn_payments_raw").append()

    print("=== bronze churn counts ===")
    for t in [
        "churn_users_raw",
        "churn_usage_raw",
        "churn_tickets_raw",
        "churn_payments_raw",
    ]:
        spark.sql(f"SELECT COUNT(*) AS n FROM lakehouse.bronze.{t}").show()
    print("Churn bronze ingest OK.")
    spark.stop()


if __name__ == "__main__":
    main()
