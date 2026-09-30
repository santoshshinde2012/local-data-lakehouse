"""Ingest the AI coding assistant's raw event CSVs into bronze Iceberg tables.

One bronze table per source file, typed, with lineage columns. Bronze keeps
everything, including usage after each subscriber's T-7 date: point-in-time
filtering is the gold job's responsibility, not ingestion's.
"""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

SAMPLE_DIR = os.environ.get("CHURN_SAMPLE_DIR", "/opt/data/sample/churn")

# table → (csv file, "column TYPE, ..." in file order)
BRONZE = {
    "churn_subscription_snapshots_raw": (
        "subscription_snapshots.csv",
        "subscription_id STRING, snapshot_date DATE, user_name STRING, plan_tier STRING, "
        "status STRING, current_period_end DATE, started_at DATE, city STRING",
    ),
    "churn_invoices_raw": (
        "invoices.csv",
        "subscription_id STRING, invoice_date DATE, amount_usd DOUBLE, status STRING, attempt INT",
    ),
    "churn_subscription_events_raw": (
        "subscription_events.csv",
        "subscription_id STRING, event_date DATE, event_type STRING",
    ),
    "churn_usage_raw": (
        "daily_usage.csv",
        "subscription_id STRING, activity_date DATE, ide_sessions INT, cli_sessions INT, "
        "agent_requests INT, cheap_model_requests INT, suggestions_shown INT, "
        "suggestions_accepted INT, agent_tasks INT, agent_tasks_kept INT, "
        "total_requests INT, failed_requests INT",
    ),
    "churn_limit_events_raw": (
        "limit_events.csv",
        "subscription_id STRING, hit_at TIMESTAMP, limit_type STRING",
    ),
    "churn_overage_settings_raw": (
        "overage_settings.csv",
        "subscription_id STRING, changed_at DATE, overage STRING",
    ),
    "churn_overage_charges_raw": (
        "overage_charges.csv",
        "subscription_id STRING, charged_at DATE, amount_usd DOUBLE",
    ),
    "churn_incidents_raw": ("incidents.csv", "incident_id STRING, starts_on DATE, ends_on DATE"),
    "churn_support_tickets_raw": (
        "support_tickets.csv",
        "ticket_id STRING, subscription_id STRING, created_date DATE",
    ),
    "churn_pricing_changes_raw": (
        "pricing_changes.csv",
        "change_id STRING, effective_date DATE, description STRING",
    ),
}


def main() -> None:
    spark = SparkSession.builder.appName("churn_01_ingest_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    ingested_at = datetime.utcnow()

    for table, (csv, schema) in BRONZE.items():
        df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(f"{SAMPLE_DIR}/{csv}")
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        )
        df.writeTo(f"lakehouse.bronze.{table}").using("iceberg").createOrReplace()
        n = spark.table(f"lakehouse.bronze.{table}").count()
        print(f"bronze.{table}: {n} rows")

    print("Churn bronze ingest OK.")
    spark.stop()


if __name__ == "__main__":
    main()
