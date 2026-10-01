"""Clean churn bronze → silver: normalise enums, dedupe, derive event dates."""
from __future__ import annotations

from pyspark.sql import SparkSession, Window, functions as F


def latest(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


LINEAGE = ["_source_file", "_ingested_at"]


def silver_tables(b) -> dict:
    """Silver DataFrames from a bronze reader ``b(table_name) -> DataFrame``."""
    return {
        "churn_subscription_snapshots": latest(
            b("churn_subscription_snapshots_raw")
            .withColumn("plan_tier", F.lower(F.trim("plan_tier")))
            .withColumn("user_name", F.trim("user_name"))
            .withColumn("status", F.lower(F.trim("status"))),
            ["subscription_id", "snapshot_date"],
        ),
        "churn_usage_daily": latest(
            b("churn_usage_raw").filter(F.col("activity_date").isNotNull()),
            ["subscription_id", "activity_date"],
        ),
        "churn_invoices": b("churn_invoices_raw").withColumn("status", F.lower(F.trim("status"))),
        "churn_subscription_events": b("churn_subscription_events_raw").withColumn(
            "event_type", F.lower(F.trim("event_type"))
        ),
        # Cap hits are windowed by calendar date, like every other as-of feature.
        "churn_limit_events": b("churn_limit_events_raw").withColumn("hit_date", F.to_date("hit_at")),
        "churn_overage_settings": b("churn_overage_settings_raw").withColumn(
            "overage", F.lower(F.trim("overage"))
        ),
        "churn_overage_charges": b("churn_overage_charges_raw"),
        "churn_incidents": b("churn_incidents_raw"),
        "churn_support_tickets": b("churn_support_tickets_raw"),
        "churn_pricing_changes": b("churn_pricing_changes_raw"),
    }


def main() -> None:
    spark = SparkSession.builder.appName("churn_02_transform_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    silver = silver_tables(lambda t: spark.table(f"lakehouse.bronze.{t}"))
    for name, df in silver.items():
        df.drop(*[c for c in LINEAGE if c in df.columns]).writeTo(f"lakehouse.silver.{name}").using(
            "iceberg"
        ).createOrReplace()
        print(f"silver.{name}: {spark.table(f'lakehouse.silver.{name}').count()} rows")

    print("Churn silver OK.")
    spark.stop()


if __name__ == "__main__":
    main()
