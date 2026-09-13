"""Clean churn bronze → silver."""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F, Window


def main() -> None:
    spark = SparkSession.builder.appName("07_transform_churn_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    users = spark.table("lakehouse.bronze.churn_users_raw")
    w = Window.partitionBy("user_id").orderBy(F.col("_ingested_at").desc())
    users_s = (
        users.withColumn("plan_tier", F.lower(F.trim("plan_tier")))
        .withColumn("user_name", F.trim("user_name"))
        .withColumn("city", F.trim("city"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("plan_tier").isin("free", "starter", "pro", "enterprise"))
        .drop("rn")
    )
    users_s.writeTo("lakehouse.silver.churn_users").createOrReplace()

    usage = spark.table("lakehouse.bronze.churn_usage_raw")
    usage_s = (
        usage.filter(F.col("event_date").isNotNull())
        .filter(F.col("sessions") >= 0)
        .withColumn("sessions", F.col("sessions").cast("int"))
    )
    usage_s.writeTo("lakehouse.silver.churn_usage_daily").createOrReplace()

    tickets = spark.table("lakehouse.bronze.churn_tickets_raw")
    tickets.writeTo("lakehouse.silver.churn_tickets").createOrReplace()

    payments = spark.table("lakehouse.bronze.churn_payments_raw")
    payments_s = payments.withColumn("status", F.lower(F.trim("status")))
    payments_s.writeTo("lakehouse.silver.churn_payments").createOrReplace()

    print("=== silver churn users ===")
    spark.sql(
        "SELECT user_id, user_name, plan_tier, city, churned FROM lakehouse.silver.churn_users ORDER BY user_id"
    ).show(truncate=False)
    print("Churn silver OK.")
    spark.stop()


if __name__ == "__main__":
    main()
