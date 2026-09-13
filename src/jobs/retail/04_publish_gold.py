"""Publish gold.daily_order_metrics from silver.orders."""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    spark = SparkSession.builder.appName("04_publish_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    orders = spark.table("lakehouse.silver.orders")
    metrics = (
        orders.withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date")
        .agg(
            F.count("*").alias("orders"),
            F.sum(
                F.when(F.col("status").isin("paid", "shipped"), F.col("amount")).otherwise(0.0)
            ).alias("revenue"),
            F.avg(
                F.when(F.col("status").isin("paid", "shipped"), F.col("amount"))
            ).alias("avg_order_value"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias(
                "cancelled_orders"
            ),
            F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)).alias(
                "refunded_orders"
            ),
        )
        .orderBy("order_date")
    )

    metrics.writeTo("lakehouse.gold.daily_order_metrics").createOrReplace()

    print("=== gold.daily_order_metrics ===")
    spark.sql(
        """
        SELECT order_date, orders, ROUND(revenue, 2) AS revenue,
               ROUND(avg_order_value, 2) AS avg_order_value,
               cancelled_orders, refunded_orders
        FROM lakehouse.gold.daily_order_metrics
        ORDER BY order_date
        """
    ).show(truncate=False)
    print("Gold publish OK.")
    spark.stop()


if __name__ == "__main__":
    main()
