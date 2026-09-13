"""Transform bronze → silver: dedupe, type, validate status allow-list."""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F, Window


VALID_STATUS = ["paid", "shipped", "cancelled", "refunded"]


def main() -> None:
    spark = SparkSession.builder.appName("03_transform_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    orders_raw = spark.table("lakehouse.bronze.orders_raw")
    w = Window.partitionBy("order_id").orderBy(
        F.col("order_ts").desc(), F.col("_ingested_at").desc()
    )
    orders_silver = (
        orders_raw.withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("status").isin(VALID_STATUS))
        .drop("rn")
        .select(
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "amount",
            "_source_file",
            "_ingested_at",
        )
    )

    (
        orders_silver.writeTo("lakehouse.silver.orders")
        .createOrReplace()
    )

    customers_raw = spark.table("lakehouse.bronze.customers_raw")
    cw = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    customers_silver = (
        customers_raw.withColumn("name", F.trim(F.col("name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("rn", F.row_number().over(cw))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "customer_id",
            "name",
            "email",
            "city",
            "signup_date",
            "_source_file",
            "_ingested_at",
        )
    )
    (
        customers_silver.writeTo("lakehouse.silver.customers")
        .createOrReplace()
    )

    print("=== silver.orders (note: o-1003 / o-1006 deduped; pending dropped) ===")
    spark.sql(
        "SELECT order_id, status, amount FROM lakehouse.silver.orders ORDER BY order_id"
    ).show(50, truncate=False)
    print(f"Bronze orders: {orders_raw.count()} → Silver orders: {orders_silver.count()}")
    print("Silver transform OK.")
    spark.stop()


if __name__ == "__main__":
    main()
