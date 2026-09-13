"""Query gold metrics and peek at Iceberg snapshot history (time travel)."""
from __future__ import annotations

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("05_query_and_timetravel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=== Same gold table via Spark SQL ===")
    spark.sql(
        """
        SELECT order_date, orders, ROUND(revenue, 2) AS revenue,
               ROUND(avg_order_value, 2) AS aov
        FROM lakehouse.gold.daily_order_metrics
        ORDER BY order_date
        """
    ).show(truncate=False)

    print("=== Orders joined to customers (IN + US mix) ===")
    spark.sql(
        """
        SELECT o.order_id, c.name, c.city, o.status, ROUND(o.amount, 2) AS amount
        FROM lakehouse.silver.orders o
        JOIN lakehouse.silver.customers c ON o.customer_id = c.customer_id
        ORDER BY o.order_id
        """
    ).show(50, truncate=False)

    print("=== Iceberg history: lakehouse.silver.orders ===")
    spark.sql("SELECT * FROM lakehouse.silver.orders.history").show(truncate=False)

    print("=== Iceberg snapshots: lakehouse.silver.orders ===")
    spark.sql(
        """
        SELECT committed_at, snapshot_id, operation, summary
        FROM lakehouse.silver.orders.snapshots
        ORDER BY committed_at
        """
    ).show(truncate=False)

    # Time travel: read current snapshot id, then demonstrate VERSION AS OF if >=1 snapshot
    snaps = spark.sql(
        "SELECT snapshot_id FROM lakehouse.silver.orders.snapshots ORDER BY committed_at"
    ).collect()
    if snaps:
        snap_id = snaps[-1]["snapshot_id"]
        print(f"=== Time travel VERSION AS OF snapshot {snap_id} (current) ===")
        spark.sql(
            f"""
            SELECT COUNT(*) AS n
            FROM lakehouse.silver.orders VERSION AS OF {snap_id}
            """
        ).show()

    print(
        "Note: other engines can read the same Iceberg tables "
        "when pointed at the same warehouse + catalog."
    )
    print("Query + time travel OK.")
    spark.stop()


if __name__ == "__main__":
    main()
