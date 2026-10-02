"""Query gold metrics and use Iceberg snapshots for time travel.

02_ingest_bronze.py appends orders_day1 and orders_day2 as two snapshots of
lakehouse.bronze.orders_raw (snapshot property ldl.batch). This job reads the table
as of the day-1 snapshot (an EARLIER version) and as of now, then checks the silver
contract (19 orders) through the current snapshot id.
"""
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

    print("=== Iceberg snapshots: lakehouse.bronze.orders_raw ===")
    snaps = spark.sql(
        """
        SELECT committed_at, snapshot_id, parent_id, operation,
               summary['ldl.batch'] AS batch, summary['total-records'] AS total_records
        FROM lakehouse.bronze.orders_raw.snapshots
        ORDER BY committed_at
        """
    )
    snaps.show(truncate=False)

    rows = snaps.collect()
    day1 = [r for r in rows if r["batch"] == "orders_day1"]
    if not day1:
        raise SystemExit("no snapshot with ldl.batch=orders_day1: run 02_ingest_bronze.py first")
    day1_id, current_id = day1[-1]["snapshot_id"], rows[-1]["snapshot_id"]

    print(f"=== Time travel: bronze.orders_raw VERSION AS OF {day1_id} (after day 1) ===")
    spark.sql(
        f"""
        SELECT COUNT(*) AS orders, COUNT(DISTINCT _source_file) AS files
        FROM lakehouse.bronze.orders_raw VERSION AS OF {day1_id}
        """
    ).show()
    print(f"=== Current: bronze.orders_raw (snapshot {current_id}) ===")
    spark.sql(
        "SELECT COUNT(*) AS orders, COUNT(DISTINCT _source_file) AS files FROM lakehouse.bronze.orders_raw"
    ).show()

    silver_id = spark.sql(
        "SELECT snapshot_id FROM lakehouse.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]["snapshot_id"]
    print(f"=== silver.orders VERSION AS OF {silver_id} (current) ===")
    spark.sql(f"SELECT COUNT(*) AS n FROM lakehouse.silver.orders VERSION AS OF {silver_id}").show()

    print(
        "Note: DuckDB, PyIceberg, Polars and Trino read the same tables (and the same "
        "snapshots) through the Lakekeeper REST catalog."
    )
    print("Query + time travel OK.")
    spark.stop()


if __name__ == "__main__":
    main()
