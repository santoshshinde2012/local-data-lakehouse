"""Smoke test: create namespace/table, insert rows, select back via Iceberg."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("01_smoke_test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.smoke_demo (
          id INT,
          note STRING,
          created_at TIMESTAMP
        ) USING iceberg
        """
    )

    spark.sql("DELETE FROM lakehouse.bronze.smoke_demo")
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(
        f"""
        INSERT INTO lakehouse.bronze.smoke_demo VALUES
          (1, 'hello lakehouse', TIMESTAMP '{now}'),
          (2, 'silo + iceberg + spark', TIMESTAMP '{now}')
        """
    )

    print("=== smoke_demo rows ===")
    spark.sql("SELECT * FROM lakehouse.bronze.smoke_demo ORDER BY id").show(truncate=False)
    print("Smoke test OK.")
    spark.stop()


if __name__ == "__main__":
    main()
