"""
Retail medallion DAG — mirrors the article chain:

    land/smoke >> bronze >> silver >> gold >> query

Airflow schedules; MinIO + Iceberg + Spark remain the lakehouse foundation.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from lakehouse_operators import spark_submit_task

with DAG(
    dag_id="lakehouse_retail_medallion",
    description="Retail bronze → silver → gold + Iceberg time travel via Spark jobs",
    start_date=datetime(2024, 3, 1),
    schedule=None,  # manual trigger for local demos
    catchup=False,
    tags=["lakehouse", "retail", "medallion"],
) as dag:
    smoke = spark_submit_task("land_smoke", "retail/01_smoke_test.py")
    bronze = spark_submit_task("bronze", "retail/02_ingest_bronze.py")
    silver = spark_submit_task("silver", "retail/03_transform_silver.py")
    gold = spark_submit_task("gold", "retail/04_publish_gold.py")
    query = spark_submit_task("query_timetravel", "retail/05_query_timetravel.py")

    smoke >> bronze >> silver >> gold >> query
