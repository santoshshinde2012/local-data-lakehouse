"""
Renewal features for a monthly AI coding assistant, on the same lakehouse:

    ingest (billing + usage events) >> silver >> gold T-7 renewal features >> export
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from lakehouse_operators import spark_submit_task

with DAG(
    dag_id="lakehouse_churn_features",
    description="Billing + usage events → silver → gold.churn_renewal_features (as of T-7) → export",
    start_date=datetime(2024, 3, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "churn", "medallion"],
) as dag:
    bronze = spark_submit_task("bronze", "churn/01_ingest_bronze.py")
    silver = spark_submit_task("silver", "churn/02_transform_silver.py")
    gold = spark_submit_task("gold_features", "churn/03_publish_gold_features.py")
    export = spark_submit_task("export_features", "churn/04_export_features.py")

    bronze >> silver >> gold >> export
