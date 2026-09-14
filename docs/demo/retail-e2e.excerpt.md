# Retail E2E excerpt (verified)

```text
Stack: postgres=healthy silo=healthy spark=running
Jobs: 01_smoke_test → 02_ingest_bronze → 03_transform_silver → 04_publish_gold → 05_query_and_timetravel

Bronze orders: 22 → Silver orders: 19  (pending dropped; o-1003 / o-1006 deduped)

gold.daily_order_metrics
+----------+------+-------+-----+---+
|order_date|orders|revenue| aov |…  |
+----------+------+-------+-----+---+
|2024-03-01|    10| 424.94|60.71|…  |
|2024-03-02|     9| 537.94|76.85|…  |
+----------+------+-------+-----+---+

Santosh Shinde (Pune) on silver.orders: o-1001, o-1003, o-1008, o-1016
Iceberg time travel on silver.orders: VERSION AS OF <current snapshot> → 19 rows
```
