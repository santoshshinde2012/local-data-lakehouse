# Retail E2E excerpt (verified 2026-10-02)

`make e2e` on the full profile: Spark 4.1.3 + Iceberg 1.11.0 through the Lakekeeper REST catalog.
Total 1 min 51 s on a running stack (five `spark-submit` runs). Re-run on Iceberg 1.12.0 from empty
volumes (2026-10-02): 100 s, same rows and snapshot log. Snapshot ids shortened.

```text
$ make e2e
Stack ready (postgres=healthy lakekeeper=healthy objectstore=healthy lakehouse-init=healthy spark=healthy )
Jobs: 01_smoke_test → 02_ingest_bronze → 03_transform_silver → 04_publish_gold → 05_query_timetravel
Smoke test OK.
Bronze ingest OK.
=== silver.orders (note: o-1003 / o-1006 deduped; pending dropped) ===
Bronze orders: 22 → Silver orders: 19
Silver transform OK.

=== Same gold table via Spark SQL ===
+----------+------+-------+-----+
|order_date|orders|revenue|aov  |
+----------+------+-------+-----+
|2024-03-01|10    |424.94 |60.71|
|2024-03-02|9     |537.94 |76.85|
+----------+------+-------+-----+

=== Orders joined to customers (IN + US mix) ===
|o-1001  |Santosh Shinde|Pune         |paid     |42.5  |
|o-1003  |Santosh Shinde|Pune         |shipped  |99.99 |
|o-1008  |Santosh Shinde|Pune         |paid     |7.5   |
|o-1016  |Santosh Shinde|Pune         |paid     |9.99  |
… 19 rows

=== Iceberg snapshots: lakehouse.bronze.orders_raw ===
|operation|batch      |total_records|
|delete   |NULL       |0            |
|append   |orders_day1|10           |
|append   |orders_day2|22           |

=== Time travel: bronze.orders_raw VERSION AS OF 3339…0626 (after day 1) ===
|orders|files|
|    10|    1|
=== Current: bronze.orders_raw (snapshot 8941…0726) ===
|orders|files|
|    22|    2|
Query + time travel OK.

==> Retail E2E complete.
```

Each re-run is a full refresh (`DELETE` then one append per orders file), so the snapshot log grows by
three entries per run. Each append carries the snapshot property `ldl.batch`.
