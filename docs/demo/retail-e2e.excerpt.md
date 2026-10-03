# Retail medallion in Spark

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `2fcb92f`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices, blank lines); long outputs keep their head and tail. `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## `make e2e`

Exit 0, 93.8 s.

```text
$ make e2e
./pipelines/wait_for_stack.sh
==> Waiting for: postgres lakekeeper objectstore lakehouse-init spark
Stack ready (postgres=healthy lakekeeper=healthy objectstore=healthy lakehouse-init=healthy spark=healthy )
./pipelines/run_retail_e2e.sh
==> Retail E2E (2026-10-03T08:21Z)
======== retail/01_smoke_test.py ========
==> spark-submit /opt/jobs/retail/01_smoke_test.py
=== smoke_demo rows ===
+---+-------------------------------------+-------------------+
|id |note                                 |created_at         |
+---+-------------------------------------+-------------------+
|1  |hello lakehouse                      |2026-10-03 08:21:49|
|2  |rustfs + lakekeeper + iceberg + spark|2026-10-03 08:21:49|
+---+-------------------------------------+-------------------+
Smoke test OK.
======== retail/02_ingest_bronze.py ========
==> spark-submit /opt/jobs/retail/02_ingest_bronze.py
=== bronze.orders_raw ===
+--------+---------+------+---------------------------------------+
|order_id|status   |amount|_source_file                           |
+--------+---------+------+---------------------------------------+
|o-1001  |PAID     |42.5  |file:///opt/data/sample/orders_day1.csv|
|o-1002  |paid     |18.0  |file:///opt/data/sample/orders_day1.csv|
|o-1003  |Shipped  |99.99 |file:///opt/data/sample/orders_day1.csv|
|o-1003  |SHIPPED  |99.99 |file:///opt/data/sample/orders_day2.csv|
|o-1004  |CANCELLED|25.0  |file:///opt/data/sample/orders_day1.csv|
|o-1005  |Paid     |61.2  |file:///opt/data/sample/orders_day1.csv|
|o-1006  |pending  |12.0  |file:///opt/data/sample/orders_day1.csv|
|o-1006  |CANCELLED|12.0  |file:///opt/data/sample/orders_day2.csv|
|o-1007  |SHIPPED  |140.0 |file:///opt/data/sample/orders_day1.csv|
|o-1008  |paid     |7.5   |file:///opt/data/sample/orders_day1.csv|
|o-1009  |Refunded |33.0  |file:///opt/data/sample/orders_day1.csv|
|o-1010  |PAID     |55.75 |file:///opt/data/sample/orders_day1.csv|
|o-1011  |paid     |22.4  |file:///opt/data/sample/orders_day2.csv|
|o-1012  |PAID     |88.0  |file:///opt/data/sample/orders_day2.csv|
|o-1013  |shipped  |15.25 |file:///opt/data/sample/orders_day2.csv|
|o-1014  |Cancelled|40.0  |file:///opt/data/sample/orders_day2.csv|
|o-1015  |PAID     |210.0 |file:///opt/data/sample/orders_day2.csv|
|o-1016  |Paid     |9.99  |file:///opt/data/sample/orders_day2.csv|
|o-1017  |pending  |5.0   |file:///opt/data/sample/orders_day2.csv|
|o-1018  |SHIPPED  |72.3  |file:///opt/data/sample/orders_day2.csv|
|o-1019  |refunded |19.5  |file:///opt/data/sample/orders_day2.csv|
|o-1020  |paid     |120.0 |file:///opt/data/sample/orders_day2.csv|
+--------+---------+------+---------------------------------------+
=== bronze.customers_raw (count) ===
+---+
|  n|
+---+
| 10|
+---+
Bronze ingest OK.
======== retail/03_transform_silver.py ========
==> spark-submit /opt/jobs/retail/03_transform_silver.py
=== silver.orders (note: o-1003 / o-1006 deduped; pending dropped) ===
+--------+---------+------+
|order_id|status   |amount|
+--------+---------+------+
|o-1001  |paid     |42.5  |
|o-1002  |paid     |18.0  |
|o-1003  |shipped  |99.99 |
|o-1004  |cancelled|25.0  |
|o-1005  |paid     |61.2  |
|o-1006  |cancelled|12.0  |
|o-1007  |shipped  |140.0 |
|o-1008  |paid     |7.5   |
|o-1009  |refunded |33.0  |
|o-1010  |paid     |55.75 |
|o-1011  |paid     |22.4  |
|o-1012  |paid     |88.0  |
|o-1013  |shipped  |15.25 |
|o-1014  |cancelled|40.0  |
|o-1015  |paid     |210.0 |
|o-1016  |paid     |9.99  |
|o-1018  |shipped  |72.3  |
|o-1019  |refunded |19.5  |
|o-1020  |paid     |120.0 |
+--------+---------+------+
Bronze orders: 22 → Silver orders: 19
Silver transform OK.
... (37 lines trimmed)
|o-1015  |Emily Carter  |Seattle      |paid     |210.0 |
|o-1016  |Santosh Shinde|Pune         |paid     |9.99  |
|o-1018  |Rahul Mehta   |Mumbai       |shipped  |72.3  |
|o-1019  |Jordan Miles  |Austin       |refunded |19.5  |
|o-1020  |Vikram Patel  |Ahmedabad    |paid     |120.0 |
+--------+--------------+-------------+---------+------+
=== Iceberg snapshots: lakehouse.bronze.orders_raw ===
+-----------------------+-------------------+-------------------+---------+-----------+-------------+
|committed_at           |snapshot_id        |parent_id          |operation|batch      |total_records|
+-----------------------+-------------------+-------------------+---------+-----------+-------------+
|2026-10-03 08:21:01.872|3281666524872723268|NULL               |append   |NULL       |10           |
|2026-10-03 08:21:02.068|2115488132624370553|3281666524872723268|append   |NULL       |22           |
|2026-10-03 08:21:06.464|6449849691100563385|2115488132624370553|delete   |NULL       |22           |
|2026-10-03 08:21:06.806|6134302769723622486|6449849691100563385|append   |NULL       |32           |
|2026-10-03 08:21:06.921|7947038385582522679|6134302769723622486|append   |NULL       |44           |
|2026-10-03 08:22:06.654|5350795540535698399|7947038385582522679|delete   |NULL       |0            |
|2026-10-03 08:22:08.521|8478826077722680749|5350795540535698399|append   |orders_day1|10           |
|2026-10-03 08:22:08.741|5740840578924602149|8478826077722680749|append   |orders_day2|22           |
+-----------------------+-------------------+-------------------+---------+-----------+-------------+
=== Time travel: bronze.orders_raw VERSION AS OF 8478826077722680749 (after day 1) ===
+------+-----+
|orders|files|
+------+-----+
|    10|    1|
+------+-----+
=== Current: bronze.orders_raw (snapshot 5740840578924602149) ===
+------+-----+
|orders|files|
+------+-----+
|    22|    2|
+------+-----+
=== silver.orders VERSION AS OF 7135897253402319095 (current) ===
+---+
|  n|
+---+
| 19|
+---+
Note: DuckDB, PyIceberg, Polars and Trino read the same tables (and the same snapshots) through the Lakekeeper REST catalog.
Query + time travel OK.
==> Retail E2E complete.
```
