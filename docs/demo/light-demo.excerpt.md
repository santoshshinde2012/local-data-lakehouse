# Light demo excerpt (verified 2026-10-02)

`make demo-light` on the light profile (Postgres 18.6 + Lakekeeper v0.13.6 + RustFS 1.0.0 + init; no JVM).
The engines run on the host from `.venv` (DuckDB 1.5.6, PyIceberg 0.12.0, Polars 1.44.2) and get
short-lived S3 credentials from Lakekeeper. Snapshot ids shortened.

```text
$ make demo-light
docker compose -f docker-compose.yml  --profile light up -d --wait
==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001
.venv/bin/python scripts/light_demo.py all
=== gold.daily_order_metrics (DuckDB) ===
   ('2024-03-01', 10, 424.94, 60.71)
   ('2024-03-02', 9, 537.94, 76.85)
=== time travel: bronze.orders_raw ===
   snapshot 3870…4635 (after day 1): DuckDB 10, PyIceberg 10, Polars 10
   current snapshot 3250…0924: 22 rows
=== contract ===
  OK   bronze 22 -> silver 19 (22 -> 19)
  OK   gold daily metrics
  OK   o-1001 belongs to Santosh Shinde (c-01)
  OK   time travel to the day-1 snapshot reads 10 rows in 3 engines
  OK   Polars reads the same gold
Retail (light) OK in 2.4 s.
=== gold.churn_renewal_features_twin (pandas twin of data/sample/churn, written by PyIceberg, read by DuckDB) ===
   ('cancel_flow', 'voluntary_lapse', 287, 1.0)
   ('dunning', 'involuntary_lapse', 326, 0.0)
   ('model', 'renewed', 6839, 0.0)
   ('model', 'voluntary_lapse', 548, 1.0)
   ('score_today', 'pending', 1, 0.0)
  OK   8001 renewals published and read back
  OK   exactly one renewal is scored today (sub_maya)
Churn twin (light) OK in 1.1 s.
```

Wall time including `up --wait` on a warm stack: about 11 s. On SILO (`STORE=silo`) the same run
printed `Retail (light) OK in 1.8 s` and `Churn twin (light) OK in 1.2 s`.
