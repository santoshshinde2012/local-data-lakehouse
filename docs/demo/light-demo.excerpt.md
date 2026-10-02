# Light demo (`make demo-light`): DuckDB / PyIceberg / Polars, no JVM

Captured on 2026-10-02 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `feat/local-first-stack-2026` at `7f5fc43`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

Exit 0, 4.8 s.

```text
$ make demo-light
docker compose -f docker-compose.yml  --profile light up -d --wait
==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001
.venv/bin/python scripts/light_demo.py all
=== gold.daily_order_metrics (DuckDB) ===
   ('2024-03-01', 10, 424.94, 60.71)
   ('2024-03-02', 9, 537.94, 76.85)
=== time travel: bronze.orders_raw ===
   snapshot 2772959020612010823 (after day 1): DuckDB 10, PyIceberg 10, Polars 10
   current snapshot 3623439888630828024: 22 rows
=== contract ===
  OK   bronze 22 -> silver 19 (22 -> 19)
  OK   gold daily metrics
  OK   o-1001 belongs to Santosh Shinde (c-01)
  OK   time travel to the day-1 snapshot reads 10 rows in 3 engines
  OK   Polars reads the same gold
Retail (light) OK in 1.3 s.
=== gold.churn_renewal_features_twin (pandas twin of <repo>/data/sample/churn, written by PyIceberg, read by DuckDB) ===
   ('cancel_flow', 'voluntary_lapse', 287, 1.0)
   ('dunning', 'involuntary_lapse', 326, 0.0)
   ('model', 'renewed', 6839, 0.0)
   ('model', 'voluntary_lapse', 548, 1.0)
   ('score_today', 'pending', 1, 0.0)
  OK   8001 renewals published and read back
  OK   exactly one renewal is scored today (sub_maya)
Churn twin (light) OK in 1.0 s.
```
