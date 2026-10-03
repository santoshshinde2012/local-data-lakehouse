# Test tiers T0 to T3 and the graph suite

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `2fcb92f`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices, blank lines); long outputs keep their head and tail. `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## T0 `make test-t0` (unit and static checks, file names and links, no Docker)

Exit 0, 1.5 s.

```text
$ make test-t0
python3 scripts/check_docs.py
check_docs: OK (0 problem(s); 286 tracked files, 54 Markdown)
.venv/bin/python -m pytest -q tests/unit
.........................                                                [100%]
25 passed in 1.05s
```

## T1 `make test-t1` (REST catalog contract on testcontainers)

Exit 0, 7.4 s.

```text
$ make test-t1
.venv/bin/python -m pytest -q -s tests/contract
T1 stack (Postgres + RustFS + Lakekeeper migrate/serve + bootstrap) ready in 3.6 s
....T1 snapshots: [(1, 'append', '1000'), (2, 'append', '1001'), (3, 'overwrite', '1011'), (4, 'delete', '1011'), (5, 'overwrite', '1012')]
.
5 passed in 6.73s
```

## T2 `make test-t2` (light-profile smoke)

Exit 0, 5.9 s.

```text
$ make test-t2
docker compose -f docker-compose.yml  --profile light up -d --wait
==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001
LDL_REQUIRE_STACK=1 .venv/bin/python -m pytest -q -s tests/smoke
...=== gold.daily_order_metrics (DuckDB) ===
   ('2024-03-01', 10, 424.94, 60.71)
   ('2024-03-02', 9, 537.94, 76.85)
=== time travel: bronze.orders_raw ===
   snapshot 3281666524872723268 (after day 1): DuckDB 10, PyIceberg 10, Polars 10
   current snapshot 2115488132624370553: 22 rows
=== contract ===
  OK   bronze 22 -> silver 19 (22 -> 19)
  OK   gold daily metrics
  OK   o-1001 belongs to Santosh Shinde (c-01)
  OK   time travel to the day-1 snapshot reads 10 rows in 3 engines
  OK   Polars reads the same gold
Retail (light) OK in 2.2 s.
.=== gold.churn_renewal_features_twin (pandas twin of <repo>/data/sample/churn/fixtures/tiny, written by PyIceberg, read by DuckDB) ===
   ('cancel_flow', 'voluntary_lapse', 4, 1.0)
   ('dunning', 'involuntary_lapse', 8, 0.0)
   ('model', 'renewed', 99, 0.0)
   ('model', 'voluntary_lapse', 9, 1.0)
   ('score_today', 'pending', 1, 0.0)
  OK   121 renewals published and read back
  OK   exactly one renewal is scored today (sub_santosh)
Churn twin (light) OK in 0.4 s.
..
6 passed in 3.21s
```

## T3 `make test-t3` (Spark and Trino vs DuckDB / PyIceberg / Polars parity, full profile + Trino)

Exit 0, 213.5 s.

```text
$ make test-t3
/Applications/Xcode.app/Contents/Developer/usr/bin/make up-full TRINO=1
docker compose -f docker-compose.yml  --profile full --profile trino up -d --build --wait
==> full up: Spark UI http://localhost:4040 (while a job runs)  Trino http://localhost:8088
LDL_REQUIRE_STACK=1 .venv/bin/python -m pytest -q -s tests/parity
...=== gold.churn_renewal_features_twin (pandas twin of <repo>/data/sample/churn, written by PyIceberg, read by DuckDB) ===
   ('cancel_flow', 'voluntary_lapse', 287, 1.0)
   ('dunning', 'involuntary_lapse', 326, 0.0)
   ('model', 'renewed', 6839, 0.0)
   ('model', 'voluntary_lapse', 548, 1.0)
   ('score_today', 'pending', 1, 0.0)
  OK   8001 renewals published and read back
  OK   exactly one renewal is scored today (sub_santosh)
Churn twin (light) OK in 1.2 s.
max |spark - pandas| per feature: {'accept_rate_change': 9.999999999998899e-05}
....
7 passed in 196.79s (0:03:16)
```

## Graph suite `make graph-test` (`.venv-graph`, no containers)

Exit 0, 787.1 s.

```text
$ make graph-test
==> graph-venv: .venv-graph exists (Python 3.12); re-syncing it to requirements-graph.txt
uv pip sync --python .venv-graph/bin/python --require-hashes requirements-graph.txt
Using Python 3.12.9 environment at: .venv-graph
Checked 41 packages in 1ms
.venv-graph/bin/python -m pytest -q tests/graph
... (13 lines trimmed)
........................................................................ [ 70%]
..sss................................................................... [ 75%]
........................................................................ [ 80%]
........................................................................ [ 85%]
........................................................................ [ 90%]
........................................................................ [ 95%]
.................................................................        [100%]
1403 passed, 32 skipped in 785.14s (0:13:05)
```
