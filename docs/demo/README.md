# End-to-end evidence (local-first stack)

| | |
|---|---|
| Date | 2026-10-02 (IST), one run from empty volumes (`make purge` first) |
| Commit | `7f5fc43` on `feat/local-first-stack-2026` (the code under test; later commits on the branch are docs and evidence tooling) |
| Machine | MacBook Pro, Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1 (VM 10 CPUs / 7.65 GiB), Compose 5.5.1 |
| Stack | Postgres 18.6 · Lakekeeper v0.13.6 (Iceberg REST) · RustFS 1.0.0 (SILO `RELEASE.2026-09-16T00-00-00Z` overlay) · lakehouse-init (curl 8.22.0) · Spark 4.1.3 + Iceberg 1.12.0 · Trino 483 (optional) · Airflow 3.3.2 + socket-proxy 1.13.1 (overlay) |
| Host engines | DuckDB 1.5.6 · PyIceberg 0.12.0 · Polars 1.44.2 (`.venv`, no JVM) |
| Graph layer | Python 3.12.9 · LadybugDB 0.21.2 · pandas 3.0.6 · networkx 3.7; Spark harness pyspark 4.1.3 |
| Consumer | retention-radar `feat/local-first-stack-2026` @ `65cab25` ([radar PR #21](https://github.com/santoshshinde2012/retention-radar/pull/21)), Python 3.12, XGBoost 3.4.1 |

![Architecture: Lakekeeper REST catalog on Postgres 18, RustFS or SILO, Spark 4.1.3 + Iceberg 1.12, optional Trino, host engines, Airflow 3 overlay, graph layer, retention-radar](architecture-e2e.png)

Source: [architecture-e2e.mmd](architecture-e2e.mmd) (Mermaid; the regenerate command is in its first lines).

## Steps

Every step exited 0. Total 17.7 min of step time (plus the Airflow CLI reads).

| Step | Time | Result | Excerpt |
|---|---:|---|---|
| `make purge` → `make up-light` | 8.3 s | ✅ light stack healthy (Postgres, Lakekeeper, RustFS, lakehouse-init) | [stack-up.excerpt.md](stack-up.excerpt.md) |
| T1 `make test-t1` | 6.8 s | ✅ 5 passed (REST catalog contract, testcontainers) | [tests.excerpt.md](tests.excerpt.md) |
| T2 `make test-t2` | 5 s | ✅ 6 passed (light smoke) | [tests.excerpt.md](tests.excerpt.md) |
| `make demo-light` | 4.8 s | ✅ retail 22 → 19, time travel in 3 engines; churn twin 8,001 renewals | [light-demo.excerpt.md](light-demo.excerpt.md) |
| `make up-full` | 11.1 s | ✅ Spark 4.1.3 + Iceberg 1.12.0 healthy | [stack-up.excerpt.md](stack-up.excerpt.md) |
| `make e2e` (retail) | 95.6 s | ✅ bronze 22 → silver 19, gold 2 days, snapshot log + time travel | [retail-e2e.excerpt.md](retail-e2e.excerpt.md) |
| `make churn-sample` | 2 s | ✅ 8,001 subscriptions, 176,217 usage rows | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make churn-e2e` | 83.1 s | ✅ 8,001 renewals; 7,387 routed to the model; 3 exports | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make churn-parity` | 14.6 s | ✅ 8,001 × 27 match (Spark SQL vs pandas) | [churn-parity.excerpt.md](churn-parity.excerpt.md) |
| radar sync + ingest + score | 66.6 s | ✅ 7,387 rows scored (radar PR #21 branch) | [radar-consume.excerpt.md](radar-consume.excerpt.md) |
| radar `pytest` | 40.8 s | ✅ 96 passed | [radar-consume.excerpt.md](radar-consume.excerpt.md) |
| `make graph-e2e` (Docker, REST) | 103.5 s | ✅ publish → Iceberg build → strict contract → lineage → cohorts → promote | [graph-e2e.excerpt.md](graph-e2e.excerpt.md) |
| `make churn-gold-local` | 3.7 s | ✅ pandas twin export (7,387 rows), export contract OK | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make graph-local PROFILE=default` (strict) | 13.8 s | ✅ 40,204 nodes / 130,366 edges, golden s42, strict | [graph-e2e.excerpt.md](graph-e2e.excerpt.md) |
| T3 `make test-t3` (+ Trino 483) | 203.5 s | ✅ 7 passed; max Spark vs pandas difference 1e-4 | [tests.excerpt.md](tests.excerpt.md) |
| T0 `make test-t0` | 1.3 s | ✅ 19 passed | [tests.excerpt.md](tests.excerpt.md) |
| `make airflow-up` | 30.3 s | ✅ Airflow 3.3.2 healthy; 3 DAGs parse, 0 import errors | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |
| `make airflow-demo` | 214.8 s | ✅ retail 5/5 and churn 4/4 tasks success | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |
| `lakehouse_graph` DAG | 132.3 s | ✅ 7/7 tasks success | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |

Memory (`docker stats`) per phase and the image sizes: [stack-up.excerpt.md](stack-up.excerpt.md).
The graph checks of the same day (seed-42 build, tools, sandbox, parity, leakage, bench, lineage):
[../graph/results/index.md](../graph/results/index.md).

## Airflow 3 UI

Headless Playwright screenshots (Chromium, 1600 × 1000) of the runs above:

| | |
|---|---|
| ![DAG list](img/airflow-dags.png) | ![lakehouse_graph runs](img/airflow-lakehouse_graph.png) |
| ![lakehouse_retail_medallion runs](img/airflow-lakehouse_retail_medallion.png) | ![lakehouse_churn_features runs](img/airflow-lakehouse_churn_features.png) |

## How these were made

Each step's console output was captured to its own log, then trimmed for noise only (Spark INFO/WARN
lines, docker build and container progress lines, pip notices); the long strict-contract listings keep
their first three `ok` lines per section and count the rest. Nothing else is edited.

## Demo video

The original screen recording (`lakehouse-demo-original.mov`, 49 MB, recorded on the earlier SILO +
JDBC-catalog stack) is no longer in the tree; it is in the git history
(`git log --all -- docs/demo/lakehouse-demo-original.mov`) and can be attached to a GitHub Release.

Canonical instructions live in the repository root `README.md`.
