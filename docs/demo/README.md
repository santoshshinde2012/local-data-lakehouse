# End-to-end evidence (local-first stack)

The summary of this run, with every number and the CI runs, is [RESULTS.md](../../RESULTS.md) at the repo root.

| | |
|---|---|
| Date | 2026-10-03 (IST), one session from empty volumes (`make purge` first) |
| Commit | `2fcb92f` on `chore/sample-customer-santosh` for the stack, pipeline, test and Airflow steps; `5d8e09f` for the graph steps (re-run after the README rewrite) |
| Machine | MacBook Pro, Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1 (VM 10 CPUs / 7.65 GiB), Compose 5.5.1 |
| Stack | Postgres 18.6 · Lakekeeper v0.13.6 (Iceberg REST) · RustFS 1.0.0 · lakehouse-init (curl 8.22.0) · Spark 4.1.3 + Iceberg 1.12.0 · Trino 483 · Airflow 3.3.2 + socket-proxy 1.13.1 |
| Host engines | DuckDB 1.5.6 · PyIceberg 0.12.0 · Polars 1.44.2 (`.venv`, no JVM) |
| Consumer | retention-radar `chore/sample-customer-santosh` at `98df572` ([radar PR #24](https://github.com/santoshshinde2012/retention-radar/pull/24)), Python 3.12, XGBoost 3.4.1 |

![Architecture: Lakekeeper REST catalog on Postgres 18, RustFS or SILO, Spark 4.1.3 + Iceberg 1.12, optional Trino, host engines, Airflow 3 overlay, graph layer, retention-radar](architecture-e2e.png)

Source: [architecture-e2e.mmd](architecture-e2e.mmd) (Mermaid; the regenerate command is in its first lines).

## Steps

All rows are from this run and exited 0. Two first attempts failed and were fixed in the run: `make airflow-up`
(host port 8080 taken by another project's container; restarted with `AIRFLOW_API_PORT=8085`) and the first
graph re-run (stale single-file README mount in `ldl-graph`; `make graph-e2e` now recreates the container).

| Step | Time | Result | Excerpt |
|---|---:|---|---|
| `make purge` → `make up-light` | 1.1 s + 8.0 s | ✅ light stack healthy (Postgres, Lakekeeper, RustFS, lakehouse-init) | [stack-up.excerpt.md](stack-up.excerpt.md) |
| T1 `make test-t1` | 7.4 s | ✅ 5 passed (REST catalog contract, testcontainers) | [tests.excerpt.md](tests.excerpt.md) |
| T2 `make test-t2` | 5.9 s | ✅ 6 passed (light smoke) | [tests.excerpt.md](tests.excerpt.md) |
| `make demo-light` | 5.6 s | ✅ retail 22 → 19, time travel in 3 engines; churn twin 8,001 renewals | [light-demo.excerpt.md](light-demo.excerpt.md) |
| `make up-full` | 11.6 s | ✅ Spark 4.1.3 + Iceberg 1.12.0 healthy | [stack-up.excerpt.md](stack-up.excerpt.md) |
| `make e2e` (retail) | 93.8 s | ✅ bronze 22 → silver 19, gold 2 days, snapshot log + time travel | [retail-e2e.excerpt.md](retail-e2e.excerpt.md) |
| `make churn-sample` | 1.9 s | ✅ 8,001 subscriptions, 176,217 usage rows | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make churn-e2e` | 85.0 s | ✅ 8,001 renewals; 7,387 routed to the model; 3 exports | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make churn-parity` | 15.3 s | ✅ 8,001 × 27 match (Spark SQL vs pandas) | [churn-parity.excerpt.md](churn-parity.excerpt.md) |
| radar sync + ingest + score | 72.4 s | ✅ 7,387 rows scored (radar `98df572`) | [radar-consume.excerpt.md](radar-consume.excerpt.md) |
| radar `pytest` | 38.2 s | ✅ 101 passed | [radar-consume.excerpt.md](radar-consume.excerpt.md) |
| T3 `make test-t3` (+ Trino 483) | 213.5 s | ✅ 7 passed | [tests.excerpt.md](tests.excerpt.md) |
| T0 `make test-t0` | 1.5 s | ✅ 25 passed | [tests.excerpt.md](tests.excerpt.md) |
| `make graph-test` | 787.1 s | ✅ 1,403 passed, 32 skipped | [tests.excerpt.md](tests.excerpt.md) |
| `make airflow-up` | 31.3 s | ✅ Airflow 3.3.2 healthy; 3 DAGs parse, 0 import errors | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |
| `make airflow-demo` | 270.3 s | ✅ retail 5/5 and churn 4/4 tasks success | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |
| `lakehouse_graph` DAG | 240.3 s | ✅ 7/7 tasks success | [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md) |
| `make graph-e2e` (Docker, REST) at `5d8e09f` | 103.1 s | ✅ publish → Iceberg build → strict contract → lineage → cohorts → promote | [graph-e2e.excerpt.md](graph-e2e.excerpt.md) |
| `make churn-gold-local` | 3.9 s | ✅ pandas twin export (7,387 rows), export contract OK | [churn-e2e.excerpt.md](churn-e2e.excerpt.md) |
| `make graph-local PROFILE=default` (strict) | 14.2 s | ✅ 40,204 nodes / 130,366 edges, golden s42 | [graph-e2e.excerpt.md](graph-e2e.excerpt.md) |
| s42 build, cohorts, lineage | 3.6 + 14.0 + 3.6 + 4.8 s | ✅ strict contract, 15 cohorts, lineage build `3999f2dea0cb` | [graph-e2e.excerpt.md](graph-e2e.excerpt.md) |

Memory (`docker stats`) per phase and the image sizes: [stack-up.excerpt.md](stack-up.excerpt.md).
The graph checks of the same run (tools, sandbox, parity, leakage, bench, lineage):
[../graph/results/index.md](../graph/results/index.md).

## Airflow 3 UI

Headless Playwright screenshots (Chromium, 1600 × 1000) from the 2026-10-02 run; the DAGs are unchanged, and
this run's task states are in [airflow-e2e.excerpt.md](airflow-e2e.excerpt.md):

| | |
|---|---|
| ![DAG list](img/airflow-dags.png) | ![lakehouse_graph runs](img/airflow-lakehouse_graph.png) |
| ![lakehouse_retail_medallion runs](img/airflow-lakehouse_retail_medallion.png) | ![lakehouse_churn_features runs](img/airflow-lakehouse_churn_features.png) |

## How these were made

Each step's console output was captured to its own log, then trimmed for noise only (Spark INFO/WARN
lines, docker build and container progress lines, pip notices, blank lines); long outputs keep their
head and tail, and the strict-contract listings keep their first three `ok` lines per section. Nothing
else is edited.

## Demo video

The original screen recording (`lakehouse-demo-original.mov`, 49 MB, recorded on the earlier SILO +
JDBC-catalog stack) is no longer in the tree; it is in the git history
(`git log --all -- docs/demo/lakehouse-demo-original.mov`) and can be attached to a GitHub Release.

Canonical instructions live in the repository root [README.md](../../README.md).
