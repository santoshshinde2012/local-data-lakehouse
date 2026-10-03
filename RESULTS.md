# Results: the local-first stack, end to end

One scripted run from empty volumes (`make purge` first) on 2026-10-03, with every step's console output committed as an excerpt under [docs/demo/](docs/demo/README.md). Every number on this page comes from that run: from those excerpts, from [docs/graph/results/](docs/graph/results/index.md) (regenerated in the same run) or from the linked CI logs. Nothing was re-typed from memory or carried over from earlier runs.

| | |
|---|---|
| Date | 2026-10-03 (IST), one session from empty volumes |
| Code under test | branch `chore/sample-customer-santosh` ([PR #16](https://github.com/santoshshinde2012/local-data-lakehouse/pull/16)): `2fcb92f` for the stack, pipeline, test and Airflow steps; `5d8e09f` for the graph steps, re-run after the README rewrite (the lineage graph reads the README) and the `graph-e2e` fix found in this run. Between them only docs, `pipelines/airflow_wait.sh` and the `graph-e2e` target changed |
| Machine | MacBook Pro, Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1 (VM 10 CPUs / 7.65 GiB), Compose 5.5.1 |
| Stack | Postgres 18.6 · Lakekeeper v0.13.6 (Iceberg REST) · RustFS 1.0.0 · lakehouse-init (curl 8.22.0) · Spark 4.1.3 + Iceberg 1.12.0 · Trino 483 · Airflow 3.3.2 + socket proxy |
| Host engines | DuckDB 1.5.6 · PyIceberg 0.12.0 · Polars 1.44.2 (no JVM) |
| Consumer | retention-radar `chore/sample-customer-santosh` ([PR #24](https://github.com/santoshshinde2012/retention-radar/pull/24)) at `98df572` and `07d8205`, Python 3.12, XGBoost 3.4.1 |

![Architecture: host engines, light / full / trino profiles, Airflow 3 and graph overlays, data/export and the consumers](docs/demo/architecture-e2e.png)

Source: [docs/demo/architecture-e2e.mmd](docs/demo/architecture-e2e.mmd); palette and rules: [docs/diagrams.md](docs/diagrams.md).

## Start-up and memory per profile

Start-up is the `make` step's wall time to healthy (`docker compose up --wait`), with images already built. Memory is the sum of `docker stats --no-stream` over this project's containers ([stack-up.excerpt.md](docs/demo/stack-up.excerpt.md)).

| Profile | Start-up | Memory |
|---|---:|---|
| light (Postgres, Lakekeeper, RustFS, lakehouse-init), empty volumes | 8.0 s | 186 MiB idle |
| full (+ Spark 4.1.3) | 11.6 s | 281 MiB idle (Spark idles between `spark-submit` runs) |
| full + graph overlay (`ldl-graph`) | in `make graph-e2e` | 521 MiB after `make graph-e2e` |
| full + trino (+ Trino 483) | in `make test-t3` | 1,494 MiB after `make test-t3` (Trino 909 MiB) |
| Airflow 3.3.2 overlay | 31.3 s ¹ | Airflow services 860 MiB; whole stack 2,427 MiB 20 s after start, 2,484 MiB after the three DAG runs |

¹ The first `make airflow-up` failed after 21.3 s because host port 8080 was taken by another project's container. The overlay was removed (`make airflow-down`) and started with `AIRFLOW_API_PORT=8085`; the time shown is that second start (its database was already initialised). This run also fixed `pipelines/airflow_wait.sh` to read the port from `.env`, as Compose does.

SILO (`STORE=silo`) was not part of this run.

## Tests

| Tier | Result | Excerpt |
|---|---|---|
| T0 `make test-t0` (unit + static, file names and links, no Docker) | 25 passed in 1.51 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| T1 `make test-t1` (REST catalog contract, testcontainers) | 5 passed in 6.73 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| T2 `make test-t2` (light-profile smoke) | 6 passed in 3.21 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| T3 `make test-t3` (Spark + Trino vs DuckDB / PyIceberg / Polars) | 7 passed in 196.79 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| Graph suite `make graph-test` | 1,403 passed, 0 failed, 32 skipped in 785.14 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| retention-radar `pytest` on the consumer checkout | 101 passed (`98df572`: 36.66 s; `07d8205`: 40.93 s) | [radar-consume.excerpt.md](docs/demo/radar-consume.excerpt.md) |

## End-to-end steps

Every step below exited 0 in this run, except the first `make airflow-up` (port clash, see ¹ above) and the first graph re-run, whose lineage step failed because the running `ldl-graph` held a stale single-file mount of the rewritten README; `make graph-e2e` now recreates that container, and the re-run passed. Times are wall time of each command.

| Step | Time | Result | Excerpt |
|---|---:|---|---|
| `make purge` | 1.1 s | this project's volumes deleted (none left) | [stack-up.excerpt.md](docs/demo/stack-up.excerpt.md) |
| `make up-light` | 8.0 s | light stack healthy | [stack-up.excerpt.md](docs/demo/stack-up.excerpt.md) |
| `make test-t1` / `make test-t2` | 7.4 s / 5.9 s | 5 passed / 6 passed | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| `make demo-light` | 5.6 s | retail 22 → 19 and time travel in 3 engines; churn twin 8,001 renewals | [light-demo.excerpt.md](docs/demo/light-demo.excerpt.md) |
| `make up-full` | 11.6 s | Spark 4.1.3 + Iceberg 1.12.0 healthy | [stack-up.excerpt.md](docs/demo/stack-up.excerpt.md) |
| `make e2e` (retail) | 93.8 s | bronze 22 → silver 19, gold 2 days, snapshot log + time travel | [retail-e2e.excerpt.md](docs/demo/retail-e2e.excerpt.md) |
| `make churn-sample` | 1.9 s | 8,001 subscriptions, 176,217 usage rows | [churn-e2e.excerpt.md](docs/demo/churn-e2e.excerpt.md) |
| `make churn-e2e` | 85.0 s | 8,001 renewals; 7,387 routed to the model; 3 exports | [churn-e2e.excerpt.md](docs/demo/churn-e2e.excerpt.md) |
| `check_churn_export.py --strict` | 0.7 s | export contract OK (7,387 renewals, 25 columns) | [churn-e2e.excerpt.md](docs/demo/churn-e2e.excerpt.md) |
| `make churn-parity` | 15.3 s | 8,001 × 27 match (Spark SQL vs pandas) | [churn-parity.excerpt.md](docs/demo/churn-parity.excerpt.md) |
| `radar_consume.sh` (`RADAR_REF=chore/sample-customer-santosh`) | 72.4 s | 7,387 rows scored (radar `98df572`) | [radar-consume.excerpt.md](docs/demo/radar-consume.excerpt.md) |
| radar `pytest` | 38.2 s | 101 passed | [radar-consume.excerpt.md](docs/demo/radar-consume.excerpt.md) |
| `make test-t3` (+ Trino 483) | 213.5 s | 7 passed | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| `make graph-test` | 787.1 s | 1,403 passed, 32 skipped | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| `make airflow-up` | 31.3 s | 3 DAGs parse, 0 import errors | [airflow-e2e.excerpt.md](docs/demo/airflow-e2e.excerpt.md) |
| `make airflow-demo` | 270.3 s | `lakehouse_retail_medallion` 5/5 and `lakehouse_churn_features` 4/4 tasks success | [airflow-e2e.excerpt.md](docs/demo/airflow-e2e.excerpt.md) |
| `lakehouse_graph` DAG | 240.3 s | 7/7 tasks success | [airflow-e2e.excerpt.md](docs/demo/airflow-e2e.excerpt.md) |
| `make graph-e2e` (Docker, REST) at `5d8e09f` | 103.1 s | publish → Iceberg build → strict contract → lineage → cohorts → promote | [graph-e2e.excerpt.md](docs/demo/graph-e2e.excerpt.md) |
| `make churn-gold-local` + strict export check | 3.9 s + 0.4 s | pandas twin export (7,387 rows), export contract OK | [churn-e2e.excerpt.md](docs/demo/churn-e2e.excerpt.md) |
| `make graph-local PROFILE=default` (strict) | 14.2 s | 40,204 nodes / 130,366 edges, golden s42, build `dde502e2a8e1` | [graph-e2e.excerpt.md](docs/demo/graph-e2e.excerpt.md) |
| s42: `graph-sample`, `graph-local`, `graph-cohorts`, `lineage-local` | 3.6 + 14.0 + 3.6 + 4.8 s | strict contract; 15 cohorts; lineage build `3999f2dea0cb` | [graph-e2e.excerpt.md](docs/demo/graph-e2e.excerpt.md) |
| `make test-t0` at `5d8e09f` | 2.0 s | 25 passed in 1.51 s | [tests.excerpt.md](docs/demo/tests.excerpt.md) |
| `scripts/graph_evidence.py --bench 20` | 378.6 s | regenerated [docs/graph/results/](docs/graph/results/index.md) | [index.md](docs/graph/results/index.md) |
| `make down` | | containers removed, volumes kept | |

## Pipeline numbers

**Retail medallion** ([retail-e2e.excerpt.md](docs/demo/retail-e2e.excerpt.md)):
- bronze `orders_raw` 22 rows → silver `orders` 19 (o-1003 / o-1006 deduped, pending dropped);
- gold `daily_order_metrics`: 2024-03-01 had 10 orders and revenue 424.94; 2024-03-02 had 9 orders and revenue 537.94;
- time travel to the day-1 snapshot reads 10 rows, against 22 now.

**Churn features** ([churn-e2e.excerpt.md](docs/demo/churn-e2e.excerpt.md)):

| Item | Value |
|---|---|
| Sample (seed 42) | 8,001 subscriptions, 176,217 usage rows, 10,602 limit events, 50,748 invoices, 2,134 tickets |
| Gold routes | `model` 7,387 (6,839 renewed, 548 voluntary lapse), `dunning` 326, `cancel_flow` 287, `score_today` 1 |
| Exports | `churn_user_features.csv` 7,387 renewals (voluntary-lapse rate 0.074); `churn_renewals_audit.csv` 8,001; `hero_inference_record.json` (sub_santosh) |
| sha256 | features `742f9028e421…`, hero `0db4f2de0cde…` (unchanged from earlier runs: the export is deterministic) |

**Parity** ([churn-parity.excerpt.md](docs/demo/churn-parity.excerpt.md)): Spark SQL gold and the pandas twin match on 8,001 renewals × 27 columns.

**Retention Radar consumes the export** ([radar-consume.excerpt.md](docs/demo/radar-consume.excerpt.md); radar's write-up of this run: [results/lakehouse-consume-e2e.md](https://github.com/santoshshinde2012/retention-radar/blob/chore/sample-customer-santosh/results/lakehouse-consume-e2e.md)): radar loaded 7,387 rows (churn rate 0.074) and scored all of them, with the same counts on both consumes.

| Action | Rows |
|---|---:|
| `no_action` | 6,499 |
| `cancel_flow_discount` | 410 |
| `limit_reset` | 276 |
| `pause_offer` | 111 |
| `holdout` | 87 |
| `personal_email` | 4 |

`auto_action` was `none` for every row.

## Graph layer

Full record: [docs/graph/results/index.md](docs/graph/results/index.md), regenerated in this run by `scripts/graph_evidence.py --bench 20` at `5d8e09f` (15 pass, 0 fail, 2 not run or not available).

- **Graph contracts:** tiny build `e2b501f9dbe9` 616 nodes / 1,949 edges; s42 and default build `dde502e2a8e1` 40,204 nodes / 130,366 edges. All strict, with point-in-time parity 0 mismatches × 6 features (pandas and Cypher).
- **Docker path:** `make graph-e2e` built the Iceberg-sourced build `b7591b1d7e0a` inside `ldl-graph` (read through the REST catalog, no keys in the container) and passed its strict contract there ([graph-e2e.excerpt.md](docs/demo/graph-e2e.excerpt.md)). The evidence script cannot re-check it on the host (its build id includes the Linux platform), so that row is "not run".
- **Agent tools:** tiny 59/59 checks; s42 97/97 (188.4 s). The macOS sandbox check passed 47/47.
- **Spark SQL twin parity:** tiny 1,182 and s42 80,010 SIMILAR_TO edges identical.
- **Lineage contract:** lineage build `3999f2dea0cb` (it follows the README and pipeline files); 30 gold SQL columns resolve.
- **Cohorts:** leiden and louvain each found 15 cohorts (modularity 0.8056 / 0.8065).
- **Not covered:** there is no LLM eval report; [docker-e2e.md](docs/graph/results/docker-e2e.md) keeps the timings of an earlier recorded Docker run (this run's Docker graph output is the excerpt above).

## Airflow 3 UI

Headless Playwright screenshots from the 2026-10-02 run (the DAGs and task chains are unchanged; this run's task states are in [airflow-e2e.excerpt.md](docs/demo/airflow-e2e.excerpt.md)):

| DAG list | Retail medallion |
|---|---|
| ![Airflow DAG list](docs/demo/img/airflow-dags.png) | ![lakehouse_retail_medallion runs](docs/demo/img/airflow-lakehouse_retail_medallion.png) |
| **Churn features** | **Graph** |
| ![lakehouse_churn_features runs](docs/demo/img/airflow-lakehouse_churn_features.png) | ![lakehouse_graph runs](docs/demo/img/airflow-lakehouse_graph.png) |

## CI

CI runs on every push to [PR #16](https://github.com/santoshshinde2012/local-data-lakehouse/pull/16) (label `full-stack`, so `t3-full` runs too); a newer push cancels the run of the previous head. The PR body links the run of the final head. The first push of this work:

| Head | Run | t0-unit | t2-light | t3-full | graph |
|---|---|---|---|---|---|
| `2fcb92f` (naming convention, link check) | [37109135526](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37109135526) | [2 min 18 s](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37109135526/job/111163479310) | [1 min 5 s](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37109135526/job/111163479346) | [5 min 29 s](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37109135526/job/111163479265) | [18 min 32 s](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37109135526/job/111163479150) |

retention-radar CI for the paired branch is on [PR #24](https://github.com/santoshshinde2012/retention-radar/pull/24) (`test` and `e2e-local`; `e2e-local` clones this branch).

## Known gaps

- **Strict default graph contract:** run `make churn-gold-local` first. Against a Spark-written export it reports the 2 rounding cells as drift ([lakehouse-twin.md](docs/graph/lakehouse-twin.md#why-gold-drifts-by-1e-4-in-two-cells)).
- **Radar drift check:** radar's `feature_stats.json` has no `psi_bins`, so the check falls back to SMD.
- **Airflow screenshots** are from the 2026-10-02 run; this run's DAG and task states are captured as text.
- **No LLM eval report** for the graph agent.
