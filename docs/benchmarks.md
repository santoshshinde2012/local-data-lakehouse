# Benchmarks

Measured numbers from real runs on one laptop. Nothing here is estimated or typed from memory: each
table names the log or file it comes from. Treat them as indicative. One machine, one day, with
whatever else the machine was running.

| | |
|---|---|
| Date | 2026-10-03 (IST) |
| Machine | MacBook Pro, Apple M1 Pro, 16 GB, macOS 26.6.2 |
| Docker | Docker Desktop 29.8.1 (VM 10 CPUs / 7.65 GiB), Compose 5.5.1 |
| Stack | Postgres 18.6 · Lakekeeper v0.13.6 · RustFS 1.0.0 · Spark 4.1.3 + Iceberg 1.12.0 · Trino 483 · Airflow 3.3.2 |
| Runs | **A**: 13:50 IST at `2fcb92f` (graph steps at `5d8e09f`), from empty volumes, written up in [RESULTS.md](../RESULTS.md). **B**: 18:01 IST at `59b08b6`, the re-run in [docker-e2e.md](graph/results/docker-e2e.md). **C**: 18:40 IST, a warm restart on B's volumes to take the [snapshots](demo/README.md#snapshots) |

## Start-up

Wall-clock time of the `make` target until every container is healthy.

| Target | Run A (empty volumes) | Run B | Run C (warm volumes) |
|---|---:|---:|---:|
| `make up-light` | 8.0 s | 7.8 s | |
| `make up-full` | 11.6 s | 11.8 s | |
| `make up-full TRINO=1` | | | 16 s |
| `make airflow-up` (`AIRFLOW_API_PORT=8085`) | 31.3 s | 34.1 s | 31 s |

## Pipeline steps

| Step | Run A | Run B |
|---|---:|---:|
| `make demo-light` | 5.6 s | 4.9 s |
| `make e2e` (retail medallion, Spark) | 93.8 s | 110.8 s |
| `make churn-sample` | 1.9 s | 2.1 s |
| `make churn-e2e` (bronze → silver → gold → 3 exports) | 85.0 s | 92.4 s |
| `make churn-parity` (Spark SQL vs pandas, 8,001 × 27) | 15.3 s | 15.8 s |
| radar consume (train + score 7,387 renewals) | 72.4 s | 75.9 s |
| radar `pytest` on the consumer checkout | 38.2 s | 41.7 s |
| `make graph-e2e` (Docker, REST catalog) | 103.1 s | 126.6 s |
| `make churn-gold-local` (no Docker) | 3.9 s | 4.0 s |
| `make graph-local PROFILE=default` (strict) | 14.2 s | 14.9 s |
| `make airflow-demo` (two DAGs) | 270.3 s | 279.7 s |
| `lakehouse_graph` DAG, trigger to done | 240.3 s | 141.8 s |

Source: `steps.txt` of each run (exit code, seconds, finish time per step), summarised in
[RESULTS.md](../RESULTS.md#end-to-end-steps) and [docker-e2e.md](graph/results/docker-e2e.md).

### Airflow DAG runs (run B)

Start and end times from the Airflow REST API (`/api/v2/dags/<dag>/dagRuns`), shown in the
[run snapshots](demo/README.md#snapshots).

| DAG | Run | Duration | Tasks |
|---|---|---:|---|
| `lakehouse_retail_medallion` | `manual__ldl_20261003T124500` | 137.7 s | 5/5 success |
| `lakehouse_churn_features` | `manual__ldl_20261003T124732` | 112.8 s | 4/4 success |
| `lakehouse_graph` | `manual__ldl_20261003T124941` | 126.7 s | 7/7 success |

Slowest `lakehouse_graph` tasks: `publish_gold_graph` 56.2 s, `build_graph` 24.8 s,
`check_graph_contract` 24.2 s, `build_lineage` 6.3 s, `build_cohorts` 5.3 s.

## Tests

| Tier | Run A | Run B |
|---|---:|---:|
| T0 `make test-t0` (no Docker) | 1.5 s | 1.7 s |
| T1 `make test-t1` (REST catalog contract) | 7.4 s | 10.9 s |
| T2 `make test-t2` (light smoke) | 5.9 s | 5.3 s |
| T3 `make test-t3` (Spark + Trino vs DuckDB / PyIceberg / Polars) | 213.5 s | 224.1 s |
| `make graph-test` (1,403 passed, 32 skipped) | 787.1 s | |

## Queries

| Query | Engine | Time |
|---|---|---:|
| Lapse rate by plan over `gold.churn_renewal_features` (8,001 rows) | Trino 483 | 392 ms elapsed (71 ms planning, 305 ms execution) |

Source: the Trino UI query page in run C ([trino-query.png](demo/img/trino-query.png)).

## Graph tools

Profile s42 (40,204 nodes, 130,366 edges), 20 warm calls per tool. Full table and file sizes in
[bench.md](graph/results/bench.md).

| Item | Value |
|---|---:|
| Fresh build, wall clock | 6.63 s (builder 4.32 s, max RSS 312 MiB) |
| Ladybug load | 1.4 s (max RSS 231 MiB) |
| MCP server start (graph / metrics / lineage / cohorts) | 0.99 / 0.87 / 0.84 / 0.85 s |

| Tool | In process p50 / p95 (ms) | Over stdio p50 / p95 (ms) |
|---|---:|---:|
| `graph_renewal_evidence` | 5.59 / 7.08 | 6.15 / 6.89 |
| `graph_similar_renewals` | 15.8 / 18.87 | 17.34 / 19.78 |
| `graph_exposure` | 3.02 / 3.2 | 4.28 / 4.79 |
| `metric_lapse_rate` | 0.39 / 0.42 | 1.7 / 2.14 |
| `lineage_trace` | 2.95 / 3.45 | 4.77 / 5.94 |
| `cohort_list` | 23.99 / 27.54 | 25.59 / 27.43 |

## Memory

`docker stats --no-stream`, summed over this project's containers.

| Moment | Total | Biggest |
|---|---:|---|
| light profile, idle (run A) | 186 MiB | RustFS 108 MiB |
| full profile, idle (run A) | 281 MiB | RustFS 162 MiB |
| after `make test-t3` (run A) | 1,494 MiB | Trino 909 MiB |
| after the three DAG runs (run A) | 2,484 MiB | Trino 843 MiB, Airflow scheduler 445 MiB |
| full + Trino + Airflow, idle after start (run C) | 1,836 MiB | Trino 699 MiB, Airflow services 870 MiB together |

The graph container is capped at 1.5 GiB; the graph builder peaked at 312 MiB RSS (see above).

## Disk

| Item | Size |
|---|---:|
| Images this stack uses (graph 629 MB, Spark 1.42 GB, Airflow 2.44 GB, Lakekeeper 166 MB, Postgres 298 MB, RustFS 247 MB, curl 25.8 MB, Trino 1.38 GB) | 6.6 GB |
| Volume `postgres-data` (catalog) | 70.7 MB |
| Volume `objectstore-data` (Iceberg files; 1,169 objects, 34.7 MiB in the RustFS console) | 37.3 MB |
| Volume `airflow-postgres-data` | 69.6 MB |
| Volume `airflow-logs` | 0.7 MB |
| `data/graph/` (builds for default, s42 and tiny) | 321 MB |
| `data/export/` | 2.2 MB |

Images: `docker images` in run A. Volumes and folders: `docker system df -v` and `du -sh` after run C.

## CI

GitHub-hosted `ubuntu-latest` runners, the run on `main` after
[PR #16](https://github.com/santoshshinde2012/local-data-lakehouse/pull/16) merged
([run 37119697670](https://github.com/santoshshinde2012/local-data-lakehouse/actions/runs/37119697670)).

| Job | What it runs | Duration |
|---|---|---:|
| `t0-unit` | syntax and docs checks, T0, no-Docker gold + strict contract, Spark vs pandas parity, radar consumes the export | 2 min 29 s |
| `t2-light` | Compose validation, image pulls, T1, T2, light demo | 1 min 5 s |
| `t3-full` | churn sample, full stack + Trino, T3 | 5 min 31 s |
| `graph` | graph test suite (`tests/graph`) | 13 min 40 s |

## Reproduce

```bash
make up-light && make test-t1 test-t2 demo-light
make up-full TRINO=1 && make e2e churn-sample churn-e2e churn-parity test-t3
make graph-e2e graph-test
AIRFLOW_API_PORT=8085 make airflow-up airflow-demo
make down          # keeps the volumes
```

Time each target with your shell (`time make …`) and `docker stats --no-stream` between steps.
