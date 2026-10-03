# Migration: JDBC catalog + SILO + Spark 3.5 → Lakekeeper REST + RustFS + Iceberg 1.12 + Spark 4.1

This is the October 2026 "local-first stack" upgrade (branch `feat/local-first-stack-2026`).
It changes how every engine finds tables, so **old volumes cannot be reused**: start clean.

```bash
make down
docker volume ls --filter label=com.docker.compose.project=local-data-lakehouse   # only this project's
make venv             # host .venv (Python 3.12, uv, hash-locked requirements.txt)
make reset            # make purge (THIS project's volumes) + make up-light
make demo-light       # or: make up-full && make demo
```

`make purge` only removes volumes of this Compose project (`postgres-data`, `objectstore-data`,
`silo-data`, `airflow-postgres-data`, `airflow-logs`); it never prunes other projects, images or the
build cache. It runs `down -v` twice (with and without the SILO overlay) because `down -v` only removes
volumes that a service of the loaded model mounts.

## What changed and why

| Area | Before | After | Why |
|---|---|---|---|
| Catalog | Iceberg JDBC catalog in Postgres 16 (`iceberg_tables`) | **Lakekeeper v0.13.6** REST catalog (Apache-2.0) on **Postgres 18.6** | One catalog every engine speaks (Spark, Trino, DuckDB, PyIceberg, Polars); vended credentials; no JDBC driver or SQL-level catalog access in clients. |
| Credentials | `spark-defaults.conf` hard-coded `minioadmin` keys; every client held root S3 keys | `.env` only (compose fails fast when unset); engines get **short-lived STS credentials** from Lakekeeper (`X-Iceberg-Access-Delegation: vended-credentials`) | No secrets in tracked files; least privilege. |
| Object store | SILO `RELEASE.2026-09-03…` + `pgsty/mc` init | **RustFS 1.0.0** default; **SILO `RELEASE.2026-09-16…`** (security release) as `STORE=silo` | Apache-2.0, STS support, small. SILO kept as a drop-in. |
| Endpoint | `http://silo:9000` (in-network only) | `http://objectstore.localhost:9000` everywhere | Vended endpoint must work from containers and the host. |
| Spark | 3.5.3 (Scala 2.12, Java 17), Iceberg 1.6.1, hadoop-aws + aws-java-sdk-bundle, `s3a://` | **Spark 4.1.3** (Scala 2.13, Java 21), **Iceberg 1.12.0** runtime + `iceberg-aws-bundle`, `S3FileIO`, `s3://` | Current Iceberg; no Hadoop S3A stack. Spark 4.2 has no Iceberg runtime yet; 3.5.x is the LTS fallback (see README). |
| Profiles | one stack, always with the JVM | `light` (no JVM) and `full` (+ Spark, + optional Trino) | A laptop-sized default; JVM only when you need it. |
| Host engines | pandas only | **DuckDB 1.5.6**, **PyIceberg 0.12.0**, **Polars 1.44.2**, pyarrow 25.0.1 in a hash-locked `.venv` | Run the medallion without Spark (`make demo-light`). |
| Trino | none | **Trino 483**, optional (`TRINO=1`, port 8088) | Cross-engine parity in T3. |
| Airflow | 2.10.4 (EOL 2026-04-22), root, Docker socket, committed Fernet key, admin/admin, EXPOSE_CONFIG | **3.3.2**: api-server + scheduler + dag-processor, non-root, socket **proxy** limited to `docker exec` into ldl-spark/ldl-graph, generated secrets, config hidden, UI on 127.0.0.1 | Supported version; much smaller blast radius. Still a teaching overlay. |
| Tests | ad-hoc scripts | **T0** unit (no containers), **T1** testcontainers contract, **T2** light smoke, **T3** full cross-engine parity; `make test-t0…t3` | Each tier answers one question; CI runs T0-T2 per PR, T3 on main or label `full-stack`. |
| CI | checkout@v4, setup-python@v5, setup-java@v4 (Node 20, removed 2026-09-23) | checkout@v7, setup-python@v7, setup-java@v6; uv 0.12.22 | Node 24 actions. |
| Radar step | cloned retention-radar `main` (v1 consumer) and failed on main | retention-radar `main`, which reads the v2 export since radar #21 (`pipelines/radar_consume.sh`; `RADAR_REF` to override) | See "Retention Radar step" below. |
| Graph | PyIceberg SqlCatalog on the JDBC tables (+ optional Postgres read-only role) | PyIceberg **REST** catalog (`type rest`), vended credentials, fsspec FileIO in the container | The graph container no longer touches Postgres; `config/graph/postgres_graph_ro.sql` removed. |
| Time travel demo | `05_query_timetravel.py` read only the latest snapshot | bronze day-1 snapshot (10 rows) vs current (22) via `VERSION AS OF`; the light demo shows the same with DuckDB `AT (VERSION => …)`, PyIceberg and Polars | Shows what Iceberg time travel is for. |
| Demo video | 49 MB `docs/demo/lakehouse-demo-original.mov` in the tree | removed from the tree (still in history) | Keep clones small; host the file as a release asset. |

## Table names

Unchanged: `bronze.*`, `silver.*`, `gold.*` as listed in [catalog.md](catalog.md).
New: `gold.churn_renewal_features_twin` (pandas twin published by the light demo / T3 via PyIceberg).
Spark owns `gold.churn_renewal_features`; DuckDB in the light demo writes the retail tables with
Spark-compatible types (`TIMESTAMPTZ`), so `make e2e` can append to them afterwards.

## Things to know

- **DuckDB 2.0 is due 21 Oct 2026; 1.5 is end of life on 1 Nov 2026.** The lock pins 1.5.6. Re-lock and
  re-run `make test-t1 test-t2` when moving to 2.0 (the iceberg extension's `ATTACH` options may move).
- **Polars:** its native Iceberg reader ignores vended credentials and calls the EC2 metadata service
  (169.254.169.254). `lakehouse_client.polars_scan` always uses `reader_override="pyiceberg"`.
- **Do not set `LAKEKEEPER__BASE_URI`.** Lakekeeper then derives URLs from the request's Host header,
  which is right both for `lakekeeper:8181` (containers) and `localhost:8181` (host).
- **Linux hosts:** the Spark container runs as uid 185 and writes `data/export`: `chmod a+rwx data/export`.
- Spark 4 enables ANSI SQL by default; the retail and churn SQL ran unchanged (no ANSI failures).
- OpenLineage for Spark moved to the Scala 2.13 artifact (`openlineage-spark_2.13`).
- **`lakehouse-init` no longer exits.** Compose v5 `up --wait` returned 1 when a one-shot service
  exited 0, so the init service now stays up idle after setup and turns healthy (`/tmp/ready`); Spark,
  Trino and the graph container depend on `service_healthy`. `pipelines/create_bucket.sh` re-runs it
  with `--once`. Only `lakekeeper-migrate` is a one-shot.
- **DuckDB stale connection.** A DuckDB connection attached before another engine replaced a table can
  raise "Metadata-log exists but none of the entries were valid for the current transaction start
  time" (duckdb-iceberg 1.5.6). Open a fresh connection after a writer in another process.
- **Vended credentials expire after laptop sleep.** A Spark job that spans a sleep fails with S3
  `400 Bad Request` and "Failed to refresh storage credentials … Invalid credentials endpoint: null".
  Re-run the job.
- **Iceberg 1.11+ (also 1.12) rejects `option("snapshot-id")`.** Use `VERSION AS OF` or `option("versionAsOf", …)`.
- **Silver dedupe fix.** Bronze now records one `_ingested_at` per orders file and silver breaks ties
  on `_source_file DESC`; before, o-1006 (same `order_ts` on both days) made the silver count 18 or 19.
- **Graph Spark harness on 4.1.** `scripts/check_graph_parity.py` keeps its hermetic local harness
  (SQLite JDBC catalog, no Docker), now on pyspark 4.1.3 + `iceberg-spark-runtime-4.1_2.13` 1.12.0 and a
  JDK 17 or 21, like the stack. Put the runtime jar in `~/.m2` or `GRAPH_SPARK_JARS_DIR` (it is never
  downloaded at run time). `requirements-graph-spark.txt` no longer pulls psycopg2 (LGPL).
- **Iceberg 1.12** removes the deprecated S3 signer classes and moves the AWS HTTP client to Apache
  HttpClient 5. Neither is configured here; the jobs ran unchanged on RustFS and on SILO.
- **Switching `STORE`** keeps the Postgres catalog but not the objects: `make reset` after a switch
  (T2 on RustFS right after a SILO run fails with `FileNotFoundError` until then).
- **Airflow:** DAGs import `airflow.sdk.DAG` and `airflow.providers.standard.operators.bash.BashOperator`.
  Secrets come from `pipelines/airflow_env.sh` (`make airflow-up`); on Linux it also writes `AIRFLOW_UID`.
  `make airflow-down` removes only the Airflow services.

## Retention Radar step (CI)

The step failed on `main` after PR #11 because retention-radar's `main` still reads the **v1** export
(`santosh_inference_record.json`), while this repo exports **v2** (`hero_inference_record.json`). The
radar-side v2 consumer (its PR #18, branch `feat/coding-assistant-renewal-v2`) was closed without being
merged, so the "same-named branch, else main" rule fell back to a v1 consumer on every push to main.
Radar PR #21 put the v2 reader on radar `main` (squash merge `7e3bec8`, 2026-10-02). CI and
`pipelines/radar_consume.sh` now clone radar `main`; there is no same-named-branch lookup and no pinned
v2 fallback any more. `RADAR_REF=<branch or commit>` points the script at a paired radar change.
Re-checked against radar `main` `7e3bec8` on macOS: 7,387 rows scored, same action counts.

## Verified on macOS arm64 (2026-10-02)

Re-run after the Iceberg 1.12.0 / graph dependency bump, except where marked.

| Check | Result |
|---|---|
| T0 `make test-t0` | 19 passed |
| T1 `make test-t1` | 5 passed in 5.7 s |
| T2 `make test-t2` | 6 passed in 2.8 s (RustFS); 6 passed in 3.4 s (`STORE=silo`) |
| T3 `make test-t3` (from empty volumes) | 7 passed in 188.4 s; max Spark vs pandas difference 1e-4 (`accept_rate_change`) |
| `make e2e` / `make churn-e2e` | OK in 98 s / 90 s on RustFS, 101 s / 86 s on SILO (`STORE=silo`); export contract `--strict` OK. Full chain from empty volumes (`make purge`, `make up-full` 15 s): 100 s / 87 s, 8,001 renewals, 7,387 routed to the model |
| `make churn-parity` with pyspark 4.1.3 (no Iceberg) | 8,001 × 27 match in 16.6 s (re-run after the bump; the target now uses `.venv-graph-spark`); tiny 121 × 27 (23.6 s, before the bump) |
| `make graph-e2e` | OK in 113 s from empty volumes including the overlay build (`run_graph_e2e.sh` 97 s); graph (strict, gold drift 2 cells, info) and lineage contracts pass |
| `make churn-gold-local` + `make graph-local PROFILE=default` (strict) | OK in 17 s: 40,204 nodes / 130,366 edges, golden s42, export cross-check exact. Against the Spark-written export it fails on the 2 known rounding cells ([lakehouse-twin.md](graph/lakehouse-twin.md#why-gold-drifts-by-1e-4-in-two-cells)), so run `churn-gold-local` first |
| `check_graph_parity.py parity --profile tiny --strict` (pyspark 4.1.3) | OK in 23 s; Spark harness tests 10 passed |
| `make airflow-up` + `make airflow-demo` (Airflow 3) | healthy in 37 s; 3 DAGs parse, 0 import errors; retail and churn `success` (demo 212 s); `lakehouse_graph` triggered after them: `success` in 122 s |
| `pipelines/radar_consume.sh` (macOS, radar `feat/local-first-stack-2026` = radar PR #21; re-checked on radar `main` `7e3bec8` after the merge) | fresh clone + venv in 66 s, sync + ingest + batch score 7,387 rows; radar `pytest` 96 passed in 34 s |
| `make graph-test` | 1,391 passed, 0 failed, 32 skipped in 12 min (the 18 failures that were also on `origin/main` are fixed; see [docs/graph/README.md](graph/README.md#status)) |

The change list for this upgrade is in [CHANGELOG.md](../CHANGELOG.md#2026-10-02-local-first-stack).
