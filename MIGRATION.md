# Migration: JDBC catalog + SILO + Spark 3.5 → Lakekeeper REST + RustFS + Iceberg 1.11 + Spark 4.1

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
| Spark | 3.5.3 (Scala 2.12, Java 17), Iceberg 1.6.1, hadoop-aws + aws-java-sdk-bundle, `s3a://` | **Spark 4.1.3** (Scala 2.13, Java 21), **Iceberg 1.11.0** runtime + `iceberg-aws-bundle`, `S3FileIO`, `s3://` | Current Iceberg; no Hadoop S3A stack. Spark 4.2 has no Iceberg runtime yet; 3.5.x is the LTS fallback (see README). |
| Profiles | one stack, always with the JVM | `light` (no JVM) and `full` (+ Spark, + optional Trino) | A laptop-sized default; JVM only when you need it. |
| Host engines | pandas only | **DuckDB 1.5.6**, **PyIceberg 0.12.0**, **Polars 1.44.2**, pyarrow 25.0.1 in a hash-locked `.venv` | Run the medallion without Spark (`make demo-light`). |
| Trino | none | **Trino 483**, optional (`TRINO=1`, port 8088) | Cross-engine parity in T3. |
| Airflow | 2.10.4 (EOL 2026-04-22), root, Docker socket, committed Fernet key, admin/admin, EXPOSE_CONFIG | **3.3.2**: api-server + scheduler + dag-processor, non-root, socket **proxy** limited to `docker exec` into ldl-spark/ldl-graph, generated secrets, config hidden, UI on 127.0.0.1 | Supported version; much smaller blast radius. Still a teaching overlay. |
| Tests | ad-hoc scripts | **T0** unit (no containers), **T1** testcontainers contract, **T2** light smoke, **T3** full cross-engine parity; `make test-t0…t3` | Each tier answers one question; CI runs T0-T2 per PR, T3 on main or label `full-stack`. |
| CI | checkout@v4, setup-python@v5, setup-java@v4 (Node 20, removed 2026-09-23) | checkout@v6, setup-python@v6, setup-java@v5 | Node 24 actions. |
| Radar step | cloned retention-radar `main` (v1 consumer) and failed on main | pinned v2 consumer commit unless a same-named branch exists (`pipelines/radar_consume.sh`) | See "Retention Radar step" below. |
| Graph | PyIceberg SqlCatalog on the JDBC tables (+ optional Postgres read-only role) | PyIceberg **REST** catalog (`type rest`), vended credentials, fsspec FileIO in the container | The graph container no longer touches Postgres; `config/graph/postgres_graph_ro.sql` removed. |
| Time travel demo | `05_query_timetravel.py` read only the latest snapshot | bronze day-1 snapshot (10 rows) vs current (22) via `VERSION AS OF`; the light demo shows the same with DuckDB `AT (VERSION => …)`, PyIceberg and Polars | Shows what Iceberg time travel is for. |
| Demo video | 49 MB `docs/demo/lakehouse-demo-original.mov` in the tree | removed from the tree (still in history) | Keep clones small; host the file as a release asset. |

## Table names

Unchanged: `bronze.*`, `silver.*`, `gold.*` as listed in [config/CATALOG.md](config/CATALOG.md).
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
- **Iceberg 1.11 rejects `option("snapshot-id")`.** Use `VERSION AS OF` or `option("versionAsOf", …)`.
- **Silver dedupe fix.** Bronze now records one `_ingested_at` per orders file and silver breaks ties
  on `_source_file DESC`; before, o-1006 (same `order_ts` on both days) made the silver count 18 or 19.
- **Graph Spark harness stays on 3.5.** `scripts/check_graph_parity.py` keeps its hermetic local
  harness (`requirements-graph-spark.txt`: pyspark 3.5.3 + iceberg-spark-runtime 3.5_2.12 1.6.1 on a
  SQLite JDBC catalog, no Docker). The stack and `scripts/check_gold_parity.py` use Spark 4.1.3.
- **Airflow:** DAGs import `airflow.sdk.DAG` and `airflow.providers.standard.operators.bash.BashOperator`.
  Secrets come from `pipelines/airflow_env.sh` (`make airflow-up`); on Linux it also writes `AIRFLOW_UID`.
  `make airflow-down` removes only the Airflow services.

## Retention Radar step (CI)

The step failed on `main` after PR #11 because retention-radar's `main` still reads the **v1** export
(`santosh_inference_record.json`), while this repo exports **v2** (`hero_inference_record.json`). The
radar-side v2 consumer (its PR #18, branch `feat/coding-assistant-renewal-v2`) was closed without being
merged, so the "same-named branch, else main" rule fell back to a v1 consumer on every push to main.
`pipelines/radar_consume.sh` now uses `RADAR_REF` if set, else a same-named radar branch, else the
pinned v2 consumer commit `953af3a`. When radar merges a v2 consumer to main, set `RADAR_V2_SHA` to that
commit (or to `main`).

## Verified on macOS arm64 (2026-10-02)

| Check | Result |
|---|---|
| T0 `make test-t0` | 19 passed |
| T1 `make test-t1` | 5 passed in 11.7 s (stack ready in 8.7 s) |
| T2 `make test-t2` | 6 passed in 3.0 s (RustFS); 6 passed in 4.25 s (`STORE=silo`) |
| T3 `make test-t3` (from empty volumes) | 7 passed in 234.7 s; max Spark vs pandas difference 1e-4 (`accept_rate_change`) |
| `make e2e` / `make churn-e2e` | OK in 111 s / 109 s; export contract `--strict` OK |
| `make churn-parity` with pyspark 4.1.3 | 8,001 × 27 match (20.8 s); tiny 121 × 27 (23.6 s) |
| `pipelines/run_graph_e2e.sh` | OK in 178 s; graph and lineage contracts pass |
| `make airflow-up` + `make airflow-demo` | healthy in 65 s; both DAGs `success` |
| `pipelines/radar_consume.sh` (Linux container) | radar v2 consumer scored 7,387 rows |
| `make graph-test` | 1,368 passed, 23 failed: 18 fail identically on `origin/main` (privacy-suppression tests and `test_evidence`), see [docs/graph/README.md](docs/graph/README.md#status) |

## Changelog

### 2026-10-02: local-first stack (`feat/local-first-stack-2026`)

- **Stack:** Lakekeeper v0.13.6 REST catalog on Postgres 18.6; RustFS 1.0.0 (default) or SILO
  RELEASE.2026-09-16T00-00-00Z (`STORE=silo`); `lakehouse-init` (curl 8.22.0); profiles `light`
  (no JVM), `full` (+ Spark 4.1.3), `trino` (+ Trino 483). Every image pinned by tag + digest.
- **Engines:** Spark 4.1.3 (Scala 2.13, Java 21) + Iceberg 1.11.0 with `S3FileIO`; host DuckDB 1.5.6,
  PyIceberg 0.12.0, Polars 1.44.2, pyarrow 25.0.1 (`requirements.txt`, hash-locked, uv).
- **Security:** no secrets in tracked files; vended credentials; non-root containers; Airflow behind a
  socket proxy with generated secrets.
- **Airflow 3.3.2** overlay (was 2.10.4).
- **Graph:** PyIceberg REST catalog (was `SqlCatalog` on the JDBC tables); Postgres read-only role removed.
- **Tests and CI:** T0–T3 tiers; CI on Node 24 actions, uv 0.12.4, pyspark 4.1.3 parity on Java 21;
  the Retention Radar step uses radar's v2 consumer.
- **Fixes:** nondeterministic silver dedupe; Iceberg 1.11 time-travel option; lineage extractor
  resolution of the bronze ingest and the radar step.
- **Docs:** README, `config/CATALOG.md`, `docs/object-store.md`, `docs/graph/*`, demo excerpts; the
  49 MB demo video removed from the tree (attach it to a GitHub Release).
