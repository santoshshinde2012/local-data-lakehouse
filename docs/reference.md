# Reference: prerequisites, versions, ports and layout

Back to the [README](../README.md).

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop (or another engine) with **Compose v2.24+** | tested with Compose v5.5.1; `!override` in the SILO overlay needs 2.24+ |
| Make, Git | macOS / most Linux |
| [uv](https://docs.astral.sh/uv/) | `make venv` builds `.venv` (Python 3.12) from the hash-locked `requirements.txt` |
| Java 17 or 21 | only for `make churn-parity` (local pyspark 4.1.3) |
| Ports free | `8181` Lakekeeper, `9000` S3 API, `9001` store console, `4040` Spark UI, `8088` Trino, `8080` Airflow (all configurable in `.env`) |
| Host resolver | `objectstore.localhost` must resolve to loopback (macOS and systemd-resolved / nss-myhostname do; plain glibc does not: add `127.0.0.1 objectstore.localhost` to `/etc/hosts`) |

Apple Silicon (arm64) and amd64 are supported (every image is multi-arch).

## Other ways to run

### Trino

```bash
make up-full TRINO=1  # + Trino 483 on http://localhost:8088
docker compose --profile full --profile trino exec trino trino --execute \
  "SELECT * FROM lakehouse.gold.daily_order_metrics ORDER BY order_date"
```

### SILO instead of RustFS

```bash
make purge                       # catalog and objects must match: start from empty volumes
make up-light STORE=silo         # or: docker compose -f docker-compose.yml -f docker-compose.silo.yml --profile light up -d --wait
make demo-light
make down
```

`STORE=silo` works on every `up` / `test` target. See [object-store.md](object-store.md).

### Without Make

```bash
cp .env.example .env
docker compose --profile full up -d --build --wait
./pipelines/run_retail_e2e.sh
cp data/sample/churn/fixtures/tiny/*.csv data/sample/churn/   # or: python3 scripts/generate_churn_sample.py
./pipelines/run_churn_e2e.sh
docker compose --profile '*' down
```

## Versions

All images are pinned by tag **and** multi-arch digest; `tests/unit/test_stack_static.py` fails when a
tag floats, a digest is missing, or this table drifts from the pins.

| Component | Version | Image / package | Source |
|---|---|---|---|
| PostgreSQL | 18.6 | `postgres:18.6-alpine` | https://hub.docker.com/_/postgres |
| Lakekeeper | v0.13.6 | `quay.io/lakekeeper/catalog:v0.13.6` | https://github.com/lakekeeper/lakekeeper/releases |
| RustFS | 1.0.0 | `rustfs/rustfs:1.0.0` | https://github.com/rustfs/rustfs/releases |
| SILO | RELEASE.2026-09-16T00-00-00Z | `pgsty/silo:RELEASE.2026-09-16T00-00-00Z` | https://github.com/pgsty/silo |
| curl (init) | 8.22.0 | `curlimages/curl:8.22.0` | https://hub.docker.com/r/curlimages/curl |
| Apache Spark | Spark 4.1.3 (Scala 2.13, Java 21) | `apache/spark:4.1.3-scala2.13-java21-python3-ubuntu` | https://spark.apache.org/downloads.html |
| Apache Iceberg | Iceberg 1.12.0 | `iceberg-spark-runtime-4.1_2.13`, `iceberg-aws-bundle` | https://iceberg.apache.org/releases/ |
| Trino | 483 | `trinodb/trino:483` | https://trino.io/docs/current/release.html |
| Apache Airflow | 3.3.2 | `apache/airflow:3.3.2-python3.12` | https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html |
| socket-proxy | 1.13.1 | `wollomatic/socket-proxy:1.13.1` | https://github.com/wollomatic/socket-proxy |
| Docker CLI (Airflow image) | 29.8.2 | static binary, sha256-checked | https://download.docker.com/linux/static/stable/ |
| DuckDB | duckdb 1.5.6 | PyPI | https://pypi.org/project/duckdb/ |
| PyIceberg | pyiceberg 0.12.0 | PyPI | https://pypi.org/project/pyiceberg/ |
| Polars | polars 1.44.2 | PyPI | https://pypi.org/project/polars/ |
| PyArrow | pyarrow 25.0.1 | PyPI | https://pypi.org/project/pyarrow/ |
| testcontainers | 4.15.0 | PyPI | https://pypi.org/project/testcontainers/ |
| pyspark (parity check) | 4.1.3 | PyPI | https://pypi.org/project/pyspark/ |
| uv (CI) | 0.12.22 | PyPI | https://github.com/astral-sh/uv/releases |

**DuckDB 2.0** is due on 21 Oct 2026 and **1.5 reaches end of life on 1 Nov 2026**. This repo pins 1.5.6;
before moving to 2.0, re-lock `requirements.txt` and re-run `make test-t1 test-t2` (the iceberg
extension's `ATTACH` options may change). Spark 4.2 has no Iceberg runtime yet; 3.5.x is the LTS
fallback. The graph layer's local Spark harness uses the same pyspark 4.1.3 with
`iceberg-spark-runtime-4.1_2.13` 1.12.0 on a SQLite JDBC catalog ([MIGRATION.md](../MIGRATION.md)).

## UIs while the stack is up

| Service | URL | Credentials |
|---|---|---|
| Lakekeeper REST catalog | http://localhost:8181/catalog | none (no auth; local only) |
| Store console (RustFS or SILO) | http://localhost:9001 | `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env` (**sample-only**) |
| Spark UI | http://localhost:4040 | while a job runs |
| Trino | http://localhost:8088 | any user name, no password |
| Airflow | http://localhost:8080 | `admin` / generated `_AIRFLOW_WWW_USER_PASSWORD` in `.env` |

Objects live in bucket `lake` under `warehouse/`, one folder per Iceberg table.

## Make targets

| Target | What it does |
|---|---|
| `make env` / `make venv` | `.env` from `.env.example`; `.venv` (Python 3.12) from `requirements.txt` |
| `make up-light` · `make up-full [TRINO=1]` | start a profile and wait for health (`STORE=silo` swaps the store) |
| `make wait` · `make ps` · `make logs` · `make stats` | health wait, status, logs, `docker stats` of this project |
| `make demo-light` · `make e2e` · `make churn-e2e` · `make demo` | the demos (light; Spark retail; Spark renewal; both) |
| `make churn-sample` · `make churn-gold-local` · `make churn-check` · `make churn-parity` | the no-Docker renewal path |
| `make test-t0` … `make test-t3` · `make test` · `make graph-test` | test tiers |
| `make docs-check` | file-naming convention and relative Markdown links ([CONTRIBUTING.md](../CONTRIBUTING.md#file-names)) |
| `make airflow-up` · `airflow-wait` · `airflow-trigger-retail` · `airflow-trigger-churn` · `airflow-demo` · `airflow-down` | Airflow overlay |
| `make graph-*` | graph on gold (`make help` lists them) |
| `make down` | stop every profile and overlay (volumes kept) |
| `make purge` | stop everything and delete **this project's** volumes only |
| `make reset` | `make purge`, then `make up-light` |

## Repository layout

```text
local-data-lakehouse/
  config/                 # spark-defaults.conf, log4j2, trino/ catalog, catalog.md
  docker/spark/           # Spark 4.1.3 + Iceberg 1.12.0 image
  docker/init/            # lakehouse-init bootstrap (bucket, warehouse)
  docker/airflow/ docker/graph/
  src/jobs/retail|churn|graph/   # Spark jobs, one stage each
  src/lakehouse_client/   # host client: REST catalog, DuckDB ATTACH, Polars scan
  src/lakehouse_graph/    # graph on gold
  scripts/                # light demo, pandas twin, checks, graph CLIs
  pipelines/              # wait, submit, e2e and Airflow helper scripts (no business logic)
  sql/retail|churn/       # companion DDL / gold SQL
  tests/unit|contract|smoke|parity|graph/   # T0 | T1 | T2 | T3 | graph
  data/sample/            # retail CSVs + churn tiny fixture (churn/*.csv generated by make churn-sample)
  data/export/            # generated outputs (gitignored except .gitkeep)
  docs/demo/              # verified demo excerpts
  docker-compose.yml  docker-compose.silo.yml  docker-compose.airflow.yml  docker-compose.graph.yml
  Makefile  MIGRATION.md
```

Design notes:

- **Single responsibility**: each job owns one stage; pipelines only sequence `spark-submit`
- **One catalog**: every engine reads and writes through the REST catalog; no engine has store keys
- **Stable contracts**: table names and gold metrics are documented here, in `config/catalog.md` and under `sql/`
