# Stack start-up, memory and images

Captured on 2026-10-02 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `feat/local-first-stack-2026` at `7f5fc43`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## `make purge` → `make up-light` (light profile: Postgres, Lakekeeper, RustFS, lakehouse-init)

Exit 0, 0.4 s.

```text
$ make purge
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-unused} AIRFLOW_JWT_SECRET=${AIRFLOW_JWT_SECRET:-unused} AIRFLOW_API_SECRET_KEY=${AIRFLOW_API_SECRET_KEY:-unused} AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-un …
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-unused} AIRFLOW_JWT_SECRET=${AIRFLOW_JWT_SECRET:-unused} AIRFLOW_API_SECRET_KEY=${AIRFLOW_API_SECRET_KEY:-unused} AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-un …
```

Exit 0, 7.9 s.

```text
$ make up-light
docker compose -f docker-compose.yml  --profile light up -d --wait
==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001
```

`docker stats --no-stream`, 20 s after the light stack was healthy:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-lakehouse-init   12.79MiB / 7.651GiB   1.14%
ldl-lakekeeper       23.37MiB / 7.651GiB   3.49%
ldl-objectstore      118.6MiB / 7.651GiB   0.04%
ldl-postgres         67.01MiB / 7.651GiB   0.05%
```

After `make demo-light`:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-lakehouse-init   12.58MiB / 7.651GiB   2.04%
ldl-lakekeeper       29.18MiB / 7.651GiB   0.00%
ldl-objectstore      168.2MiB / 7.651GiB   0.04%
ldl-postgres         90.3MiB / 7.651GiB    2.13%
```

## `make up-full` (adds Spark 4.1.3 + Iceberg 1.12.0)

Exit 0, 11.1 s.

```text
$ make up-full
docker compose -f docker-compose.yml  --profile full  up -d --build --wait
==> full up: Spark UI http://localhost:4040 (while a job runs)
```

Idle, 20 s after start (Spark idles between `spark-submit` runs):

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            2.184MiB / 7.651GiB   0.00%
ldl-lakehouse-init   13.57MiB / 7.651GiB   2.17%
ldl-lakekeeper       27.12MiB / 7.651GiB   3.33%
ldl-objectstore      166.2MiB / 7.651GiB   1.74%
ldl-postgres         94.48MiB / 7.651GiB   1.87%
```

With the graph overlay (`ldl-graph`) after `make graph-e2e`:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            1.395MiB / 7.651GiB   0.00%
ldl-graph            5.871MiB / 1.5GiB     0.00%
ldl-lakehouse-init   13.49MiB / 7.651GiB   1.60%
ldl-lakekeeper       33.82MiB / 7.651GiB   2.66%
ldl-objectstore      362.4MiB / 7.651GiB   1.82%
ldl-postgres         101.4MiB / 7.651GiB   2.23%
```

Full + Trino 483 after `make test-t3`:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            1.562MiB / 7.651GiB   0.00%
ldl-trino            951.1MiB / 7.651GiB   3.86%
ldl-graph            5.797MiB / 1.5GiB     0.00%
ldl-lakehouse-init   12.27MiB / 7.651GiB   1.18%
ldl-lakekeeper       36.34MiB / 7.651GiB   3.90%
ldl-objectstore      422MiB / 7.651GiB     1.53%
ldl-postgres         107.4MiB / 7.651GiB   0.08%
```

## Airflow 3.3.2 overlay

20 s after `make airflow-up`:

```text
NAME                        MEM USAGE / LIMIT     CPU %
ldl-airflow-apiserver       230.4MiB / 7.651GiB   0.22%
ldl-airflow-scheduler       424MiB / 7.651GiB     2.09%
ldl-airflow-dag-processor   171.8MiB / 7.651GiB   1.12%
ldl-airflow-postgres        43.79MiB / 7.651GiB   1.39%
ldl-docker-proxy            5.082MiB / 7.651GiB   0.00%
ldl-spark                   1.531MiB / 7.651GiB   0.00%
ldl-trino                   952.9MiB / 7.651GiB   4.53%
ldl-graph                   5.793MiB / 1.5GiB     0.00%
ldl-lakehouse-init          12.86MiB / 7.651GiB   1.39%
ldl-lakekeeper              33.43MiB / 7.651GiB   0.02%
ldl-objectstore             423MiB / 7.651GiB     0.03%
ldl-postgres                108.5MiB / 7.651GiB   2.43%
```

After the three DAG runs:

```text
NAME                        MEM USAGE / LIMIT     CPU %
ldl-airflow-apiserver       183.4MiB / 7.651GiB   0.24%
ldl-airflow-scheduler       373MiB / 7.651GiB     1.71%
ldl-airflow-dag-processor   121.8MiB / 7.651GiB   0.44%
ldl-airflow-postgres        40.57MiB / 7.651GiB   0.96%
ldl-docker-proxy            15.96MiB / 7.651GiB   2.48%
ldl-spark                   26.53MiB / 7.651GiB   0.00%
ldl-trino                   625.1MiB / 7.651GiB   2.17%
ldl-graph                   218.7MiB / 1.5GiB     0.01%
ldl-lakehouse-init          2.02MiB / 7.651GiB    1.29%
ldl-lakekeeper              34.49MiB / 7.651GiB   0.36%
ldl-objectstore             503.1MiB / 7.651GiB   1.58%
ldl-postgres                101.3MiB / 7.651GiB   2.62%
```

## Images

```text
ldl-graph:local 629MB
ldl-spark:4.1.3-iceberg1.12.0 1.42GB
ldl-airflow:3.3.2 2.44GB
ldl-spark:4.1.3-iceberg1.11.0 1.42GB
quay.io/lakekeeper/catalog:v0.13.6 166MB
postgres:18.6-alpine 298MB
rustfs/rustfs:1.0.0 247MB
curlimages/curl:8.22.0 25.8MB
trinodb/trino:483 1.38GB
```

`ldl-spark:4.1.3-iceberg1.11.0` is the image from before the Iceberg bump; nothing uses it (`docker image rm` it to reclaim 1.4 GB).
