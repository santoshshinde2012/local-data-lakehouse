# Stack start-up, memory and images

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `2fcb92f`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices, blank lines); long outputs keep their head and tail. `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## `make purge` (deletes this project's volumes)

Exit 0, 1.1 s.

```text
$ make purge
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-unused} AIRFLOW_JWT_SECRET=${AIRFLOW_JWT_SECRET:-unused} AIRFLOW_API_SECRET_KEY=${AIRFLOW_API_SECRET_KEY:-unused} AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-u …
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-unused} AIRFLOW_JWT_SECRET=${AIRFLOW_JWT_SECRET:-unused} AIRFLOW_API_SECRET_KEY=${AIRFLOW_API_SECRET_KEY:-unused} AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-u …
```

Volumes of this project after `make purge`:

```text
DRIVER    VOLUME NAME
```

## `make up-light` (light profile: Postgres, Lakekeeper, RustFS, lakehouse-init)

Exit 0, 8 s.

```text
$ make up-light
docker compose -f docker-compose.yml  --profile light up -d --wait
==> light up: catalog http://localhost:8181/catalog  S3 http://objectstore.localhost:9000  console http://localhost:9001
```

`docker stats --no-stream`, 20 s after the light stack was healthy:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-lakehouse-init   1.871MiB / 7.651GiB   1.26%
ldl-lakekeeper       21.78MiB / 7.651GiB   3.10%
ldl-objectstore      108.2MiB / 7.651GiB   1.66%
ldl-postgres         53.78MiB / 7.651GiB   1.62%
```

## `make up-full` (adds Spark 4.1.3 + Iceberg 1.12.0)

Exit 0, 11.6 s.

```text
$ make up-full
docker compose -f docker-compose.yml  --profile full  up -d --build --wait
==> full up: Spark UI http://localhost:4040 (while a job runs)
```

Idle, 20 s after start (Spark idles between `spark-submit` runs):

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            708KiB / 7.651GiB     0.00%
ldl-lakehouse-init   1.465MiB / 7.651GiB   1.96%
ldl-lakekeeper       33.96MiB / 7.651GiB   0.04%
ldl-objectstore      162.4MiB / 7.651GiB   1.36%
ldl-postgres         82.05MiB / 7.651GiB   1.46%
```

With the graph overlay (`ldl-graph`) after `make graph-e2e`:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            1.488MiB / 7.651GiB   0.00%
ldl-graph            5.586MiB / 1.5GiB     0.00%
ldl-lakehouse-init   2.211MiB / 7.651GiB   1.36%
ldl-lakekeeper       49.71MiB / 7.651GiB   3.93%
ldl-objectstore      368.6MiB / 7.651GiB   0.02%
ldl-postgres         92.93MiB / 7.651GiB   0.05%
```

Full + Trino 483 after `make test-t3`:

```text
NAME                 MEM USAGE / LIMIT     CPU %
ldl-spark            1.562MiB / 7.651GiB   0.00%
ldl-trino            909.4MiB / 7.651GiB   3.04%
ldl-graph            6.039MiB / 1.5GiB     0.00%
ldl-lakehouse-init   1.984MiB / 7.651GiB   1.12%
ldl-lakekeeper       50.24MiB / 7.651GiB   0.00%
ldl-objectstore      428.9MiB / 7.651GiB   1.46%
ldl-postgres         95.49MiB / 7.651GiB   1.97%
```

## Airflow 3.3.2 overlay

Exit 0, 31.3 s. The first `make airflow-up` of this run failed after 21.3 s: host port 127.0.0.1:8080 was already taken by a container of another project (`Bind for 127.0.0.1:8080 failed: port is already allocated`). The overlay was removed with `make airflow-down` (1.7 s) and started again with `AIRFLOW_API_PORT=8085`; that start is shown. The Airflow images were already built and its database initialised by the first attempt.

```text
$ make airflow-up
./pipelines/airflow_env.sh
==> Airflow UI user: admin (password: _AIRFLOW_WWW_USER_PASSWORD in .env)
docker compose -f docker-compose.yml  -f docker-compose.airflow.yml --profile full up -d --build --wait
```

20 s after `make airflow-up`:

```text
NAME                        MEM USAGE / LIMIT     CPU %
ldl-airflow-apiserver       229.8MiB / 7.651GiB   0.15%
ldl-airflow-dag-processor   171.4MiB / 7.651GiB   1.00%
ldl-airflow-scheduler       415MiB / 7.651GiB     1.51%
ldl-airflow-postgres        38.77MiB / 7.651GiB   4.34%
ldl-docker-proxy            5.082MiB / 7.651GiB   3.87%
ldl-spark                   1.508MiB / 7.651GiB   0.00%
ldl-trino                   1005MiB / 7.651GiB    2.27%
ldl-graph                   6.039MiB / 1.5GiB     0.01%
ldl-lakehouse-init          1.16MiB / 7.651GiB    1.22%
ldl-lakekeeper              52.73MiB / 7.651GiB   3.52%
ldl-objectstore             405.4MiB / 7.651GiB   1.81%
ldl-postgres                94.77MiB / 7.651GiB   2.66%
```

After the three DAG runs:

```text
NAME                        MEM USAGE / LIMIT     CPU %
ldl-airflow-apiserver       148.1MiB / 7.651GiB   0.10%
ldl-airflow-dag-processor   102.8MiB / 7.651GiB   0.87%
ldl-airflow-scheduler       445.4MiB / 7.651GiB   92.69%
ldl-airflow-postgres        38.19MiB / 7.651GiB   1.07%
ldl-docker-proxy            9.043MiB / 7.651GiB   0.00%
ldl-spark                   2.664MiB / 7.651GiB   0.00%
ldl-trino                   842.5MiB / 7.651GiB   2.14%
ldl-graph                   218.8MiB / 1.5GiB     0.00%
ldl-lakehouse-init          2.105MiB / 7.651GiB   0.95%
ldl-lakekeeper              45.84MiB / 7.651GiB   2.60%
ldl-objectstore             541.2MiB / 7.651GiB   1.72%
ldl-postgres                87.25MiB / 7.651GiB   1.74%
```

## Images

```text
REPOSITORY:TAG                                                              SIZE
ldl-graph:local                                                             629MB
ldl-spark:4.1.3-iceberg1.12.0                                               1.42GB
ldl-airflow:3.3.2                                                           2.44GB
ldl-spark:4.1.3-iceberg1.11.0                                               1.42GB
quay.io/lakekeeper/catalog:v0.13.6                                          166MB
postgres:18.6-alpine                                                        298MB
rustfs/rustfs:1.0.0                                                         247MB
curlimages/curl:8.22.0                                                      25.8MB
trinodb/trino:483                                                           1.38GB
```
