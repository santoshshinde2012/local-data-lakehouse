# Troubleshooting

Back to the [README](../README.md). Every fix below touches only this project; `make reset` and `make purge` delete only this project's volumes.

| Symptom | Fix |
|---|---|
| Cannot connect to Docker | Start Docker Desktop; retry |
| Port already allocated | Free `8181` / `9000` / `9001` / `4040` / `8088` / `8080`, or change the `*_PORT` values in `.env` (keep `S3_ENDPOINT`'s port equal to `S3_API_PORT`) |
| Host engines: `Could not resolve host objectstore.localhost` | Add `127.0.0.1 objectstore.localhost` to `/etc/hosts` |
| A Spark job fails after the laptop slept: S3 `400 Bad Request`, `Failed to refresh storage credentials … Invalid credentials endpoint: null` | The vended credentials expired (Lakekeeper advertises no refresh endpoint). Re-run the job (`make e2e`, `make churn-e2e`, or the one `./pipelines/run_job.sh <job>`) |
| DuckDB: `Metadata-log exists but none of the entries were valid for the current transaction start time` | The connection was attached before another engine replaced the table: open a fresh connection (`lakehouse_client.duckdb_connect()`) |
| DuckDB `UPDATE` / `DELETE` / `MERGE` on an Iceberg table fails | DuckDB writes only merge-on-read tables; it fails on copy-on-write or sorted tables. Make the change in Spark or Trino, or set the table's `write.update.mode` / `write.delete.mode` / `write.merge.mode` to `merge-on-read` |
| Polars hangs or calls `169.254.169.254` | Read with `reader_override="pyiceberg"` (`lakehouse_client.polars_scan` does) |
| `NoSuchBucket` or `warehouse … not found` | `./pipelines/create_bucket.sh` (re-runs `lakehouse-init --once`) |
| Odd counts or a catalog that does not match the objects (for example after switching `STORE`) | `make reset` (this project's volumes only) |
| Linux: Spark cannot write `data/export` | `chmod a+rwx data/export` (Spark runs as uid 185) |
| Linux: Airflow cannot read or write its bind mounts | `make airflow-up` writes `AIRFLOW_UID=$(id -u)` to `.env` on Linux; re-run it |
| `up --wait` fails with `required variable … is missing` | `make env` (or add the key from `.env.example` to your `.env`) |
| Old volumes from the JDBC-catalog era | Not readable by this stack (Postgres 18 refuses a 16 data directory): `make purge` ([migration.md](migration.md)) |
| `make airflow-up` fails with `Bind for 127.0.0.1:8080 failed: port is already allocated` | Another app or container holds 8080. Set `AIRFLOW_API_PORT` (for example `8085`) in `.env` or the shell; `make airflow-wait` and the UI follow it |
| `make graph-e2e`: `Lineage build FAILED: README.md: cannot be read` | `ldl-graph` bind-mounts `README.md` and `Makefile` as single files, and an editor that replaces the file leaves a running container on the old copy. `make graph-e2e` now recreates `ldl-graph` each time; on an older checkout run `docker compose -f docker-compose.yml -f docker-compose.graph.yml --profile full up -d --force-recreate graph` |
| Airflow never healthy | `docker compose -f docker-compose.yml -f docker-compose.airflow.yml --profile full logs airflow-apiserver` |
