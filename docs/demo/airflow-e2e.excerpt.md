# Airflow E2E excerpt (verified 2026-10-02)

Airflow 3.3.2 overlay (api-server, scheduler, dag-processor, metadata Postgres 18.6, socket proxy) on
the full profile:

```bash
make up-full
make airflow-up        # generates the secrets into .env on first run; 65 s with the image already built
make airflow-demo
```

```text
./pipelines/airflow_wait.sh
==> Waiting for the Airflow API server…
Airflow healthy: http://localhost:8080
./pipelines/airflow_trigger.sh lakehouse_retail_medallion
==> Waiting for lakehouse_retail_medallion to be parsed…
==> Triggering lakehouse_retail_medallion (manual__ldl_20261002T074903)
  [5s] state=running
  [35s] state=running
  [65s] state=running
  [95s] state=success
==> lakehouse_retail_medallion succeeded
./pipelines/airflow_trigger.sh lakehouse_churn_features
==> Waiting for lakehouse_churn_features to be parsed…
==> Triggering lakehouse_churn_features (manual__ldl_20261002T075206)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_churn_features succeeded

==> Airflow demo complete.
    Airflow UI: http://localhost:8080  (user and generated password in .env)
    Exports:    data/export/
make airflow-demo  … 5:05.81 total
```

| DAG | Result |
|---|---|
| `lakehouse_retail_medallion` | success (`land_smoke >> bronze >> silver >> gold >> query_timetravel`) |
| `lakehouse_churn_features` | success (`bronze >> silver >> gold_features >> export_features`) |

`GET /api/v2/monitor/health` reported `metadatabase`, `scheduler` and `dag_processor` healthy (no
triggerer is run). Socket-proxy checks from inside the scheduler: `docker exec ldl-spark` allowed;
`docker ps`, `docker run` and `docker exec ldl-postgres` denied; the API server gets no access.
Memory after the demo: scheduler 648 MiB, API server 278 MiB, dag-processor 180 MiB, metadata
Postgres 50 MiB, proxy 9 MiB (about 1.16 GiB). The `lakehouse_graph` DAG was not triggered in this run.
