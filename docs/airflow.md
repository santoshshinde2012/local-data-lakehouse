# Orchestration with Apache Airflow 3

Back to the [README](../README.md). Run it with `make up-full`, then `make airflow-up` and `make airflow-demo`; `make airflow-down` removes only the Airflow services.

Airflow **schedules** the same Spark jobs; it does not replace the catalog, the store or Spark.

| DAG | Chain | Spark jobs |
|---|---|---|
| `lakehouse_retail_medallion` | `land_smoke >> bronze >> silver >> gold >> query_timetravel` | `src/jobs/retail/01` … `05` |
| `lakehouse_churn_features` | `bronze >> silver >> gold_features >> export_features` | `src/jobs/churn/01` … `04` |
| `lakehouse_graph` | `publish >> build >> contract >> lineage >> lineage_check >> cohorts >> promote` | `src/jobs/graph/01` + `ldl-graph` (needs the graph overlay) |

Each task is a `BashOperator` that runs `docker exec ldl-spark spark-submit …` (or `docker exec
ldl-graph …`). Airflow never sees the Docker socket: it talks to `docker-proxy`
(`wollomatic/socket-proxy`), which accepts connections only from the scheduler and only allows
`docker exec` into `ldl-spark` / `ldl-graph` (verified: `docker ps`, `docker run` and exec into other
containers are denied). Airflow runs non-root with the simple auth manager; DAGs are paused at
creation, example DAGs and config exposure are off. Excerpt:
[demo/airflow-e2e.excerpt.md](demo/airflow-e2e.excerpt.md).

```text
airflow/
  dags/           # lakehouse_retail_medallion, lakehouse_churn_features, lakehouse_graph (+ operators)
  auth/           # passwords.json for the simple auth manager (generated, gitignored)
  logs/ plugins/
docker/airflow/   # Airflow 3.3.2 image + static Docker CLI
docker-compose.airflow.yml
```

Run the three DAGs end to end:

```bash
make up-full
make graph-e2e                                   # the graph overlay (ldl-graph) that lakehouse_graph needs
make airflow-up                                  # generates Fernet / JWT / API secrets and the UI password into .env
make airflow-demo                                # lakehouse_retail_medallion, then lakehouse_churn_features
./pipelines/airflow_trigger.sh lakehouse_graph   # publish, build, contract, lineage, cohorts, promote
```

UI: http://localhost:8080 (127.0.0.1 only; `AIRFLOW_API_PORT` changes the port). User `admin` (`_AIRFLOW_WWW_USER_USERNAME`), password `_AIRFLOW_WWW_USER_PASSWORD` in `.env`.
