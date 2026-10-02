# Airflow 3.3.2 overlay: the three DAGs

Captured on 2026-10-02 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `feat/local-first-stack-2026` at `7f5fc43`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

![Airflow DAG list: three DAGs, latest runs successful](img/airflow-dags.png)

## `make airflow-up`

Exit 0, 30.3 s.

```text
$ make airflow-up
./pipelines/airflow_env.sh
==> Airflow UI user: admin (password: _AIRFLOW_WWW_USER_PASSWORD in .env)
docker compose -f docker-compose.yml  -f docker-compose.airflow.yml --profile full up -d --build --wait
```

## DAGs parsed on Airflow 3

Exit 0, 2.5 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list
dag_id                     | fileloc                                         | owners  | is_paused | bundle_name | bundle_version
===========================+=================================================+=========+===========+=============+===============
lakehouse_churn_features   | /opt/airflow/dags/lakehouse_churn_features.py   | airflow | False     | dags-folder | None          
lakehouse_graph            | /opt/airflow/dags/lakehouse_graph.py            | airflow | False     | dags-folder | None          
lakehouse_retail_medallion | /opt/airflow/dags/lakehouse_retail_medallion.py | airflow | False     | dags-folder | None          
```

Exit 0, 2.2 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-import-errors
No data found
```

## `make airflow-demo` (retail, then churn)

Exit 0, 214.8 s.

```text
$ make airflow-demo
./pipelines/airflow_wait.sh
==> Waiting for the Airflow API server…
Airflow healthy: http://localhost:8080
./pipelines/airflow_trigger.sh lakehouse_retail_medallion
==> Waiting for lakehouse_retail_medallion to be parsed…
==> Triggering lakehouse_retail_medallion (manual__ldl_20261002T154441)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_retail_medallion succeeded
./pipelines/airflow_trigger.sh lakehouse_churn_features
==> Waiting for lakehouse_churn_features to be parsed…
==> Triggering lakehouse_churn_features (manual__ldl_20261002T154632)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_churn_features succeeded
==> Airflow demo complete.
    Airflow UI: http://localhost:8080  (user and generated password in .env)
    Exports:    data/export/
```

## `lakehouse_graph` (after the churn DAG)

Exit 0, 132.3 s.

```text
$ ./pipelines/airflow_trigger.sh lakehouse_graph
==> Waiting for lakehouse_graph to be parsed…
==> Triggering lakehouse_graph (manual__ldl_20261002T154816)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_graph succeeded
```

## `lakehouse_retail_medallion`: run and task states

![lakehouse_retail_medallion run in the Airflow UI](img/airflow-lakehouse_retail_medallion.png)

Exit 0, 2.5 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_retail_medallion -o plain
dag_id                      run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_retail_medallion  manual__ldl_20261002T154441  success  2026-10-02T15:44:43.545784+00:00                  2026-10-02T15:44:43.735988+00:00  2026-10-02T15:46:26.981996+00:00
```

Exit 0, 1.9 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_retail_medallion manual__ldl_20261002T154441 -o plain
dag_id                      logical_date    task_id           state    start_date                        end_date
lakehouse_retail_medallion                  land_smoke        success  2026-10-02T15:44:43.920307+00:00  2026-10-02T15:45:04.860631+00:00
lakehouse_retail_medallion                  bronze            success  2026-10-02T15:45:05.575592+00:00  2026-10-02T15:45:25.619681+00:00
lakehouse_retail_medallion                  silver            success  2026-10-02T15:45:25.908837+00:00  2026-10-02T15:45:46.327353+00:00
lakehouse_retail_medallion                  gold              success  2026-10-02T15:45:47.243794+00:00  2026-10-02T15:46:06.944449+00:00
lakehouse_retail_medallion                  query_timetravel  success  2026-10-02T15:46:07.346973+00:00  2026-10-02T15:46:26.820949+00:00
```

## `lakehouse_churn_features`: run and task states

![lakehouse_churn_features run in the Airflow UI](img/airflow-lakehouse_churn_features.png)

Exit 0, 2.2 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_churn_features -o plain
dag_id                    run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_churn_features  manual__ldl_20261002T154632  success  2026-10-02T15:46:34.030261+00:00                  2026-10-02T15:46:34.167393+00:00  2026-10-02T15:48:05.513548+00:00
```

Exit 0, 2 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_churn_features manual__ldl_20261002T154632 -o plain
dag_id                    logical_date    task_id          state    start_date                        end_date
lakehouse_churn_features                  silver           success  2026-10-02T15:46:56.183749+00:00  2026-10-02T15:47:21.195028+00:00
lakehouse_churn_features                  gold_features    success  2026-10-02T15:47:21.379982+00:00  2026-10-02T15:47:45.086008+00:00
lakehouse_churn_features                  export_features  success  2026-10-02T15:47:45.630860+00:00  2026-10-02T15:48:04.752221+00:00
lakehouse_churn_features                  bronze           success  2026-10-02T15:46:34.325433+00:00  2026-10-02T15:46:55.893471+00:00
```

## `lakehouse_graph`: run and task states

![lakehouse_graph run in the Airflow UI](img/airflow-lakehouse_graph.png)

Exit 0, 2.1 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_graph -o plain
dag_id           run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_graph  manual__ldl_20261002T154816  success  2026-10-02T15:48:18.376173+00:00                  2026-10-02T15:48:18.814478+00:00  2026-10-02T15:50:17.486526+00:00
```

Exit 0, 1.8 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_graph manual__ldl_20261002T154816 -o plain
dag_id           logical_date    task_id                 state    start_date                        end_date
lakehouse_graph                  publish_gold_graph      success  2026-10-02T15:48:18.939208+00:00  2026-10-02T15:49:05.368937+00:00
lakehouse_graph                  build_graph             success  2026-10-02T15:49:06.516564+00:00  2026-10-02T15:49:29.413836+00:00
lakehouse_graph                  check_graph_contract    success  2026-10-02T15:49:30.098957+00:00  2026-10-02T15:49:56.874237+00:00
lakehouse_graph                  build_lineage           success  2026-10-02T15:49:58.024227+00:00  2026-10-02T15:50:04.875713+00:00
lakehouse_graph                  check_lineage_contract  success  2026-10-02T15:50:06.449210+00:00  2026-10-02T15:50:09.403663+00:00
lakehouse_graph                  build_cohorts           success  2026-10-02T15:50:09.830433+00:00  2026-10-02T15:50:14.615760+00:00
lakehouse_graph                  promote                 success  2026-10-02T15:50:15.530312+00:00  2026-10-02T15:50:16.444824+00:00
```
