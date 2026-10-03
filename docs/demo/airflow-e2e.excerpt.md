# Airflow 3.3.2 overlay: the three DAGs

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `2fcb92f`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices, blank lines); long outputs keep their head and tail. `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

![Airflow DAG list: three DAGs, latest runs successful](img/airflow-dags.png)

## `make airflow-up`

Exit 0, 31.3 s. The first `make airflow-up` of this run failed after 21.3 s: host port 127.0.0.1:8080 was already taken by a container of another project (`Bind for 127.0.0.1:8080 failed: port is already allocated`). The overlay was removed with `make airflow-down` (1.7 s) and started again with `AIRFLOW_API_PORT=8085`; that start is shown. The Airflow images were already built and its database initialised by the first attempt.

```text
$ make airflow-up
./pipelines/airflow_env.sh
==> Airflow UI user: admin (password: _AIRFLOW_WWW_USER_PASSWORD in .env)
docker compose -f docker-compose.yml  -f docker-compose.airflow.yml --profile full up -d --build --wait
```

## DAGs parsed on Airflow 3

Exit 0, 2.7 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list
dag_id                     | fileloc                                         | owners  | is_paused | bundle_name | bundle_version
===========================+=================================================+=========+===========+=============+===============
lakehouse_churn_features   | /opt/airflow/dags/lakehouse_churn_features.py   | airflow | True      | dags-folder | None          
lakehouse_graph            | /opt/airflow/dags/lakehouse_graph.py            | airflow | True      | dags-folder | None          
lakehouse_retail_medallion | /opt/airflow/dags/lakehouse_retail_medallion.py | airflow | True      | dags-folder | None          
```

Exit 0, 2.2 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-import-errors
No data found
```

## `make airflow-demo` (retail, then churn)

Exit 0, 270.3 s.

```text
$ make airflow-demo
./pipelines/airflow_wait.sh
==> Waiting for the Airflow API server…
Airflow healthy: http://localhost:8085
./pipelines/airflow_trigger.sh lakehouse_retail_medallion
==> Waiting for lakehouse_retail_medallion to be parsed…
==> Triggering lakehouse_retail_medallion (manual__ldl_20261003T084930)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_retail_medallion succeeded
./pipelines/airflow_trigger.sh lakehouse_churn_features
==> Waiting for lakehouse_churn_features to be parsed…
==> Triggering lakehouse_churn_features (manual__ldl_20261003T085144)
  [5s] state=running
  [35s] state=running
  [65s] state=running
==> lakehouse_churn_features succeeded
==> Airflow demo complete.
    Airflow UI: http://localhost:8085  (user and generated password in .env)
    Exports:    data/export/
```

## `lakehouse_graph` (after the churn DAG)

Exit 0, 240.3 s.

```text
$ ./pipelines/airflow_trigger.sh lakehouse_graph
==> Waiting for lakehouse_graph to be parsed…
==> Triggering lakehouse_graph (manual__ldl_20261003T085401)
  [5s] state=running
  [35s] state=running
  [65s] state=running
  [95s] state=running
==> lakehouse_graph succeeded
```

## `lakehouse_retail_medallion`: runs

Exit 0, 2.9 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_retail_medallion -o plain
dag_id                      run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_retail_medallion  manual__ldl_20261003T084930  success  2026-10-03T08:49:32.357703+00:00                  2026-10-03T08:49:32.812494+00:00  2026-10-03T08:51:33.625018+00:00
```

## `lakehouse_churn_features`: runs

Exit 0, 2 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_churn_features -o plain
dag_id                    run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_churn_features  manual__ldl_20261003T085144  success  2026-10-03T08:51:46.727137+00:00                  2026-10-03T08:51:46.871179+00:00  2026-10-03T08:53:48.713769+00:00
```

## `lakehouse_graph`: runs

Exit 0, 2.4 s.

```text
$ docker exec ldl-airflow-scheduler airflow dags list-runs lakehouse_graph -o plain
dag_id           run_id                       state    run_after                         logical_date    start_date                        end_date
lakehouse_graph  manual__ldl_20261003T085401  success  2026-10-03T08:54:03.314284+00:00                  2026-10-03T08:54:03.842886+00:00  2026-10-03T08:57:49.174407+00:00
```

## `lakehouse_churn_features`: task states of the latest run

Exit 0, 2 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_churn_features manual__ldl_20261003T085144 -o plain
dag_id                    logical_date    task_id          state    start_date                        end_date
lakehouse_churn_features                  bronze           success  2026-10-03T08:51:47.076428+00:00  2026-10-03T08:52:16.179136+00:00
lakehouse_churn_features                  silver           success  2026-10-03T08:52:17.115576+00:00  2026-10-03T08:52:53.745243+00:00
lakehouse_churn_features                  gold_features    success  2026-10-03T08:52:54.299739+00:00  2026-10-03T08:53:24.209827+00:00
lakehouse_churn_features                  export_features  success  2026-10-03T08:53:24.431738+00:00  2026-10-03T08:53:48.169526+00:00
```

## `lakehouse_graph`: task states of the latest run

Exit 0, 1.9 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_graph manual__ldl_20261003T085401 -o plain
dag_id           logical_date    task_id                 state    start_date                        end_date
lakehouse_graph                  check_graph_contract    success  2026-10-03T08:56:08.920513+00:00  2026-10-03T08:57:00.114340+00:00
lakehouse_graph                  build_lineage           success  2026-10-03T08:57:01.526114+00:00  2026-10-03T08:57:27.429186+00:00
lakehouse_graph                  check_lineage_contract  success  2026-10-03T08:57:28.379061+00:00  2026-10-03T08:57:32.474258+00:00
lakehouse_graph                  build_cohorts           success  2026-10-03T08:57:33.694546+00:00  2026-10-03T08:57:42.706227+00:00
lakehouse_graph                  promote                 success  2026-10-03T08:57:45.258689+00:00  2026-10-03T08:57:48.130250+00:00
lakehouse_graph                  publish_gold_graph      success  2026-10-03T08:54:04.011960+00:00  2026-10-03T08:55:31.216060+00:00
lakehouse_graph                  build_graph             success  2026-10-03T08:55:32.907330+00:00  2026-10-03T08:56:07.858365+00:00
```

## `lakehouse_retail_medallion`: task states of the latest run

Exit 0, 1.9 s.

```text
$ docker exec ldl-airflow-scheduler airflow tasks states-for-dag-run lakehouse_retail_medallion manual__ldl_20261003T084930 -o plain
dag_id                      logical_date    task_id           state    start_date                        end_date
lakehouse_retail_medallion                  land_smoke        success  2026-10-03T08:49:33.018837+00:00  2026-10-03T08:49:57.907152+00:00
lakehouse_retail_medallion                  bronze            success  2026-10-03T08:49:58.627464+00:00  2026-10-03T08:50:20.775006+00:00
lakehouse_retail_medallion                  silver            success  2026-10-03T08:50:21.700242+00:00  2026-10-03T08:50:47.339018+00:00
lakehouse_retail_medallion                  gold              success  2026-10-03T08:50:47.791640+00:00  2026-10-03T08:51:10.674476+00:00
lakehouse_retail_medallion                  query_timetravel  success  2026-10-03T08:51:11.367811+00:00  2026-10-03T08:51:32.988885+00:00
```
