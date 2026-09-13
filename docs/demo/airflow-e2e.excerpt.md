# Airflow E2E excerpt

Verified on Apple Silicon after:

```bash
make up && make wait
make airflow-up && make airflow-wait
make airflow-demo
```

| DAG | Result |
|---|---|
| `lakehouse_retail_medallion` | success (`land_smoke >> bronze >> silver >> gold >> query_timetravel`) |
| `lakehouse_churn_features` | success (`bronze >> silver >> gold_features >> export_features`) |

Airflow UI: http://localhost:8080 (`admin` / `admin`)

Exports after churn DAG: `data/export/churn_user_features.csv`, `data/export/santosh_inference_record.json`
