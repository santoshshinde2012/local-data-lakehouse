# Renewal features excerpt (no-Docker path, verified 2026-09-30)

```text
$ make churn-sample
Wrote bronze to data/sample/churn
  subscriptions=8001 usage_rows=176217 limit_events=10602 invoices=50748 tickets=2134

$ make churn-gold-local
Wrote data/export/churn_renewals_audit.csv (8001 renewals; routes {'model': 7387, 'dunning': 326, 'cancel_flow': 287, 'score_today': 1})
Wrote data/export/churn_user_features.csv (7387 rows, voluntary-lapse rate 0.074)
Wrote data/export/hero_inference_record.json (sub_maya, as of 2026-09-30)
Churn export contract OK (7387 renewals, 25 cols)

$ make churn-parity
Gold parity OK: 8001 renewals × 27 columns match (Spark SQL vs pandas)
```

The Spark jobs (`src/jobs/churn/01` … `04`) run the same SQL on Iceberg inside the
Compose stack (`make churn-e2e`). The parity check runs that SQL in local Spark; the
Docker run itself was not repeated for this excerpt.
