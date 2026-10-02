# Renewal features excerpt (verified 2026-10-02)

## Spark path: `make churn-e2e` (full profile)

Seed 42, `N_USERS=8000` sample (`make churn-sample`). Total 1 min 49 s on a running stack.

```text
$ make churn-e2e
======== churn/01_ingest_bronze.py ========
bronze.churn_subscription_snapshots_raw: 8001 rows
bronze.churn_invoices_raw: 50748 rows
bronze.churn_subscription_events_raw: 1996 rows
bronze.churn_usage_raw: 176217 rows
bronze.churn_limit_events_raw: 10602 rows
bronze.churn_overage_settings_raw: 849 rows
bronze.churn_overage_charges_raw: 470 rows
bronze.churn_incidents_raw: 3 rows
bronze.churn_support_tickets_raw: 2134 rows
bronze.churn_pricing_changes_raw: 2 rows
Churn bronze ingest OK.
======== churn/02_transform_silver.py ========
silver.churn_subscription_snapshots: 8001 rows
silver.churn_usage_daily: 176217 rows
… (10 silver tables)
Churn silver OK.
======== churn/03_publish_gold_features.py ========
=== gold.churn_renewal_features: routes ===
|route      |outcome          |renewals|lapse_rate|
|cancel_flow|voluntary_lapse  |287     |1.0       |
|dunning    |involuntary_lapse|326     |0.0       |
|model      |renewed          |6839    |0.0       |
|model      |voluntary_lapse  |548     |1.0       |
|score_today|pending          |1       |0.0       |
Churn gold features OK.
======== churn/04_export_features.py ========
Wrote /opt/data/export/churn_user_features.csv (7387 renewals routed to the model)
Wrote /opt/data/export/churn_renewals_audit.csv (8001 renewals)
Wrote /opt/data/export/hero_inference_record.json (sub_maya)
==> Churn E2E complete.

$ .venv/bin/python scripts/check_churn_export.py --strict
Churn export contract OK (7387 renewals, 25 cols)
```

## No-Docker path and the Spark-vs-pandas check

```text
$ make churn-sample
Wrote bronze to data/sample/churn
  subscriptions=8001 usage_rows=176217 limit_events=10602 invoices=50748 tickets=2134

$ make churn-gold-local
Wrote data/export/churn_renewals_audit.csv (8001 renewals; routes {'model': 7387, 'dunning': 326, 'cancel_flow': 287, 'score_today': 1})
Wrote data/export/churn_user_features.csv (7387 rows, voluntary-lapse rate 0.074)
Wrote data/export/hero_inference_record.json (sub_maya, as of 2026-09-30)
Churn export contract OK (7387 renewals, 25 cols)

$ uv pip install --python .venv/bin/python pyspark==4.1.3   # once; Java 17 or 21
$ make churn-parity
Gold parity OK: 8001 renewals × 27 columns match (Spark SQL vs pandas)
```

The parity check took 20.8 s on the full sample and 23.6 s on the tiny fixture
(`CHURN_SAMPLE_DIR=data/sample/churn/fixtures/tiny`, 121 renewals) with pyspark 4.1.3 on Java 17.
