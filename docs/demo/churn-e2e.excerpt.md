# Churn E2E: sample, Spark renewal gold and the export

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `4bc3af8`, in one run on the existing volumes (no `make purge`; `make up-full` 17.6 s first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## `make churn-sample`

Exit 0, 2 s.

```text
$ make churn-sample
N_USERS=${N_USERS:-8000} CHURN_SEED=${CHURN_SEED:-42} .venv/bin/python scripts/generate_churn_sample.py
Wrote bronze to <repo>/data/sample/churn
  subscriptions=8001 usage_rows=176217 limit_events=10602 invoices=50748 tickets=2134
  cohort lapse rate=0.145 (voluntary 0.104, involuntary 0.041)
```

## `make churn-e2e` (Spark bronze → silver → gold → export)

Exit 0, 85.5 s.

```text
$ make churn-e2e
./pipelines/wait_for_stack.sh
==> Waiting for: postgres lakekeeper objectstore lakehouse-init spark
Stack ready (postgres=healthy lakekeeper=healthy objectstore=healthy lakehouse-init=healthy spark=healthy )
./pipelines/run_churn_e2e.sh
==> Churn features E2E (2026-10-03T05:20Z)
======== churn/01_ingest_bronze.py ========
==> spark-submit /opt/jobs/churn/01_ingest_bronze.py
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
==> spark-submit /opt/jobs/churn/02_transform_silver.py
silver.churn_subscription_snapshots: 8001 rows
silver.churn_usage_daily: 176217 rows
silver.churn_invoices: 50748 rows
silver.churn_subscription_events: 1996 rows
silver.churn_limit_events: 10602 rows
silver.churn_overage_settings: 849 rows
silver.churn_overage_charges: 470 rows
silver.churn_incidents: 3 rows
silver.churn_support_tickets: 2134 rows
silver.churn_pricing_changes: 2 rows
Churn silver OK.
======== churn/03_publish_gold_features.py ========
==> spark-submit /opt/jobs/churn/03_publish_gold_features.py
=== gold.churn_renewal_features: routes ===
+-----------+-----------------+--------+----------+
|route      |outcome          |renewals|lapse_rate|
+-----------+-----------------+--------+----------+
|cancel_flow|voluntary_lapse  |287     |1.0       |
|dunning    |involuntary_lapse|326     |0.0       |
|model      |renewed          |6839    |0.0       |
|model      |voluntary_lapse  |548     |1.0       |
|score_today|pending          |1       |0.0       |
+-----------+-----------------+--------+----------+
Churn gold features OK.
======== churn/04_export_features.py ========
==> spark-submit /opt/jobs/churn/04_export_features.py
Wrote /opt/data/export/churn_user_features.csv (7387 renewals routed to the model)
Wrote /opt/data/export/churn_renewals_audit.csv (8001 renewals)
Wrote /opt/data/export/hero_inference_record.json (sub_santosh)
==> Exports:
-rw-r--r--@ 1 santosh  staff  1418794 Oct  3 10:51 churn_renewals_audit.csv
-rw-r--r--@ 1 santosh  staff   831028 Oct  3 10:51 churn_user_features.csv
-rw-r--r--@ 1 santosh  staff      729 Oct  3 10:51 hero_inference_record.json
==> Churn E2E complete.
```

## `make churn-gold-local` (the no-Docker pandas twin; run before the strict default graph contract)

Exit 0, 3.3 s.

```text
$ make churn-gold-local
N_USERS=${N_USERS:-8000} CHURN_SEED=${CHURN_SEED:-42} .venv/bin/python scripts/generate_churn_sample.py
Wrote bronze to <repo>/data/sample/churn
  subscriptions=8001 usage_rows=176217 limit_events=10602 invoices=50748 tickets=2134
  cohort lapse rate=0.145 (voluntary 0.104, involuntary 0.041)
.venv/bin/python scripts/build_churn_gold_local.py
Wrote <repo>/data/export/churn_renewals_audit.csv (8001 renewals; routes {'model': 7387, 'dunning': 326, 'cancel_flow': 287, 'score_today': 1})
Wrote <repo>/data/export/churn_user_features.csv (7387 rows, voluntary-lapse rate 0.074)
Wrote <repo>/data/export/hero_inference_record.json (sub_santosh, as of 2026-09-30)
.venv/bin/python scripts/check_churn_export.py
Churn export contract OK (7387 renewals, 25 cols) → <repo>/data/export
```
