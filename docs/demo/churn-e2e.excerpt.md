# Churn foundation E2E excerpt (verified)

```text
Jobs: 01_ingest_bronze → 02_transform_silver → 03_publish_gold_features → 04_export_features

bronze: users=10 usage=410 tickets=17 payments=27
gold.churn_user_features (as-of 2024-03-02):

u-01 Santosh Shinde  pro         sessions 12/57  engagement_trend 0.842  churned 0
u-02 Priya Sharma    starter     18/80   0.900  0
u-03 Jordan Miles    free         2/17   0.471  1
… (10 users total)

Exports:
  data/export/churn_user_features.csv
  data/export/santosh_inference_record.json  (no churned — serve-style)
```
