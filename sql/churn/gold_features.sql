-- Gold churn features — matches jobs/08_publish_churn_gold_features.py
-- Contract aligned with xgboost-ai-churn schemas/user_record.schema.json (+ churned for train)

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

-- Built in PySpark (windowed aggregates). This file documents the output grain.
-- SELECT * FROM lakehouse.gold.churn_user_features;
