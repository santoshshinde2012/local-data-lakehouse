-- Gold churn features contract (Spark: src/jobs/churn/03_publish_gold_features.py)
-- Aligned with xgboost-ai-churn schemas/user_record.schema.json (+ churned for train)
--
-- Grain: one row per user_id as-of CHURN_AS_OF (default 2024-03-02)
-- Build: make churn-e2e  OR  make churn-gold-local (pandas parity, no Docker)

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

-- Logical columns written to lakehouse.gold.churn_user_features:
--   user_id, user_name,
--   days_since_signup, sessions_last_7d, sessions_last_30d, avg_session_minutes,
--   models_used_count, api_calls_last_30d, tokens_consumed_last_30d, tools_used_count,
--   failed_requests_rate, support_tickets_last_90d, plan_tier, payment_failures_last_90d,
--   feature_adoption_score, nps_score, last_active_days_ago, weekend_usage_ratio,
--   engagement_trend,          -- clipped to [0, 5] for RR schema
--   spend_usd_last_30d, days_until_renewal, agent_runs_last_30d,
--   ide_plugin_sessions_last_30d, seat_utilization,
--   churned,                   -- train only; stripped from santosh_inference_record.json
--   city, feature_as_of, built_at   -- lake metadata; stripped on ML export
--
-- Exports (job 04 / build_churn_gold_local.py):
--   data/export/churn_user_features.csv
--   data/export/santosh_inference_record.json
--
-- SELECT * FROM lakehouse.gold.churn_user_features;
