"""Publish gold.churn_user_features user-level feature contract for churn modeling."""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

AS_OF = os.environ.get("CHURN_AS_OF", "2024-03-02")


def main() -> None:
    spark = SparkSession.builder.appName("08_publish_churn_gold_features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    as_of = F.lit(AS_OF).cast("date")
    users = spark.table("lakehouse.silver.churn_users")
    usage = spark.table("lakehouse.silver.churn_usage_daily")
    tickets = spark.table("lakehouse.silver.churn_tickets")
    payments = spark.table("lakehouse.silver.churn_payments")

    u7 = usage.filter(
        (F.col("event_date") > F.date_sub(as_of, 7)) & (F.col("event_date") <= as_of)
    )
    u30 = usage.filter(
        (F.col("event_date") > F.date_sub(as_of, 30)) & (F.col("event_date") <= as_of)
    )

    agg7 = u7.groupBy("user_id").agg(F.sum("sessions").alias("sessions_last_7d"))
    agg30 = u30.groupBy("user_id").agg(
        F.sum("sessions").alias("sessions_last_30d"),
        F.sum("session_minutes").alias("minutes_30d"),
        F.sum("api_calls").alias("api_calls_last_30d"),
        F.sum("tokens").alias("tokens_consumed_last_30d"),
        F.max("tools_used").alias("tools_used_count"),
        F.sum("failed_requests").alias("failed_sum"),
        F.sum(F.col("failed_requests") + F.col("successful_requests")).alias("req_sum"),
        F.sum("agent_runs").alias("agent_runs_last_30d"),
        F.sum("ide_plugin_sessions").alias("ide_plugin_sessions_last_30d"),
        F.sum(F.when(F.col("is_weekend") == 1, F.col("sessions")).otherwise(0)).alias(
            "weekend_sessions"
        ),
        F.sum("spend_usd").alias("spend_usd_last_30d"),
        F.max("event_date").alias("last_event_date"),
        F.countDistinct("event_date").alias("active_days_30d"),
    )

    # models_used_count: proxy from tools + plan richness
    t90 = tickets.filter(
        (F.col("created_date") > F.date_sub(as_of, 90)) & (F.col("created_date") <= as_of)
    ).groupBy("user_id").agg(F.count("*").alias("support_tickets_last_90d"))

    p90 = (
        payments.filter(
            (F.col("payment_date") > F.date_sub(as_of, 90))
            & (F.col("payment_date") <= as_of)
            & (F.col("status") == "failed")
        )
        .groupBy("user_id")
        .agg(F.count("*").alias("payment_failures_last_90d"))
    )

    features = (
        users.alias("u")
        .join(agg7, "user_id", "left")
        .join(agg30, "user_id", "left")
        .join(t90, "user_id", "left")
        .join(p90, "user_id", "left")
        .fillna(
            {
                "sessions_last_7d": 0,
                "sessions_last_30d": 0,
                "minutes_30d": 0.0,
                "api_calls_last_30d": 0,
                "tokens_consumed_last_30d": 0,
                "tools_used_count": 0,
                "failed_sum": 0,
                "req_sum": 0,
                "agent_runs_last_30d": 0,
                "ide_plugin_sessions_last_30d": 0,
                "weekend_sessions": 0,
                "spend_usd_last_30d": 0.0,
                "support_tickets_last_90d": 0,
                "payment_failures_last_90d": 0,
                "active_days_30d": 0,
            }
        )
        .withColumn(
            "avg_session_minutes",
            F.when(
                F.col("sessions_last_30d") > 0,
                F.col("minutes_30d") / F.col("sessions_last_30d"),
            ).otherwise(F.lit(0.5)),
        )
        .withColumn(
            "failed_requests_rate",
            F.when(F.col("req_sum") > 0, F.col("failed_sum") / F.col("req_sum")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "weekend_usage_ratio",
            F.when(
                F.col("sessions_last_30d") > 0,
                F.col("weekend_sessions") / F.col("sessions_last_30d"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "engagement_trend",
            F.least(
                F.lit(5.0),
                F.col("sessions_last_7d")
                / F.greatest(F.lit(1.0), F.col("sessions_last_30d") / F.lit(4.0)),
            ),
        )
        .withColumn(
            "last_active_days_ago",
            F.when(
                F.col("last_event_date").isNotNull(),
                F.datediff(as_of, F.col("last_event_date")),
            ).otherwise(F.lit(365)),
        )
        .withColumn("days_since_signup", F.datediff(as_of, F.col("signup_date")))
        .withColumn(
            "days_until_renewal",
            F.lit(30)
            - (F.datediff(as_of, F.col("signup_date")) % F.lit(30)),
        )
        .withColumn(
            "models_used_count",
            F.least(F.lit(30), F.col("tools_used_count") + F.lit(1)),
        )
        .withColumn(
            "feature_adoption_score",
            F.least(
                F.lit(1.0),
                (
                    F.col("tools_used_count") / F.lit(15.0)
                    + F.col("active_days_30d") / F.lit(30.0)
                    + F.col("agent_runs_last_30d") / F.lit(50.0)
                )
                / F.lit(3.0),
            ),
        )
        .withColumn(
            "seat_utilization",
            F.when(F.col("plan_tier") == "enterprise", F.lit(0.75))
            .when(F.col("plan_tier") == "pro", F.lit(0.55))
            .when(F.col("plan_tier") == "starter", F.lit(0.35))
            .otherwise(F.lit(0.15)),
        )
        .withColumn("feature_as_of", as_of)
        .withColumn("built_at", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
        .select(
            "user_id",
            "user_name",
            "days_since_signup",
            "sessions_last_7d",
            "sessions_last_30d",
            "avg_session_minutes",
            "models_used_count",
            "api_calls_last_30d",
            "tokens_consumed_last_30d",
            "tools_used_count",
            "failed_requests_rate",
            "support_tickets_last_90d",
            "plan_tier",
            "payment_failures_last_90d",
            "feature_adoption_score",
            "nps_score",
            "last_active_days_ago",
            "weekend_usage_ratio",
            "engagement_trend",
            "spend_usd_last_30d",
            "days_until_renewal",
            "agent_runs_last_30d",
            "ide_plugin_sessions_last_30d",
            "seat_utilization",
            "churned",
            "city",
            "feature_as_of",
            "built_at",
        )
    )

    features.writeTo("lakehouse.gold.churn_user_features").createOrReplace()

    print("=== gold.churn_user_features (Santosh + sample) ===")
    spark.sql(
        """
        SELECT user_id, user_name, plan_tier, sessions_last_7d, sessions_last_30d,
               ROUND(engagement_trend, 3) AS engagement_trend,
               ROUND(failed_requests_rate, 3) AS fail_rate, churned
        FROM lakehouse.gold.churn_user_features
        ORDER BY user_id
        """
    ).show(truncate=False)
    print("Churn gold features OK.")
    spark.stop()


if __name__ == "__main__":
    main()
