#!/usr/bin/env python3
"""Spark-parity local gold builder (no Docker).

Reads bronze CSVs from data/sample/churn, applies the same window math as
src/jobs/churn/03_publish_gold_features.py, writes:
  data/export/churn_user_features.csv
  data/export/santosh_inference_record.json

Use when Spark/Docker is unavailable; prefer `make churn-e2e` on a laptop with Docker.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = Path(os.environ.get("CHURN_SAMPLE_DIR", ROOT / "data/sample/churn"))
EXPORT = Path(os.environ.get("CHURN_EXPORT_DIR", ROOT / "data/export"))
AS_OF = date.fromisoformat(os.environ.get("CHURN_AS_OF", "2024-03-02"))


def _clip_engagement(s: pd.Series) -> pd.Series:
    # Align with Retention Radar schema max=5
    return s.clip(lower=0, upper=5)


def main() -> None:
    users = pd.read_csv(SAMPLE / "users.csv", parse_dates=["signup_date"])
    usage = pd.read_csv(SAMPLE / "daily_usage_snapshots.csv", parse_dates=["event_date"])
    tickets = pd.read_csv(SAMPLE / "support_tickets.csv", parse_dates=["created_date"])
    payments = pd.read_csv(SAMPLE / "payments.csv", parse_dates=["payment_date"])

    users["plan_tier"] = users["plan_tier"].str.lower().str.strip()
    users = users.drop_duplicates("user_id", keep="last")
    users = users[users["plan_tier"].isin(["free", "starter", "pro", "enterprise"])]

    as_of = pd.Timestamp(AS_OF)
    u7 = usage[(usage["event_date"] > as_of - pd.Timedelta(days=7)) & (usage["event_date"] <= as_of)]
    u30 = usage[(usage["event_date"] > as_of - pd.Timedelta(days=30)) & (usage["event_date"] <= as_of)]

    agg7 = u7.groupby("user_id", as_index=False)["sessions"].sum().rename(columns={"sessions": "sessions_last_7d"})
    tmp = u30.copy()
    tmp["weekend_sess"] = np.where(tmp["is_weekend"] == 1, tmp["sessions"], 0)
    agg30 = tmp.groupby("user_id").agg(
        sessions_last_30d=("sessions", "sum"),
        minutes_30d=("session_minutes", "sum"),
        api_calls_last_30d=("api_calls", "sum"),
        tokens_consumed_last_30d=("tokens", "sum"),
        tools_used_count=("tools_used", "max"),
        failed_sum=("failed_requests", "sum"),
        success_sum=("successful_requests", "sum"),
        agent_runs_last_30d=("agent_runs", "sum"),
        ide_plugin_sessions_last_30d=("ide_plugin_sessions", "sum"),
        weekend_sessions=("weekend_sess", "sum"),
        spend_usd_last_30d=("spend_usd", "sum"),
        last_event_date=("event_date", "max"),
        active_days_30d=("event_date", "nunique"),
    ).reset_index()

    t90 = tickets[(tickets["created_date"] > as_of - pd.Timedelta(days=90)) & (tickets["created_date"] <= as_of)]
    t_agg = t90.groupby("user_id", as_index=False).size().rename(columns={"size": "support_tickets_last_90d"})

    p90 = payments[
        (payments["payment_date"] > as_of - pd.Timedelta(days=90))
        & (payments["payment_date"] <= as_of)
        & (payments["status"].str.lower().str.strip() == "failed")
    ]
    p_agg = p90.groupby("user_id", as_index=False).size().rename(columns={"size": "payment_failures_last_90d"})

    feat = (
        users.merge(agg7, on="user_id", how="left")
        .merge(agg30, on="user_id", how="left")
        .merge(t_agg, on="user_id", how="left")
        .merge(p_agg, on="user_id", how="left")
    )
    fill = {
        "sessions_last_7d": 0,
        "sessions_last_30d": 0,
        "minutes_30d": 0.0,
        "api_calls_last_30d": 0,
        "tokens_consumed_last_30d": 0,
        "tools_used_count": 0,
        "failed_sum": 0,
        "success_sum": 0,
        "agent_runs_last_30d": 0,
        "ide_plugin_sessions_last_30d": 0,
        "weekend_sessions": 0,
        "spend_usd_last_30d": 0.0,
        "support_tickets_last_90d": 0,
        "payment_failures_last_90d": 0,
        "active_days_30d": 0,
    }
    feat = feat.fillna(fill)
    req_sum = feat["failed_sum"] + feat["success_sum"]
    feat["avg_session_minutes"] = np.where(
        feat["sessions_last_30d"] > 0,
        feat["minutes_30d"] / feat["sessions_last_30d"],
        0.5,
    )
    feat["failed_requests_rate"] = np.where(req_sum > 0, feat["failed_sum"] / req_sum, 0.0)
    feat["weekend_usage_ratio"] = np.where(
        feat["sessions_last_30d"] > 0,
        feat["weekend_sessions"] / feat["sessions_last_30d"],
        0.0,
    )
    feat["engagement_trend"] = _clip_engagement(
        feat["sessions_last_7d"] / np.maximum(1.0, feat["sessions_last_30d"] / 4.0)
    )
    feat["last_active_days_ago"] = np.where(
        feat["last_event_date"].notna(),
        (as_of - pd.to_datetime(feat["last_event_date"])).dt.days,
        365,
    )
    feat["days_since_signup"] = (as_of - pd.to_datetime(feat["signup_date"])).dt.days
    feat["days_until_renewal"] = 30 - (feat["days_since_signup"] % 30)
    feat["models_used_count"] = np.minimum(30, feat["tools_used_count"] + 1)
    feat["feature_adoption_score"] = np.minimum(
        1.0,
        (
            feat["tools_used_count"] / 15.0
            + feat["active_days_30d"] / 30.0
            + feat["agent_runs_last_30d"] / 50.0
        )
        / 3.0,
    )
    seat_map = {"enterprise": 0.75, "pro": 0.55, "starter": 0.35, "free": 0.15}
    feat["seat_utilization"] = feat["plan_tier"].map(seat_map).fillna(0.15)
    feat["feature_as_of"] = AS_OF.isoformat()
    feat["built_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    cols = [
        "user_id", "user_name", "days_since_signup", "sessions_last_7d", "sessions_last_30d",
        "avg_session_minutes", "models_used_count", "api_calls_last_30d", "tokens_consumed_last_30d",
        "tools_used_count", "failed_requests_rate", "support_tickets_last_90d", "plan_tier",
        "payment_failures_last_90d", "feature_adoption_score", "nps_score", "last_active_days_ago",
        "weekend_usage_ratio", "engagement_trend", "spend_usd_last_30d", "days_until_renewal",
        "agent_runs_last_30d", "ide_plugin_sessions_last_30d", "seat_utilization", "churned",
        "city", "feature_as_of", "built_at",
    ]
    out = feat[cols].sort_values("user_id")

    EXPORT.mkdir(parents=True, exist_ok=True)
    train_cols = [c for c in cols if c not in ("city", "feature_as_of", "built_at")]
    train = out[train_cols]
    train_path = EXPORT / "churn_user_features.csv"
    train.to_csv(train_path, index=False)

    santosh = train[train["user_name"] == "Santosh Shinde"]
    if santosh.empty:
        raise SystemExit("Santosh Shinde missing from gold features")
    record = santosh.drop(columns=["churned"]).iloc[0].to_dict()
    # numpy types → python
    for k, v in list(record.items()):
        if hasattr(v, "item"):
            record[k] = v.item()
        elif hasattr(v, "isoformat"):
            record[k] = str(v)
    json_path = EXPORT / "santosh_inference_record.json"
    json_path.write_text(json.dumps(record, indent=2) + "\n")

    print(f"Wrote {train_path} ({len(train)} rows, churn={train['churned'].mean():.3f})")
    print(f"Wrote {json_path}")
    print("Santosh keys:", sorted(record.keys()))


if __name__ == "__main__":
    main()
