#!/usr/bin/env python3
"""Validate the churn gold export against the Retention Radar consumer contract.

Checks data/export/churn_user_features.csv (train) and
data/export/santosh_inference_record.json (serve) — the files retention-radar
syncs into data/external/. Structural problems (columns, nulls, plan tiers,
label leakage, missing Santosh) always fail. Schema range breaches are warnings,
matching retention-radar's soft range validation; ``--strict`` makes them fail.

Usage:
  python scripts/check_churn_export.py [--strict]
  CHURN_EXPORT_DIR=/tmp/export python scripts/check_churn_export.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EXPORT = Path(os.environ.get("CHURN_EXPORT_DIR", ROOT / "data/export"))

# Order matters: retention-radar FEATURE_COLUMNS + identity + label.
TRAIN_COLUMNS = [
    "user_id", "user_name", "days_since_signup", "sessions_last_7d", "sessions_last_30d",
    "avg_session_minutes", "models_used_count", "api_calls_last_30d", "tokens_consumed_last_30d",
    "tools_used_count", "failed_requests_rate", "support_tickets_last_90d", "plan_tier",
    "payment_failures_last_90d", "feature_adoption_score", "nps_score", "last_active_days_ago",
    "weekend_usage_ratio", "engagement_trend", "spend_usd_last_30d", "days_until_renewal",
    "agent_runs_last_30d", "ide_plugin_sessions_last_30d", "seat_utilization", "churned",
]
PLAN_TIERS = {"free", "starter", "pro", "enterprise"}
# (column, min, max) — mirrors retention-radar configs/schemas/user_record.schema.json
RANGES = [
    ("days_since_signup", 1, 2000),
    ("sessions_last_7d", 0, 60),
    ("sessions_last_30d", 0, 200),
    ("avg_session_minutes", 0.5, 240),
    ("models_used_count", 0, 30),
    ("api_calls_last_30d", 0, 100_000),
    ("tokens_consumed_last_30d", 0, 50_000_000),
    ("tools_used_count", 0, 40),
    ("failed_requests_rate", 0, 1),
    ("support_tickets_last_90d", 0, 50),
    ("payment_failures_last_90d", 0, 20),
    ("feature_adoption_score", 0, 1),
    ("nps_score", 0, 10),
    ("last_active_days_ago", 0, 365),
    ("weekend_usage_ratio", 0, 1),
    ("engagement_trend", 0, 5),
    ("spend_usd_last_30d", 0, 100_000),
    ("days_until_renewal", 0, 730),
    ("agent_runs_last_30d", 0, 5000),
    ("ide_plugin_sessions_last_30d", 0, 500),
    ("seat_utilization", 0, 1),
]
LEAKY = {"city", "feature_as_of", "built_at"}


def check(export_dir: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    train_path = export_dir / "churn_user_features.csv"
    serve_path = export_dir / "santosh_inference_record.json"
    for p in (train_path, serve_path):
        if not p.exists():
            errors.append(f"missing {p}")
    if errors:
        return errors, warnings

    df = pd.read_csv(train_path)
    if list(df.columns) != TRAIN_COLUMNS:
        errors.append(f"train columns {list(df.columns)} != contract {TRAIN_COLUMNS}")
    present = [c for c in TRAIN_COLUMNS if c in df.columns]
    nulls = df[present].isna().sum()
    if nulls.any():
        errors.append(f"nulls in export: {nulls[nulls > 0].to_dict()}")
    if "user_id" in df.columns and df["user_id"].duplicated().any():
        errors.append("duplicate user_id rows")
    if "plan_tier" in df.columns:
        bad = set(df["plan_tier"].unique()) - PLAN_TIERS
        if bad:
            errors.append(f"unknown plan_tier values: {sorted(bad)}")
    if "churned" in df.columns and not set(df["churned"].unique()) <= {0, 1}:
        errors.append("churned must be 0/1")
    for col, lo, hi in RANGES:
        if col not in df.columns:
            continue
        bad = df[(df[col] < lo) | (df[col] > hi)]
        if len(bad):
            ids = ", ".join(bad["user_id"].astype(str).head(5))
            warnings.append(f"{col} outside [{lo}, {hi}] for {len(bad)} user(s): {ids}")
    if "user_name" in df.columns and not (df["user_name"] == "Santosh Shinde").any():
        errors.append("Santosh Shinde missing from train export")

    record = json.loads(serve_path.read_text(encoding="utf-8"))
    expected = [c for c in TRAIN_COLUMNS if c != "churned"]
    if sorted(record) != sorted(expected):
        errors.append(
            "serve JSON keys differ: "
            f"missing={sorted(set(expected) - set(record))} extra={sorted(set(record) - set(expected))}"
        )
    if "churned" in record or LEAKY & set(record):
        errors.append("serve JSON leaks label / lake metadata")
    if record.get("user_name") != "Santosh Shinde":
        errors.append("serve JSON is not Santosh Shinde")
    return errors, warnings


def main(argv: list[str] | None = None) -> int:
    strict = "--strict" in (sys.argv[1:] if argv is None else argv)
    errors, warnings = check(EXPORT)
    for w in warnings:
        print(f"WARN (schema range): {w}", file=sys.stderr)
    if strict and warnings:
        errors = errors + ["range warnings are fatal with --strict"]
    if errors:
        print("Churn export contract FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    n = len(pd.read_csv(EXPORT / "churn_user_features.csv"))
    print(f"Churn export contract OK ({n} users, {len(TRAIN_COLUMNS)} cols) → {EXPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
