#!/usr/bin/env python3
"""Validate the churn gold export against the Retention Radar T-7 renewal contract.

Checks data/export/churn_user_features.csv (train: one row per renewal routed to
the model, 24 fields + churned) and data/export/hero_inference_record.json (serve:
today's T-7 record, no label) — the files retention-radar syncs into
data/external/. Structural problems always fail: columns, nulls, plan tiers,
binary flags, active-day consistency, label or lake-metadata leakage, and rows
that belong to dunning or the cancel flow. Schema range breaches are warnings;
``--strict`` makes them fail.

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

# Order matters: retention-radar INFERENCE_REQUIRED_KEYS + label.
TRAIN_COLUMNS = [
    "user_id", "user_name", "plan_tier", "renewals_completed", "active_days_7d", "active_days_28d",
    "engagement_trend", "last_active_days_ago", "agent_requests_28d", "allowance_used_pct",
    "limit_hits_14d", "cheap_model_share_28d", "overage_usd_28d", "overage_toggled_off",
    "suggestion_accept_rate_28d", "accept_rate_change", "agent_task_success_rate",
    "failed_requests_rate", "incident_exposed_28d", "support_tickets_90d", "ide_sessions_28d",
    "cli_sessions_28d", "weekend_usage_ratio", "first_renewal_after_pricing_change", "churned",
]
PLAN_TIERS = {"pro", "pro_plus", "ultra"}
BINARY = ["overage_toggled_off", "incident_exposed_28d", "first_renewal_after_pricing_change"]
# (column, min, max): mirrors retention-radar configs/schemas/user_record.schema.json
RANGES = [
    ("renewals_completed", 0, 60), ("active_days_7d", 0, 7), ("active_days_28d", 0, 28),
    ("engagement_trend", 0, 4), ("last_active_days_ago", 0, 90), ("agent_requests_28d", 0, 50_000),
    ("allowance_used_pct", 0, 3), ("limit_hits_14d", 0, 60), ("cheap_model_share_28d", 0, 1),
    ("overage_usd_28d", 0, 5000), ("suggestion_accept_rate_28d", 0, 1), ("accept_rate_change", 0, 3),
    ("agent_task_success_rate", 0, 1), ("failed_requests_rate", 0, 1), ("support_tickets_90d", 0, 50),
    ("ide_sessions_28d", 0, 500), ("cli_sessions_28d", 0, 500), ("weekend_usage_ratio", 0, 1),
]
LEAKY = {"city", "feature_as_of", "built_at", "outcome", "route", "renewal_date", "cancel_at_period_end"}


def check(export_dir: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    train_path = export_dir / "churn_user_features.csv"
    serve_path = export_dir / "hero_inference_record.json"
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
    for col in BINARY:
        if col in df.columns and not set(df[col].unique()) <= {0, 1}:
            errors.append(f"{col} must be 0/1")
    if {"active_days_7d", "active_days_28d"} <= set(df.columns) and (df["active_days_7d"] > df["active_days_28d"]).any():
        errors.append("active_days_7d > active_days_28d")
    for col, lo, hi in RANGES:
        if col not in df.columns:
            continue
        bad = df[(df[col] < lo) | (df[col] > hi)]
        if len(bad):
            ids = ", ".join(bad["user_id"].astype(str).head(5))
            warnings.append(f"{col} outside [{lo}, {hi}] for {len(bad)} row(s): {ids}")

    audit = export_dir / "churn_renewals_audit.csv"
    if audit.exists():
        a = pd.read_csv(audit)
        routed_out = set(a.loc[a["route"].isin(["dunning", "cancel_flow"]), "user_id"])
        leaked = routed_out & set(df["user_id"])
        if leaked:
            errors.append(f"{len(leaked)} dunning / cancel-flow renewals leaked into the train export")

    record = json.loads(serve_path.read_text(encoding="utf-8"))
    expected = [c for c in TRAIN_COLUMNS if c != "churned"]
    if sorted(record) != sorted(expected):
        errors.append(
            "serve JSON keys differ: "
            f"missing={sorted(set(expected) - set(record))} extra={sorted(set(record) - set(expected))}"
        )
    if "churned" in record or LEAKY & set(record):
        errors.append("serve JSON leaks label / lake metadata")
    if record.get("user_id") in set(df.get("user_id", [])):
        errors.append("serve record also appears in the train export (its renewal has not happened)")
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
    print(f"Churn export contract OK ({n} renewals, {len(TRAIN_COLUMNS)} cols) → {EXPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
