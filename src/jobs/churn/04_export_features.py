"""Export gold renewal features for retention-radar.

  churn_user_features.csv      rows routed to the model: 24-field contract + churned
  churn_renewals_audit.csv     every renewal with outcome, route and dates
  hero_inference_record.json   today's T-7 record for CHURN_HERO_ID (no label)
"""
from __future__ import annotations

import csv
import json
import os
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

EXPORT_DIR = os.environ.get("CHURN_EXPORT_DIR", "/opt/data/export")
HERO_ID = os.environ.get("CHURN_HERO_ID", "sub_santosh")
TRAIN_COLUMNS = [
    "user_id", "user_name", "plan_tier", "renewals_completed", "active_days_7d", "active_days_28d",
    "engagement_trend", "last_active_days_ago", "agent_requests_28d", "allowance_used_pct",
    "limit_hits_14d", "cheap_model_share_28d", "overage_usd_28d", "overage_toggled_off",
    "suggestion_accept_rate_28d", "accept_rate_change", "agent_task_success_rate",
    "failed_requests_rate", "incident_exposed_28d", "support_tickets_90d", "ide_sessions_28d",
    "cli_sessions_28d", "weekend_usage_ratio", "first_renewal_after_pricing_change", "churned",
]
AUDIT_EXTRA = ["outcome", "route", "feature_as_of", "renewal_date", "city", "built_at"]
EXPORTS = ("churn_renewals_audit.csv", "churn_user_features.csv", "hero_inference_record.json")


def _plain(v):
    if isinstance(v, Decimal):   # DECIMAL gold columns (Spark SQL decimal literals): JSON has no Decimal
        return float(v)
    if hasattr(v, "isoformat"):
        return v.isoformat() if not hasattr(v, "hour") else v.strftime("%Y-%m-%d %H:%M:%S")
    return v


def _staged(path: Path) -> Path:
    """Hidden sibling an export is written to first; main() moves all three into place together.

    The staged file is created in the export directory, so the directory itself (not only the
    previous exports) must be writable by the job's user. The three moves are separate renames:
    an OS error between two of them can still leave new and previous exports mixed (rerun the job).
    """
    return path.with_name(f".{path.name}.tmp")


def _write_csv(path: Path, rows: list[dict], cols: list[str]) -> None:
    with _staged(path).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: _plain(r[c]) for c in cols})


def _write_text(path: Path, text: str) -> None:
    _staged(path).write_text(text)


def main() -> None:
    spark = SparkSession.builder.appName("churn_04_export_features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    out = Path(EXPORT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    g = spark.table("lakehouse.gold.churn_renewal_features")
    audit = [r.asDict() for r in g.orderBy("user_id").collect()]
    if not audit:
        raise SystemExit("gold.churn_renewal_features is empty")
    try:
        _write_csv(out / "churn_renewals_audit.csv", audit, TRAIN_COLUMNS + AUDIT_EXTRA)
        train = [r for r in audit if r["route"] == "model"]
        _write_csv(out / "churn_user_features.csv", train, TRAIN_COLUMNS)

        hero = [r for r in audit if r["user_id"] == HERO_ID and r["route"] == "score_today"]
        if not hero:
            raise SystemExit(f"{HERO_ID} has no T-7 snapshot for today")
        record = {c: _plain(hero[0][c]) for c in TRAIN_COLUMNS if c != "churned"}
        _write_text(out / "hero_inference_record.json", json.dumps(record, indent=2) + "\n")
        for name in EXPORTS:   # all three are written: only now replace the previous exports
            os.replace(_staged(out / name), out / name)
    finally:
        for name in EXPORTS:   # a failed run leaves the previous exports and no staged file behind
            _staged(out / name).unlink(missing_ok=True)

    print(f"Wrote {out / 'churn_user_features.csv'} ({len(train)} renewals routed to the model)")
    print(f"Wrote {out / 'churn_renewals_audit.csv'} ({len(audit)} renewals)")
    print(f"Wrote {out / 'hero_inference_record.json'} ({HERO_ID})")
    spark.stop()


if __name__ == "__main__":
    main()
