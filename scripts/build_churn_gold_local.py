#!/usr/bin/env python3
"""Spark-parity local gold builder (no Docker): bronze events → T-7 renewal features.

Same logic as src/jobs/churn/02_transform_silver.py + sql/churn/gold_renewal_features.sql
(run by 03_publish_gold_features.py), in pandas; scripts/check_gold_parity.py checks
the two agree. For every subscription snapshot taken seven days before a renewal
(`snapshot_date = current_period_end - 7`):

  * features are computed from events on or before the snapshot date only
    (point-in-time: usage keeps flowing after T-7 in bronze, and is ignored);
  * the renewal outcome comes from billing events after the fact:
      paid invoice at T                         → renewed         (label 0, route model)
      cancel scheduled, canceled at T           → voluntary lapse (label 1)
          … scheduled on or before T-7          → route cancel_flow (already decided)
          … scheduled after T-7                 → route model
      invoice failed, retries exhausted         → involuntary     (route dunning)
      renewal not yet observable                → pending (scored, not trained on)

Writes:
  data/export/churn_user_features.csv   model rows: 24-field contract + churned
  data/export/churn_renewals_audit.csv  every renewal with outcome, route, dates
  data/export/hero_inference_record.json  today's T-7 record for sub_maya (no label)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = Path(os.environ.get("CHURN_SAMPLE_DIR", ROOT / "data/sample/churn"))
EXPORT = Path(os.environ.get("CHURN_EXPORT_DIR", ROOT / "data/export"))
HERO_ID = os.environ.get("CHURN_HERO_ID", "sub_maya")

PLANS = ["pro", "pro_plus", "ultra"]
ALLOWANCE = {"pro": 550, "pro_plus": 1650, "ultra": 11000}
CAP_CUT = 0.83
DUNNING_DAYS = 14

FEATURES = [
    "plan_tier", "renewals_completed", "active_days_7d", "active_days_28d", "engagement_trend",
    "last_active_days_ago", "agent_requests_28d", "allowance_used_pct", "limit_hits_14d",
    "cheap_model_share_28d", "overage_usd_28d", "overage_toggled_off",
    "suggestion_accept_rate_28d", "accept_rate_change", "agent_task_success_rate",
    "failed_requests_rate", "incident_exposed_28d", "support_tickets_90d",
    "ide_sessions_28d", "cli_sessions_28d", "weekend_usage_ratio",
    "first_renewal_after_pricing_change",
]
TRAIN_COLUMNS = ["user_id", "user_name", *FEATURES, "churned"]


def _read(name: str, dates: list[str]) -> pd.DataFrame:
    path = SAMPLE / name
    df = pd.read_csv(path) if path.exists() and path.stat().st_size else pd.DataFrame()
    for c in dates:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    return df


def silver() -> dict[str, pd.DataFrame]:
    snaps = _read("subscription_snapshots.csv", ["snapshot_date", "current_period_end", "started_at"])
    snaps["plan_tier"] = snaps["plan_tier"].str.lower().str.strip()
    snaps["user_name"] = snaps["user_name"].str.strip()
    snaps = snaps.drop_duplicates(["subscription_id", "snapshot_date"], keep="last")
    usage = _read("daily_usage.csv", ["activity_date"])
    usage = usage[usage["activity_date"].notna()].drop_duplicates(["subscription_id", "activity_date"])
    return {
        "snapshots": snaps,
        "usage": usage,
        "invoices": _read("invoices.csv", ["invoice_date"]),
        "sub_events": _read("subscription_events.csv", ["event_date"]),
        "limits": _read("limit_events.csv", ["hit_at"]).assign(
            hit_date=lambda d: d["hit_at"].dt.normalize() if len(d) else d.get("hit_at")
        ),
        "overage_settings": _read("overage_settings.csv", ["changed_at"]),
        "overage_charges": _read("overage_charges.csv", ["charged_at"]),
        "incidents": _read("incidents.csv", ["starts_on", "ends_on"]),
        "tickets": _read("support_tickets.csv", ["created_date"]),
        "pricing": _read("pricing_changes.csv", ["effective_date"]),
    }


def _window(events: pd.DataFrame, rows: pd.DataFrame, col: str, days: int) -> pd.DataFrame:
    """Events for each renewal row with as_of - days < event <= as_of (point-in-time)."""
    j = rows[["subscription_id", "as_of"]].merge(events, on="subscription_id", how="inner")
    return j[(j[col] > j["as_of"] - pd.Timedelta(days=days)) & (j[col] <= j["as_of"])]


def gold(s: dict[str, pd.DataFrame], today: pd.Timestamp) -> pd.DataFrame:
    snaps = s["snapshots"]
    rows = snaps[snaps["snapshot_date"] == snaps["current_period_end"] - pd.Timedelta(days=7)].copy()
    rows = rows.rename(columns={"current_period_end": "renewal_date", "snapshot_date": "as_of"})
    key = ["subscription_id", "as_of"]

    def agg(df, how, name):
        return (df.groupby(key)[how[0]].agg(how[1]).rename(name) if len(df) else pd.Series(name=name, dtype=float))

    u28 = _window(s["usage"], rows, "activity_date", 28)
    u7 = _window(s["usage"], rows, "activity_date", 7)
    uprev = _window(s["usage"], rows.assign(as_of=rows["as_of"] - pd.Timedelta(days=28)), "activity_date", 28)
    uprev["as_of"] = uprev["as_of"] + pd.Timedelta(days=28)
    last = s["usage"].merge(rows[key], on="subscription_id")
    last = last[last["activity_date"] <= last["as_of"]].groupby(key)["activity_date"].max().rename("last_active")

    u28 = u28.assign(weekend=(u28["activity_date"].dt.weekday >= 5).astype(int))
    sums = u28.groupby(key)[[
        "ide_sessions", "cli_sessions", "agent_requests", "cheap_model_requests", "suggestions_shown",
        "suggestions_accepted", "agent_tasks", "agent_tasks_kept", "total_requests", "failed_requests", "weekend",
    ]].sum()
    feats = rows.set_index(key).join(sums).join(
        [
            u28.groupby(key)["activity_date"].nunique().rename("active_days_28d"),
            u7.groupby(key)["activity_date"].nunique().rename("active_days_7d"),
            uprev.groupby(key)[["suggestions_shown", "suggestions_accepted"]].sum().add_prefix("prev_"),
            last,
            agg(_window(s["limits"], rows, "hit_date", 14), ("hit_date", "size"), "limit_hits_14d"),
            agg(_window(s["overage_charges"], rows, "charged_at", 28), ("amount_usd", "sum"), "overage_usd_28d").round(2),
            agg(_window(s["tickets"], rows, "created_date", 90), ("ticket_id", "size"), "support_tickets_90d"),
        ]
    ).fillna(0)
    out = feats.reset_index()

    # Overage switched off: latest setting on/before as_of is "disabled" after an "enabled".
    st = s["overage_settings"].merge(rows[key], on="subscription_id")
    st = st[st["changed_at"] <= st["as_of"]].sort_values("changed_at")
    latest = st.groupby(key)["overage"].last()
    ever_on = st[st["overage"] == "enabled"].groupby(key).size()
    out["overage_toggled_off"] = [
        int(latest.get((a, b)) == "disabled" and ever_on.get((a, b), 0) > 0)
        for a, b in zip(out["subscription_id"], out["as_of"])
    ]

    # Incident exposure: active on a day inside any declared incident window.
    exposed = np.zeros(len(u28), dtype=bool)
    for _, inc in s["incidents"].iterrows():
        exposed |= (u28["activity_date"] >= inc["starts_on"]).to_numpy() & (u28["activity_date"] <= inc["ends_on"]).to_numpy()
    exp_keys = set(map(tuple, u28.loc[exposed, key].itertuples(index=False)))
    out["incident_exposed_28d"] = [int((a, b) in exp_keys) for a, b in zip(out["subscription_id"], out["as_of"])]

    def ratio(num, den, default=0.0):
        return np.where(out[den] > 0, out[num] / out[den].where(out[den] > 0, 1), default)

    changes = sorted(s["pricing"]["effective_date"]) if len(s["pricing"]) else []
    n_cuts = sum((out["as_of"] >= c).astype(int) for c in changes) if changes else 0
    allowance = out["plan_tier"].map(ALLOWANCE) * CAP_CUT ** n_cuts
    out["agent_requests_28d"] = out["agent_requests"].astype(int)
    out["allowance_used_pct"] = np.clip(out["agent_requests"] / allowance, 0, 3).round(4)
    out["cheap_model_share_28d"] = ratio("cheap_model_requests", "agent_requests").round(4)
    out["suggestion_accept_rate_28d"] = ratio("suggestions_accepted", "suggestions_shown").round(4)
    prev = ratio("prev_suggestions_accepted", "prev_suggestions_shown", np.nan)
    out["accept_rate_change"] = np.clip(
        np.where((prev > 0) & (out["suggestions_shown"] > 0), out["suggestion_accept_rate_28d"] / np.where(prev > 0, prev, 1), 1.0),
        0, 3,
    ).round(4)
    out["agent_task_success_rate"] = ratio("agent_tasks_kept", "agent_tasks").round(4)
    out["failed_requests_rate"] = ratio("failed_requests", "total_requests").round(4)
    out["weekend_usage_ratio"] = ratio("weekend", "active_days_28d").round(4)
    out["engagement_trend"] = np.clip(out["active_days_7d"] / np.maximum(1.0, out["active_days_28d"] / 4.0), 0, 4).round(4)
    out["last_active_days_ago"] = np.where(
        out["last_active"] != 0,
        (out["as_of"] - pd.to_datetime(out["last_active"].where(out["last_active"] != 0))).dt.days,
        90,
    ).clip(0, 90).astype(int)
    out["ide_sessions_28d"] = out["ide_sessions"].astype(int)
    out["cli_sessions_28d"] = out["cli_sessions"].astype(int)
    for c in ("active_days_7d", "active_days_28d", "limit_hits_14d", "support_tickets_90d"):
        out[c] = out[c].astype(int)
    # First renewal since a pricing change: a change landed inside this billing period
    # (grandfathered until now) for a subscription that started before it.
    flag = np.zeros(len(out), dtype=bool)
    for c in changes:
        flag |= ((c >= out["renewal_date"] - pd.Timedelta(days=30)) & (c < out["renewal_date"]) & (out["started_at"] < c)).to_numpy()
    out["first_renewal_after_pricing_change"] = flag.astype(int)

    # Renewals already paid before this one.
    inv = s["invoices"]
    paid = inv[inv["status"] == "paid"].merge(out[["subscription_id", "renewal_date"]], on="subscription_id")
    out["renewals_completed"] = out["subscription_id"].map(
        paid[paid["invoice_date"] < paid["renewal_date"]].groupby("subscription_id").size()
    ).fillna(0).astype(int)

    # --- outcome and route, from billing events after the renewal -----------------
    ev = s["sub_events"]
    sched = ev[ev["event_type"] == "cancel_scheduled"].groupby("subscription_id")["event_date"].max()
    canceled = ev[ev["event_type"] == "canceled"].groupby("subscription_id")["event_date"].max()
    paid_at = inv[inv["status"] == "paid"].groupby("subscription_id")["invoice_date"].max()
    failed_at = inv[inv["status"] == "failed"].groupby("subscription_id")["invoice_date"].min()

    def outcome(r):
        sid, t = r["subscription_id"], r["renewal_date"]
        if sid in paid_at.index and paid_at[sid] == t:
            return "renewed", "model"
        if sid in canceled.index and canceled[sid] == t and sid in sched.index and sched[sid] <= t:
            return "voluntary_lapse", ("cancel_flow" if sched[sid] <= r["as_of"] else "model")
        if sid in failed_at.index and failed_at[sid] == t and sid in canceled.index:
            return "involuntary_lapse", "dunning"
        return "pending", ("score_today" if r["as_of"] == today else "pending")

    oc = out.apply(outcome, axis=1, result_type="expand")
    out["outcome"], out["route"] = oc[0], oc[1]
    out["churned"] = (out["outcome"] == "voluntary_lapse").astype(int)
    out["user_id"] = out["subscription_id"]
    out["feature_as_of"] = out["as_of"].dt.strftime("%Y-%m-%d")
    out["renewal_date"] = out["renewal_date"].dt.strftime("%Y-%m-%d")
    out["built_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return out[TRAIN_COLUMNS + ["outcome", "route", "feature_as_of", "renewal_date", "city", "built_at"]]


def main() -> None:
    s = silver()
    today = s["snapshots"]["snapshot_date"].max()
    g = gold(s, today)
    EXPORT.mkdir(parents=True, exist_ok=True)

    audit_path = EXPORT / "churn_renewals_audit.csv"
    g.sort_values("user_id").to_csv(audit_path, index=False)

    train = g[g["route"] == "model"].sort_values("user_id")[TRAIN_COLUMNS]
    train_path = EXPORT / "churn_user_features.csv"
    train.to_csv(train_path, index=False)

    hero = g[(g["user_id"] == HERO_ID) & (g["route"] == "score_today")]
    if hero.empty:
        raise SystemExit(f"{HERO_ID} has no T-7 snapshot for today ({today:%Y-%m-%d})")
    record = {k: (v.item() if hasattr(v, "item") else v) for k, v in hero.iloc[0][TRAIN_COLUMNS[:-1]].items()}
    json_path = EXPORT / "hero_inference_record.json"
    json_path.write_text(json.dumps(record, indent=2) + "\n")

    routes = g["route"].value_counts().to_dict()
    print(f"Wrote {audit_path} ({len(g)} renewals; routes {routes})")
    print(f"Wrote {train_path} ({len(train)} rows, voluntary-lapse rate {train['churned'].mean():.3f})")
    print(f"Wrote {json_path} ({HERO_ID}, as of {today:%Y-%m-%d})")


if __name__ == "__main__":
    main()
