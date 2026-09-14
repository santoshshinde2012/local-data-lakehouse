#!/usr/bin/env python3
"""Generate scalable churn bronze CSVs for the lakehouse sample path.

Reproducible (seed=42). Keeps Santosh Shinde as u-01.
Default N=5000 for Retention Radar research; use N_USERS=10 for a quick demo.

Usage:
  N_USERS=5000 python scripts/generate_churn_sample.py
  make churn-sample
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ.get("CHURN_SAMPLE_DIR", ROOT / "data/sample/churn"))
SEED = int(os.environ.get("CHURN_SEED", "42"))
N_USERS = int(os.environ.get("N_USERS", "5000"))
AS_OF = date.fromisoformat(os.environ.get("CHURN_AS_OF", "2024-03-02"))
USAGE_DAYS = int(os.environ.get("CHURN_USAGE_DAYS", "40"))  # days ending at AS_OF


CITIES = [
    "Pune", "Bengaluru", "Austin", "Hyderabad", "Seattle", "Mumbai",
    "San Francisco", "Chennai", "Chicago", "Ahmedabad", "London", "Berlin",
]
FIRST = [
    "Alex", "Jordan", "Sam", "Riley", "Casey", "Avery", "Quinn", "Morgan",
    "Priya", "Rahul", "Ananya", "Sofia", "Marcus", "Emily", "Vikram", "Aisha",
]
LAST = [
    "Sharma", "Patel", "Chen", "Garcia", "Kim", "Singh", "Brown", "Nguyen",
    "Khan", "Iyer", "Mehta", "Carter", "Ramirez", "Miles", "Lee", "Shah",
]
PLANS = np.array(["free", "starter", "pro", "enterprise"])
PLAN_P = [0.40, 0.30, 0.22, 0.08]


def main() -> None:
    rng = np.random.default_rng(SEED)
    OUT.mkdir(parents=True, exist_ok=True)

    # --- users ---
    engagement = rng.beta(2.2, 1.6, size=N_USERS)
    plan_tier = rng.choice(PLANS, size=N_USERS, p=PLAN_P)
    plan_boost = np.array(
        [{"free": 0.0, "starter": 0.15, "pro": 0.35, "enterprise": 0.55}[p] for p in plan_tier]
    )

    user_ids = [f"u-{i:04d}" for i in range(1, N_USERS + 1)]
    names = []
    for i in range(N_USERS):
        if i == 0:
            names.append("Santosh Shinde")
        else:
            names.append(f"{rng.choice(FIRST)} {rng.choice(LAST)}")

    signup = [
        AS_OF - timedelta(days=int(x))
        for x in rng.integers(30, 900, size=N_USERS)
    ]
    nps = np.clip(3 + engagement * 7 + rng.normal(0, 1.2, N_USERS), 0, 10).round(1)

    # latent churn propensity (aligned with RR teaching story)
    logit = (
        -1.2
        - 2.0 * engagement
        - 0.8 * plan_boost
        + 1.5 * (1 - engagement)
        + rng.normal(0, 0.6, N_USERS)
    )
    churn_p = 1 / (1 + np.exp(-logit))
    churned = (rng.random(N_USERS) < churn_p).astype(int)

    # Force Santosh profile
    user_ids[0] = "u-0001"
    names[0] = "Santosh Shinde"
    plan_tier[0] = "pro"
    signup[0] = date(2023, 6, 1)
    nps[0] = 7.0
    churned[0] = 0
    engagement[0] = 0.72

    users = pd.DataFrame(
        {
            "user_id": user_ids,
            "user_name": names,
            "plan_tier": plan_tier,
            "signup_date": signup,
            "city": rng.choice(CITIES, size=N_USERS),
            "nps_score": nps,
            "churned": churned,
        }
    )
    # re-id as u-01 style for first 10 compatibility + u-0001 for rest — keep u-0001 format consistently
    users["user_id"] = [f"u-{i:04d}" for i in range(1, N_USERS + 1)]
    users.loc[0, "user_id"] = "u-0001"

    users.to_csv(OUT / "users.csv", index=False)

    # --- daily usage (sparse-friendly: skip empty weekends sometimes) ---
    start = AS_OF - timedelta(days=USAGE_DAYS - 1)
    dates = [start + timedelta(days=d) for d in range(USAGE_DAYS)]
    usage_rows = []
    for i, uid in enumerate(users["user_id"]):
        eng = float(engagement[i])
        pb = float(plan_boost[i])
        base_sessions = max(0.2, (eng + pb) * 1.8)
        for d in dates:
            # activity probability
            p_act = min(0.95, 0.25 + eng * 0.7)
            if rng.random() > p_act:
                # still emit sparse zero rows ~20% to keep windows honest
                if rng.random() > 0.2:
                    continue
                sessions = 0
            else:
                sessions = int(max(0, rng.poisson(base_sessions)))
            is_weekend = 1 if d.weekday() >= 5 else 0
            if sessions == 0:
                usage_rows.append(
                    {
                        "user_id": uid,
                        "event_date": d.isoformat(),
                        "sessions": 0,
                        "session_minutes": 0.0,
                        "api_calls": 0,
                        "tokens": 0,
                        "tools_used": int(max(0, rng.integers(0, 3))),
                        "failed_requests": 0,
                        "successful_requests": 0,
                        "agent_runs": 0,
                        "ide_plugin_sessions": 0,
                        "is_weekend": is_weekend,
                        "spend_usd": round(float(rng.uniform(0, 1.5) * (1 - eng)), 2)
                        if plan_tier[i] != "free"
                        else 0.0,
                    }
                )
                continue
            minutes = round(float(np.clip(sessions * rng.uniform(8, 18), 1, 120)), 2)
            api = int(sessions * rng.uniform(20, 120) * (0.5 + eng))
            tokens = int(api * rng.uniform(80, 700))
            tools = int(np.clip(eng * 10 + pb * 5 + rng.normal(0, 1.5), 0, 20))
            fail = int(rng.poisson(max(0.1, (1 - eng) * 4)))
            success = max(0, api - fail)
            agents = int(np.clip(sessions * rng.uniform(1, 8) * eng, 0, 80))
            ide = int(np.clip(sessions * rng.uniform(0.5, 4), 0, 40))
            spend = (
                round(float(api * rng.uniform(0.001, 0.03) * (0.3 + pb)), 2)
                if plan_tier[i] != "free"
                else 0.0
            )
            usage_rows.append(
                {
                    "user_id": uid,
                    "event_date": d.isoformat(),
                    "sessions": sessions,
                    "session_minutes": minutes,
                    "api_calls": api,
                    "tokens": tokens,
                    "tools_used": tools,
                    "failed_requests": fail,
                    "successful_requests": success,
                    "agent_runs": agents,
                    "ide_plugin_sessions": ide,
                    "is_weekend": is_weekend,
                    "spend_usd": spend,
                }
            )

    usage = pd.DataFrame(usage_rows)
    usage.to_csv(OUT / "daily_usage_snapshots.csv", index=False)

    # --- tickets ---
    ticket_rows = []
    tid = 1
    for i, uid in enumerate(users["user_id"]):
        n_t = int(rng.poisson(max(0.2, (1 - engagement[i]) * 4)))
        for _ in range(n_t):
            created = AS_OF - timedelta(days=int(rng.integers(0, 90)))
            ticket_rows.append(
                {
                    "ticket_id": f"t-{tid:05d}",
                    "user_id": uid,
                    "created_date": created.isoformat(),
                }
            )
            tid += 1
    tickets = pd.DataFrame(ticket_rows)
    tickets.to_csv(OUT / "support_tickets.csv", index=False)

    # --- payments ---
    pay_rows = []
    pid = 1
    for i, uid in enumerate(users["user_id"]):
        if plan_tier[i] == "free":
            continue
        n_p = int(rng.integers(1, 5))
        for _ in range(n_p):
            created = AS_OF - timedelta(days=int(rng.integers(0, 90)))
            fail_p = 0.05 + (1 - engagement[i]) * 0.25
            status = "failed" if rng.random() < fail_p else "ok"
            pay_rows.append(
                {
                    "payment_id": f"p-{pid:05d}",
                    "user_id": uid,
                    "payment_date": created.isoformat(),
                    "status": status,
                }
            )
            pid += 1
    payments = pd.DataFrame(pay_rows)
    payments.to_csv(OUT / "payments.csv", index=False)

    rate = float(users["churned"].mean())
    print(f"Wrote sample to {OUT}")
    print(f"  users={len(users)} churn_rate={rate:.3f}")
    print(f"  usage_rows={len(usage)} tickets={len(tickets)} payments={len(payments)}")
    print(f"  Santosh: {users.iloc[0][['user_id','user_name','plan_tier','churned']].to_dict()}")
    if rate < 0.05 or rate > 0.55:
        raise SystemExit(f"churn rate {rate:.3f} outside teaching band; retune generator")


if __name__ == "__main__":
    main()
