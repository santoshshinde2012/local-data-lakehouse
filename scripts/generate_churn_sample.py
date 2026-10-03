#!/usr/bin/env python3
"""Generate bronze event CSVs for a monthly AI coding assistant (seed 42).

The source systems of a self-serve AI coding assistant, as raw events:

  subscription_snapshots.csv  the billing system's daily snapshot, kept for each
                           subscription's T-7 day (plan, status, current_period_end)
  invoices.csv             every invoice attempt (paid / failed), incl. dunning retries
  subscription_events.csv  cancel_scheduled / canceled events
  daily_usage.csv          one row per subscriber per active day (IDE, CLI, agent, suggestions)
  limit_events.csv         each time a 5-hour or weekly cap blocked a request
  overage_settings.csv     paid overage switched on / off
  overage_charges.csv      overage billed
  incidents.csv            declared incident windows (global)
  support_tickets.csv      tickets opened
  pricing_changes.csv      the date the plan caps were cut

Usage events keep coming after each subscriber's T-7 date on purpose: the gold
job must not read them (point-in-time correctness is the thing being taught).
One extra subscription, `sub_santosh`, renews after the data ends; gold scores him
"today" and exports him as the inference record.

Usage:
  N_USERS=8000 python scripts/generate_churn_sample.py
  make churn-sample
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ.get("CHURN_SAMPLE_DIR", ROOT / "data/sample/churn"))
SEED = int(os.environ.get("CHURN_SEED", "42"))
N_USERS = int(os.environ.get("N_USERS", "8000"))

PLANS = ("pro", "pro_plus", "ultra")
PRICE = {"pro": 20.0, "pro_plus": 60.0, "ultra": 200.0}
# Frontier-model requests included per 28 days, before the cap cut.
ALLOWANCE = {"pro": 550, "pro_plus": 1650, "ultra": 11000}
CAP_CUT = 0.83
PRICING_CHANGE = pd.Timestamp("2026-08-15")
# A second cut after the cohort's last renewal: only renewals scored "today" see it.
SECOND_CHANGE = pd.Timestamp("2026-09-20")
COHORT_START = pd.Timestamp("2026-06-15")  # renewal dates in the cohort
COHORT_DAYS = 93
DATA_END = pd.Timestamp("2026-09-30")  # "today"; last day any event is recorded
HISTORY_DAYS = 63  # usage history before each renewal
INCIDENTS = [
    ("inc-001", "2026-07-20", "2026-07-22"),
    ("inc-002", "2026-08-24", "2026-08-26"),
    ("inc-003", "2026-09-08", "2026-09-09"),
]
FIRST = ["Alex", "Jordan", "Sam", "Riley", "Casey", "Avery", "Quinn", "Morgan",
         "Priya", "Rahul", "Ananya", "Sofia", "Marcus", "Emily", "Vikram", "Aisha"]
LAST = ["Sharma", "Patel", "Chen", "Garcia", "Kim", "Singh", "Brown", "Nguyen",
        "Khan", "Iyer", "Mehta", "Carter", "Ramirez", "Miles", "Lee", "Shah"]


def _clip(a, lo, hi):
    return np.clip(a, lo, hi)


def main() -> None:
    rng = np.random.default_rng(SEED)
    OUT.mkdir(parents=True, exist_ok=True)
    n = N_USERS

    # --- hidden causes (never written to bronze) ------------------------------
    need = rng.beta(2.0, 2.2, n)
    fit = rng.normal(0, 1, n)
    price_sensitive = rng.beta(2.0, 3.0, n)
    side_project = rng.random(n) < 0.30
    pull = np.where(rng.random(n) < 0.24, rng.uniform(0.3, 1.0, n), 0.0)

    heavy = need + rng.normal(0, 0.15, n) - 0.4 * price_sensitive
    plan = np.where(heavy > 0.80, "ultra", np.where(heavy > 0.55, "pro_plus", "pro"))
    renewals_before = _clip(rng.geometric(0.16, n) - 1, 0, 48)
    renewal = COHORT_START + pd.to_timedelta(rng.integers(0, COHORT_DAYS, n), unit="D")
    started = renewal - pd.to_timedelta(30 * (renewals_before + 1), unit="D")

    uid = np.array([f"sub_{i:05d}" for i in range(n)])
    subs = pd.DataFrame({
        "subscription_id": uid,
        "snapshot_date": (renewal - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
        "user_name": [f"{rng.choice(FIRST)} {rng.choice(LAST)}" for _ in range(n)],
        "plan_tier": plan,
        "status": "active",
        "current_period_end": renewal.strftime("%Y-%m-%d"),
        "started_at": started.strftime("%Y-%m-%d"),
        "city": rng.choice(["Pune", "Bengaluru", "Austin", "Berlin", "London", "Seattle"], n),
    })

    # --- daily usage over the 63 days before each renewal --------------------
    base_rate = _clip(0.12 + 0.62 * need - 0.25 * pull + rng.normal(0, 0.05, n), 0.02, 0.95)
    lull = side_project & (rng.random(n) < 0.35)
    recent_rate = _clip(base_rate * (1 - 0.75 * pull) * np.where(lull, 0.25, 1.0), 0.0, 0.98)
    ide_share = rng.beta(3, 2, n)
    accept_base = _clip(0.27 + 0.05 * fit + rng.normal(0, 0.05, n), 0.02, 0.9)
    incident_hit = rng.random(n) < 0.6  # was working during incidents if active
    accept_shift = _clip(1.0 + 0.07 * fit - 0.10 * pull + rng.normal(0, 0.07, n), 0.3, 2.0)
    success = _clip(0.60 + 0.09 * fit + rng.normal(0, 0.07, n), 0.0, 1.0)
    fail_base = 0.02 + rng.exponential(0.02, n)

    idx = np.repeat(np.arange(n), HISTORY_DAYS)
    offset = np.tile(np.arange(HISTORY_DAYS, 0, -1), n)  # days before renewal: 63 … 1
    day = renewal.values[idx] - offset.astype("timedelta64[D]")
    day = pd.DatetimeIndex(day)
    weekend = day.weekday >= 5
    rate = np.where(offset > 14, base_rate[idx], recent_rate[idx])
    rate = rate * np.where(side_project[idx], np.where(weekend, 1.6, 0.75), np.where(weekend, 0.45, 1.15))
    active = (rng.random(len(idx)) < _clip(rate, 0, 0.98)) & (day <= DATA_END)
    idx, offset, day, weekend = idx[active], offset[active], day[active], weekend[active]
    m = len(idx)

    plan_mult = np.select([plan == "ultra", plan == "pro_plus"], [8.0, 2.6], 1.0)
    demand = need * rng.lognormal(0, 0.55, n) * 1.6 * (1 - 0.6 * pull)
    agent_req = rng.poisson(demand[idx] * 30 * plan_mult[idx])
    shown = rng.poisson(35, m)
    acc = np.where(offset > 35, accept_base[idx], _clip(accept_base[idx] * accept_shift[idx], 0.01, 0.95))
    in_incident = np.zeros(m, dtype=bool)
    for _, a, b in INCIDENTS:
        in_incident |= (day >= pd.Timestamp(a)) & (day <= pd.Timestamp(b))
    in_incident &= incident_hit[idx]
    acc = np.where(in_incident, acc * 0.8, acc)
    accepted = rng.binomial(shown, _clip(acc, 0, 1))
    tasks = rng.poisson(2.5, m)
    kept = rng.binomial(tasks, _clip(success[idx] - 0.1 * in_incident, 0, 1))
    total_req = agent_req + shown
    failed = rng.binomial(total_req, _clip(fail_base[idx] + 0.05 * in_incident, 0, 1))

    # Cap pressure: requests over the 28 days before T-7 against the plan allowance.
    usage = pd.DataFrame({
        "subscription_id": uid[idx],
        "activity_date": day.strftime("%Y-%m-%d"),
        "ide_sessions": rng.poisson(1.2 * ide_share[idx] * (1 - 0.5 * pull[idx]) + 0.2),
        "cli_sessions": rng.poisson(1.0 * (1 - ide_share[idx]) + 0.1),
        "agent_requests": agent_req,
        "cheap_model_requests": 0,  # filled below once cap hits are known
        "suggestions_shown": shown,
        "suggestions_accepted": accepted,
        "agent_tasks": tasks,
        "agent_tasks_kept": kept,
        "total_requests": total_req,
        "failed_requests": failed,
    })
    window = (offset > 7) & (offset <= 35)
    req_28 = np.bincount(idx[window], weights=agent_req[window], minlength=n)
    allowance = np.array([ALLOWANCE[p] for p in plan]) * CAP_CUT
    used_pct = req_28 / allowance

    # Cap hits over the 21 days before renewal (the gold job may only count the
    # ones up to T-7), rationing to the cheap model, and overage.
    hits_total = rng.poisson(np.maximum(0.0, used_pct - 0.75) * 5.5 * 1.5)
    hit_rows = []
    for i in np.nonzero(hits_total)[0]:
        offs = rng.integers(1, 22, hits_total[i])
        for o in offs:
            t = renewal[i] - pd.Timedelta(days=int(o)) + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            if t <= DATA_END:
                hit_rows.append((uid[i], t.strftime("%Y-%m-%d %H:%M:%S"), rng.choice(["five_hour", "weekly"])))
    limits = pd.DataFrame(hit_rows, columns=["subscription_id", "hit_at", "limit_type"])
    if len(limits):
        d = pd.to_datetime(limits["hit_at"]).dt.normalize()
        r = pd.to_datetime(limits["subscription_id"].map(dict(zip(uid, renewal))))
        in_win = (d > r - pd.Timedelta(days=21)) & (d <= r - pd.Timedelta(days=7))  # as-of dates
        hits_14 = limits[in_win].groupby("subscription_id").size()
    else:
        hits_14 = pd.Series(dtype=int)
    hits_14 = pd.Series(uid).map(hits_14).fillna(0).to_numpy()

    cheap_share = _clip(0.12 + 0.30 * price_sensitive + 0.07 * np.minimum(hits_14, 6) + rng.normal(0, 0.06, n), 0, 1)
    usage["cheap_model_requests"] = rng.binomial(usage["agent_requests"].to_numpy(), cheap_share[idx])

    overage_on = (used_pct > 0.9) & (rng.random(n) < 0.35)
    overage_amt = np.where(overage_on, np.maximum(0, used_pct - 1) * np.array([PRICE[p] for p in plan]) * rng.uniform(0.6, 1.4, n), 0)
    toggled_off = overage_on & (rng.random(n) < _clip(0.15 + 0.6 * price_sensitive + overage_amt / 150, 0, 0.95))
    settings, charges = [], []
    for i in np.nonzero(overage_on)[0]:
        settings.append((uid[i], (renewal[i] - pd.Timedelta(days=45)).strftime("%Y-%m-%d"), "enabled"))
        if overage_amt[i] > 0:
            charges.append((uid[i], (renewal[i] - pd.Timedelta(days=int(rng.integers(9, 30)))).strftime("%Y-%m-%d"), round(float(overage_amt[i]), 2)))
        if toggled_off[i]:
            settings.append((uid[i], (renewal[i] - pd.Timedelta(days=int(rng.integers(8, 12)))).strftime("%Y-%m-%d"), "disabled"))

    tickets = []
    lam = 0.15 + 0.35 * incident_hit * 0.3 + 0.25 * (hits_14 >= 3) + 0.5 * toggled_off
    for i in range(n):
        for _ in range(rng.poisson(lam[i])):
            t = renewal[i] - pd.Timedelta(days=int(rng.integers(1, 97)))
            if t <= DATA_END:
                tickets.append((f"t-{len(tickets) + 1:06d}", uid[i], t.strftime("%Y-%m-%d")))

    # --- the renewal outcome (from causes + the footprints above) -------------
    first_after_change = (renewal > PRICING_CHANGE) & (renewal <= PRICING_CHANGE + pd.Timedelta(days=30))
    exposed = np.bincount(idx[in_incident & (offset > 7) & (offset <= 35)], minlength=n) > 0
    tenure_risk = np.select(
        [renewals_before == 0, renewals_before == 1, renewals_before <= 3],
        [0.95, 0.5, 0.2],
        default=-0.3 * np.log1p(np.maximum(renewals_before - 3, 0)),
    )
    cap_pain = np.minimum(hits_14, 6) * np.where(plan == "pro", 0.30, 0.14)
    logit = (
        -3.75 + tenure_risk + 1.7 * pull - 0.50 * fit + 0.9 * price_sensitive - 1.1 * (need - 0.5)
        + 0.55 * (side_project & lull)
        + cap_pain * (1 + 0.9 * first_after_change)
        + 0.35 * first_after_change * (plan == "pro")
        + 0.85 * toggled_off
        + 0.30 * exposed * (fit < 0)
        + rng.normal(0, 0.55, n)
    )
    voluntary = rng.random(n) < 1 / (1 + np.exp(-logit))
    involuntary = ~voluntary & (rng.random(n) < 0.038 + 0.02 * (renewals_before == 0))
    early_cancel = voluntary & (rng.random(n) < 0.35)

    invoices, sub_events = [], []
    for i in range(n):
        for k in range(1, renewals_before[i] + 1):
            invoices.append((uid[i], (started[i] + pd.Timedelta(days=30 * k)).strftime("%Y-%m-%d"), PRICE[plan[i]], "paid", 1))
        r = renewal[i]
        if voluntary[i]:
            back = int(rng.integers(8, 31)) if early_cancel[i] else int(rng.integers(0, 7))
            sub_events.append((uid[i], (r - pd.Timedelta(days=back)).strftime("%Y-%m-%d"), "cancel_scheduled"))
            sub_events.append((uid[i], r.strftime("%Y-%m-%d"), "canceled"))
        elif involuntary[i]:
            for a, d in enumerate((0, 3, 7, 14), start=1):
                invoices.append((uid[i], (r + pd.Timedelta(days=d)).strftime("%Y-%m-%d"), PRICE[plan[i]], "failed", a))
            sub_events.append((uid[i], (r + pd.Timedelta(days=14)).strftime("%Y-%m-%d"), "canceled"))
        else:
            invoices.append((uid[i], r.strftime("%Y-%m-%d"), PRICE[plan[i]], "paid", 1))

    # --- Santosh: renews a week after the data ends; scored "today" --------------
    santosh_t = DATA_END + pd.Timedelta(days=7)
    santosh_start = santosh_t - pd.Timedelta(days=120)
    subs = pd.concat([subs, pd.DataFrame([{
        "subscription_id": "sub_santosh", "snapshot_date": DATA_END.strftime("%Y-%m-%d"),
        "user_name": "Santosh (worked example)", "plan_tier": "pro", "status": "active",
        "current_period_end": santosh_t.strftime("%Y-%m-%d"),
        "started_at": santosh_start.strftime("%Y-%m-%d"), "city": "Pune",
    }])], ignore_index=True)
    for k in (1, 2, 3):
        invoices.append(("sub_santosh", (santosh_start + pd.Timedelta(days=30 * k)).strftime("%Y-%m-%d"), 20.0, "paid", 1))
    santosh_days = []
    for o in range(63, 35, -1):  # the month before: busier, accepting more
        if o % 2 == 1:
            d = santosh_t - pd.Timedelta(days=o)
            santosh_days.append({"subscription_id": "sub_santosh", "activity_date": d.strftime("%Y-%m-%d"),
                              "ide_sessions": 2, "cli_sessions": 0, "agent_requests": 30,
                              "cheap_model_requests": 6, "suggestions_shown": 36, "suggestions_accepted": 12,
                              "agent_tasks": 3, "agent_tasks_kept": 2, "total_requests": 66,
                              "failed_requests": 2})
    for o in range(35, 7, -1):
        d = santosh_t - pd.Timedelta(days=o)
        if (o > 14 and o % 2 == 0) or o in (9, 12):
            santosh_days.append({"subscription_id": "sub_santosh", "activity_date": d.strftime("%Y-%m-%d"),
                              "ide_sessions": 1, "cli_sessions": 1, "agent_requests": 34,
                              "cheap_model_requests": 22, "suggestions_shown": 36, "suggestions_accepted": 10,
                              "agent_tasks": 3, "agent_tasks_kept": 2, "total_requests": 70,
                              "failed_requests": 3})
    usage = pd.concat([usage, pd.DataFrame(santosh_days)], ignore_index=True)
    for o in (10, 12, 13):
        limits.loc[len(limits)] = ("sub_santosh", (santosh_t - pd.Timedelta(days=o)).strftime("%Y-%m-%d 14:00:00"), "weekly")

    # --- write bronze ---------------------------------------------------------
    subs.to_csv(OUT / "subscription_snapshots.csv", index=False)
    pd.DataFrame(invoices, columns=["subscription_id", "invoice_date", "amount_usd", "status", "attempt"]).to_csv(OUT / "invoices.csv", index=False)
    pd.DataFrame(sub_events, columns=["subscription_id", "event_date", "event_type"]).to_csv(OUT / "subscription_events.csv", index=False)
    usage.sort_values(["subscription_id", "activity_date"]).to_csv(OUT / "daily_usage.csv", index=False)
    limits.to_csv(OUT / "limit_events.csv", index=False)
    pd.DataFrame(settings, columns=["subscription_id", "changed_at", "overage"]).to_csv(OUT / "overage_settings.csv", index=False)
    pd.DataFrame(charges, columns=["subscription_id", "charged_at", "amount_usd"]).to_csv(OUT / "overage_charges.csv", index=False)
    pd.DataFrame(INCIDENTS, columns=["incident_id", "starts_on", "ends_on"]).to_csv(OUT / "incidents.csv", index=False)
    pd.DataFrame(tickets, columns=["ticket_id", "subscription_id", "created_date"]).to_csv(OUT / "support_tickets.csv", index=False)
    pd.DataFrame([
        ("cap-cut-2026-08", PRICING_CHANGE.strftime("%Y-%m-%d"), "weekly and 5-hour caps cut 17%"),
        ("cap-cut-2026-09", SECOND_CHANGE.strftime("%Y-%m-%d"), "weekly caps cut again"),
    ], columns=["change_id", "effective_date", "description"]).to_csv(OUT / "pricing_changes.csv", index=False)

    lapse = (voluntary | involuntary).mean()
    print(f"Wrote bronze to {OUT}")
    print(f"  subscriptions={len(subs)} usage_rows={len(usage)} limit_events={len(limits)} "
          f"invoices={len(invoices)} tickets={len(tickets)}")
    print(f"  cohort lapse rate={lapse:.3f} (voluntary {voluntary.mean():.3f}, involuntary {involuntary.mean():.3f})")


if __name__ == "__main__":
    main()
