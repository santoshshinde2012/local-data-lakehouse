# Renewal features: from billing and usage events to the radar export

The renewal path builds `gold.churn_renewal_features`: one row per renewal, with every feature as of seven days before the renewal (T-7), then exports it for [retention-radar](https://github.com/santoshshinde2012/retention-radar). Back to the [README](../README.md).

## Source tables (bronze)

The source systems of a self-serve AI coding assistant (Pro $20 · Pro+ $60 · Ultra $200 a month), as raw events:

| Bronze table | From | Grain |
|---|---|---|
| `churn_subscription_snapshots_raw` | billing snapshot | subscription × snapshot date (`current_period_end`) |
| `churn_invoices_raw` | billing | invoice attempt (paid / failed, dunning retries) |
| `churn_subscription_events_raw` | billing | `cancel_scheduled`, `canceled` |
| `churn_usage_raw` | product analytics | subscription × active day (IDE, CLI, agent, suggestions) |
| `churn_limit_events_raw` | rate limiter | one row per blocked request (5-hour / weekly cap) |
| `churn_overage_settings_raw`, `churn_overage_charges_raw` | billing | overage switched on/off; overage billed |
| `churn_incidents_raw` | status page | incident windows |
| `churn_support_tickets_raw` | helpdesk | ticket opened |
| `churn_pricing_changes_raw` | product | date the plan caps were cut |


## Gold rules

Gold (`sql/churn/gold_renewal_features.sql`) builds one row per renewal from the snapshot taken seven days before it:

- **Point in time.** Every feature reads events dated on or before T-7. Bronze keeps usage and cap hits after T-7 on purpose; the gold SQL must ignore them.
- **Label from billing.** A paid invoice at the renewal date means renewed. A cancel that takes effect at the renewal date is a voluntary lapse. A failed invoice followed by a cancellation after retries is involuntary.
- **Routes.** Renewals lost to failed cards go to **dunning**. Voluntary lapses whose cancel was already scheduled by T-7 go to the **cancel flow**. Both are kept out of the train export. A renewal whose T-7 is today is **score_today**: it is exported for inference, with no label.


## Outputs

| Artifact | Location |
|---|---|
| Gold table | `lakehouse.gold.churn_renewal_features` |
| Pandas twin (light demo, T3) | `lakehouse.gold.churn_renewal_features_twin` |
| Train CSV | `data/export/churn_user_features.csv` (24-field contract + `churned`) |
| Audit CSV | `data/export/churn_renewals_audit.csv` |
| Inference JSON | `data/export/hero_inference_record.json` (`sub_santosh`, scored as of the latest snapshot) |

Seed 42, `make churn-sample` (8,000 subscriptions): 7,387 renewals routed to the model (7.4% voluntary lapse), 326 to dunning, 287 to the cancel flow, and one scored today.


## Without Docker, and checking Spark against pandas

```bash
make venv
make churn-sample          # bronze events → data/sample/churn/*.csv
make churn-gold-local      # pandas gold → data/export/ (+ contract check)
make churn-check           # re-validate data/export/ against the retention-radar contract
uv pip install --python .venv/bin/python pyspark==4.1.3   # once; needs Java 17 or 21
make churn-parity          # run the gold SQL in local Spark 4.1.3 and compare with pandas row by row
                           # (needs pyspark 4.1.3: uses .venv-graph-spark if present, else PARITY_PY=...)
```

`churn-check` fails on structural breaks: columns, nulls, plan tiers, 0/1 flags, `active_days_7d > active_days_28d`, dunning or cancel-flow rows in the train export, and label or metadata leaking into the inference JSON. It warns on schema range breaches (`--strict` fails on them). CI runs `churn-parity` on the tiny fixture and the full sample (measured locally: 23.6 s and 20.8 s).

A 120-subscription fixture lives in `data/sample/churn/fixtures/tiny/` for quick demos. Sufficiency notes: [churn-gold-sufficiency.md](churn-gold-sufficiency.md).


## Settings

| Knob | Env / Make | Default |
|------|------------|---------|
| Subscriptions | `N_USERS` | 8000 |
| Seed | `CHURN_SEED` | 42 |
| Inference subscriber | `CHURN_HERO_ID` | `sub_santosh` |


## Hand-off to retention-radar

`./pipelines/radar_consume.sh` checks out radar `main` (`RADAR_REF=<branch or commit>` for a paired radar change), syncs the export into its `data/external/` and batch-scores it. Model choice, calibration and the renewal policy live in radar, not here. The flow and the contract: [README, how the two repos connect](../README.md#how-the-two-repos-connect).

Excerpts: [churn-e2e](demo/churn-e2e.excerpt.md), [churn-parity](demo/churn-parity.excerpt.md), [radar consume](demo/radar-consume.excerpt.md).
