# Renewal gold: is it enough for Retention Radar?

Checklist for the data foundation that feeds
[retention-radar](https://github.com/santoshshinde2012/retention-radar). The lakehouse
builds features and labels; model choice and the renewal policy live in retention-radar.

Checked 2026-09-30 · seed 42 · `make churn-gold-local` + `make churn-check --strict` +
`make churn-parity` (Spark SQL in local mode vs pandas, 8,001 renewals × 27 columns, exact).

| Criterion | Result |
|-----------|--------|
| Contract | Train CSV = 22 features + `user_id` / `user_name` + `churned` (25 columns, retention-radar order). Inference JSON = the 24 fields, no label. |
| Point in time | Features read only events dated on or before each renewal's T-7. Bronze has usage and cap hits after T-7; gold ignores them. |
| Label | Derived from billing events after the renewal (paid invoice, scheduled cancel, failed invoice + retries), not carried in from a source column. |
| Routing | 8,001 snapshots → 7,387 model rows · 326 dunning · 287 cancel flow · 1 scored today. Dunning and cancel-flow rows never reach the train export (`churn-check` fails if they do). |
| Volume | 7,387 renewals, 548 voluntary lapses (7.4%). |
| Nulls | 0 in the export. Zero-denominator ratios are defined as 0 (1.0 for `accept_rate_change`). |
| Spark vs pandas | Same result on every contract column plus `outcome` and `route`. |

Lapses by plan:

| plan_tier | renewals | voluntary lapse rate | lapses |
|-----------|---------:|--------------------:|-------:|
| pro | 5,815 | 8.0% | 464 |
| pro_plus | 1,258 | 5.8% | 73 |
| ultra | 314 | 3.5% | 11 |

Ultra is thin: 11 lapses. That is enough to score Ultra subscribers and to show why the
one person-written playbook is Ultra-only. It is not enough to tune anything Ultra-specific.

## Reproduce

```bash
# lakehouse
make churn-sample && make churn-gold-local && make churn-parity

# retention-radar, cloned beside this repo
./scripts/run_lakehouse_e2e.sh ../local-data-lakehouse
```

`run_lakehouse_e2e.sh` trains in `artifacts/lakehouse_run/` and writes
`results/lakehouse-e2e-summary.json`; retention-radar's committed `models/` stay on its
synthetic seed-42 run.
