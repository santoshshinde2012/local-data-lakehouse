# Retention Radar consumes the export (`pipelines/radar_consume.sh`)

Captured on 2026-10-02 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `feat/local-first-stack-2026` at `7f5fc43`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

Radar branch `feat/local-first-stack-2026` (radar PR #21: https://github.com/santoshshinde2012/retention-radar/pull/21), reading the Spark export of `make churn-e2e` above (`churn_user_features.csv` 7,387 rows, `hero_inference_record.json`).

> Since this run, radar PR #21 was squash-merged: radar `main` `7e3bec8` has the same tree as `65cab25` below, and
> `pipelines/radar_consume.sh` and CI now use radar `main` by default (`RADAR_REF` only for a paired radar change).
> Re-checked with `./pipelines/radar_consume.sh data/export` (ref `main`, commit `7e3bec8`): 7,387 rows scored,
> same action counts.

## Sync, ingest, batch score

Exit 0, 66.6 s.

```text
$ env RADAR_REF=feat/local-first-stack-2026 ./pipelines/radar_consume.sh data/export /tmp/radar-e2e
retention-radar ref: feat/local-first-stack-2026
retention-radar commit: 65cab25
XGBoost/LightGBM cannot load libomp; using scikit-learn's bundled copy (or: brew install libomp)
Synced -> /tmp/radar-e2e/data/external/churn_user_features.csv
Synced -> /tmp/radar-e2e/data/external/hero_inference_record.json
Loaded 7387 rows from /tmp/radar-e2e/data/external/churn_user_features.csv
CHURN_DATA_SOURCE=lakehouse
Churn rate: 0.074
  user_id    user_name plan_tier  renewals_completed  active_days_7d  active_days_28d  engagement_trend  last_active_days_ago  agent_requests_28d  allowance_used_pct  limit_hits_14d  cheap_model_share …
sub_00000 Ananya Singh       pro                   6               2                6            1.3333                     0                 146              0.2655               0                 0. …
sub_00001  Riley Singh       pro                  24               1                9            0.4444                     6                 306              0.5564               0                 0. …
sub_00002  Riley Patel       pro                   9               2               11            0.7273                     2                 491              0.8927               3                 0. …
Scored 7387 rows → /tmp/radar-e2e/scores.csv (queue order: rank 1 = highest risk)
action
no_action               6499
cancel_flow_discount     410
limit_reset              276
pause_offer              111
holdout                   87
personal_email             4
auto_action unique: ['none']
==> radar scored 7387 rows -> /tmp/radar-e2e/scores.csv
```

## Radar test suite on that checkout

Exit 0, 40.8 s.

```text
$ zsh -c cd /tmp/radar-e2e && git log -1 --oneline && .venv/bin/python -m pip install -q pytest httpx && PYTHONPATH=src .venv/bin/python -m pytest -q -p no:cacheprovider
65cab25 models: slice metrics at tau in the committed metrics.json
........................................................................ [ 75%]
........................                                                 [100%]
... (pytest warnings summary trimmed: deprecation notices from fastapi, shap, sklearn)
96 passed, 7 warnings in 39.58s
```
