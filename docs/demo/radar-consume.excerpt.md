# Retention Radar consumes the export

Captured on 2026-10-03 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `chore/sample-customer-santosh` at `2fcb92f`, in one run from empty volumes (`make purge` first); radar cloned at the head of its branch `chore/sample-customer-santosh` with `RADAR_REF`. Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices, blank lines); long outputs keep their head and tail. `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

## Sync, ingest, batch score

Exit 0, 72.4 s.

```text
$ env RADAR_REF=chore/sample-customer-santosh ./pipelines/radar_consume.sh data/export /tmp/radar-e2e
retention-radar ref: chore/sample-customer-santosh
retention-radar commit: 98df572
XGBoost/LightGBM cannot load libomp; using scikit-learn's bundled copy (or: brew install libomp)
Synced -> /tmp/radar-e2e/data/external/churn_user_features.csv
Synced -> /tmp/radar-e2e/data/external/hero_inference_record.json
Loaded 7387 rows from /tmp/radar-e2e/data/external/churn_user_features.csv
CHURN_DATA_SOURCE=lakehouse
Churn rate: 0.074
  user_id    user_name plan_tier  renewals_completed  active_days_7d  active_days_28d  engagement_trend  last_active_days_ago  agent_requests_28d  allowance_used_pct  limit_hits_14d  cheap_model_shar …
sub_00000 Ananya Singh       pro                   6               2                6            1.3333                     0                 146              0.2655               0                 0 …
sub_00001  Riley Singh       pro                  24               1                9            0.4444                     6                 306              0.5564               0                 0 …
sub_00002  Riley Patel       pro                   9               2               11            0.7273                     2                 491              0.8927               3                 0 …
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

Exit 0, 38.2 s.

```text
$ zsh -c cd /tmp/radar-e2e && git log -1 --oneline && .venv/bin/python -m pip install -q pytest httpx && CHURN_DATA_SOURCE=synthetic PYTHONPATH=src .venv/bin/python -m pytest -q -p no:cacheprovider
98df572 docs: shared Mermaid palette guide + diagram test; pipeline and lakehouse-to-radar diagrams
........................................................................ [ 71%]
.............................                                            [100%]
... (16 lines trimmed)
  /tmp/radar-e2e/.venv/lib/python3.12/site-packages/sklearn/metrics/_classification.py:1879: UndefinedMetricWarning: Precision is ill-defined and being set to 0.0 in labels with no predicted samples. …
    _warn_prf(average, modifier, f"{metric.capitalize()} is", result.shape[0])
-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
101 passed, 7 warnings in 36.66s
```
