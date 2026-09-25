# Churn gold sufficiency (Retention Radar)

Audit checklist for the **data foundation** that feeds
[retention-radar](https://github.com/santoshshinde2012/retention-radar).
Gold features are the SoR; **algorithms live in Retention Radar**.

Verified: **2026-09-25** · seed **42** · path `make churn-gold-local` (pandas Spark-parity) + `make churn-check` (export contract).

## Verdict

**Ready: YES** — default **N=5000** is sufficient for teaching E2E (train / calibrate / infer / Streamlit).

| Criterion | Result |
|-----------|--------|
| Schema | Train CSV = 22 features + `user_id` / `user_name` + `churned` (25 cols). Serve JSON = 24 fields (no label). Matches radar `FEATURE_COLUMNS` + `configs/schemas/user_record.schema.json`. |
| Volume | **N=5000**, train-capable; stratified churn **17.0%** (target band 15–25%). |
| Nulls | **0%** nulls on all export columns after gold fill. |
| Hero user | **Santosh Shinde** `u-0001` present; inference JSON has no `churned`. |
| Leakage | Export drops `city`, `feature_as_of`, `built_at`. Label only on train CSV. |
| Layers | Bronze → silver → gold (Spark) **or** `churn-gold-local` pandas path; both documented. |
| Object store | Compose uses **SILO** (`pgsty/silo`), not MinIO — see [object-store.md](object-store.md). |
| Consumer | Sync → radar `data/external/` → `CHURN_DATA_SOURCE=lakehouse`. |

## Slice notes (why not forced scale-up)

| plan_tier | n | churn rate | churn positives |
|-----------|--:|----------:|----------------:|
| free | 1999 | 18.6% | 371 |
| starter | 1479 | 17.7% | 262 |
| pro | 1137 | 15.0% | 170 |
| enterprise | 385 | 12.2% | 47 |

- Overall and free/starter/pro slices are thick enough for teaching stratified metrics.
- **Enterprise** is the thinnest (47 positives). Fine for demos that mention the tier; thin if you want enterprise-only model tuning.
- Optional: `N_USERS=10000 CHURN_SEED=42 make churn-sample && make churn-gold-local` (~2× enterprise mass) — keep seed 42 for reproducibility. Default stays **5000**.

## Quality / honesty

- **Churn label:** bronze `users.churned` carried into gold (defined at generate time; not derived from post-as-of outcomes in the export).
- **Train ≠ serve:** lakehouse Santosh is **event-aggregated as-of `CHURN_AS_OF` (default 2024-03-02)** — scores differ from radar’s synthetic seed-42 hero profile by design.
- **Proxies:** `models_used_count` and `seat_utilization` are documented proxies in the gold job (see Spark `03_publish_gold_features.py` / local builder).
- **Published ladder:** Retention Radar’s committed `models/` metrics stay on the **synthetic** seed-42 path. Lakehouse E2E may retrain locally; do not overwrite published models for articles.

## Reproduce

```bash
# lakehouse
N_USERS=5000 CHURN_SEED=42 make churn-gold-local
# → data/export/churn_user_features.csv
# → data/export/santosh_inference_record.json
make churn-check   # columns · nulls · tiers · leakage · Santosh · schema ranges

# retention-radar (beside this repo)
./scripts/sync_lakehouse_exports.sh ../local-data-lakehouse/data/export
CHURN_DATA_SOURCE=lakehouse pytest -q
```

Radar verify (box, 2026-09-25): **33 passed** with `CHURN_DATA_SOURCE=lakehouse` after sync; `./scripts/run_lakehouse_e2e.sh` green on Python 3.12 (calibrated test AUC ≈ 0.694, Santosh u-0001 0.399 → 0.170 → low / nurture); committed `models/` left unchanged. CI (`.github/workflows/ci.yml`) re-runs the no-Docker path, the contract check, and a Retention Radar ingest + batch-score on every push.
