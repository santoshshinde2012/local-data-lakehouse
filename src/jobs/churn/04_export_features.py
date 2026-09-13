"""Export gold churn features for downstream ML training/scoring (train CSV + Santosh inference JSON)."""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

EXPORT_DIR = os.environ.get("CHURN_EXPORT_DIR", "/opt/data/export")


def main() -> None:
    spark = SparkSession.builder.appName("09_export_churn_for_xgboost").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    out = Path(EXPORT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    df = spark.table("lakehouse.gold.churn_user_features")
    train_cols = [c for c in df.columns if c not in ("city", "feature_as_of", "built_at")]
    rows = [r.asDict(recursive=True) for r in df.select(*train_cols).orderBy("user_id").collect()]
    if not rows:
        raise SystemExit("gold.churn_user_features is empty")

    def normalize(v):
        if hasattr(v, "item"):
            return v.item()
        if hasattr(v, "isoformat"):
            return str(v)
        return v

    rows = [{k: normalize(v) for k, v in r.items()} for r in rows]

    train_path = out / "churn_user_features.csv"
    with train_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    santosh_rows = [
        r for r in rows if r.get("user_name") == "Santosh Shinde"
    ]
    if not santosh_rows:
        raise SystemExit("Santosh Shinde not found in gold.churn_user_features")
    record = {k: v for k, v in santosh_rows[0].items() if k != "churned"}
    json_path = out / "santosh_inference_record.json"
    with json_path.open("w") as f:
        json.dump(record, f, indent=2, default=str)
        f.write("\n")

    print(f"Wrote {train_path} ({len(rows)} rows)")
    print(f"Wrote {json_path}")
    print("Santosh inference keys:", sorted(record.keys()))
    print(
        "Next: use these exports with https://github.com/santoshshinde2012/downstream ML"
    )
    print("Export OK.")
    spark.stop()


if __name__ == "__main__":
    main()
