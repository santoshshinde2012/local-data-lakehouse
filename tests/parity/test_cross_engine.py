"""T3: cross-engine parity on the `full` profile: Spark 4.1 writes, DuckDB / PyIceberg / Polars (and
Trino, when --profile trino is up) read and recompute, all through the one REST catalog.

    make test-t3        # up-full TRINO=1 --wait, Spark retail + churn E2E, then this file

The Spark pipelines run once per session (LDL_T3_REUSE=1 skips them and uses the tables already
in the catalog). Needs ldl-spark running; skips otherwise unless LDL_REQUIRE_STACK=1.
"""
from __future__ import annotations

import os
import subprocess
import sys

import light_demo
import numpy as np
import pytest
from support.ldl import REPO, reachable, trino_query

import lakehouse_client as lc

BASE = os.environ.get("LAKEKEEPER_URL", "http://localhost:8181").rstrip("/")
TRINO = os.environ.get("TRINO_URL", f"http://localhost:{os.environ.get('TRINO_PORT', '8088')}")
CHURN_SAMPLE = os.environ.get("CHURN_SAMPLE_DIR", str(REPO / "data/sample/churn"))


def _spark_running() -> bool:
    p = subprocess.run(["docker", "compose", "--profile", "full", "ps", "spark", "--format", "{{.State}}"],
                       cwd=REPO, capture_output=True, text=True)
    return p.returncode == 0 and p.stdout.strip() == "running"


if not (reachable(f"{BASE}/health") and _spark_running()):
    if os.environ.get("LDL_REQUIRE_STACK") == "1":
        raise RuntimeError("LDL_REQUIRE_STACK=1 but the full profile is not up (make up-full)")
    pytest.skip("full profile not up (make up-full)", allow_module_level=True)

trino = pytest.mark.skipif(not reachable(f"{TRINO}/v1/info"), reason=f"Trino not up on {TRINO} (make up-full TRINO=1)")


@pytest.fixture(scope="session")
def spark_tables():
    """Run the Spark retail + churn E2E (the same scripts as make e2e / make churn-e2e)."""
    if os.environ.get("LDL_T3_REUSE") != "1":
        for script in ("pipelines/run_retail_e2e.sh", "pipelines/run_churn_e2e.sh"):
            p = subprocess.run([str(REPO / script)], cwd=REPO, capture_output=True, text=True)
            assert p.returncode == 0, f"{script}:\n{p.stdout[-3000:]}\n{p.stderr[-3000:]}"
    return lc.catalog()


@pytest.fixture(scope="session")
def duck(spark_tables):
    return lc.duckdb_connect()


def test_duckdb_recomputes_sparks_gold_from_sparks_silver(duck):
    spark_gold = duck.sql("SELECT * FROM lakehouse.gold.daily_order_metrics ORDER BY order_date").fetchall()
    recomputed = duck.sql(light_demo.GOLD_SQL.format(silver="lakehouse.silver")).fetchall()
    assert len(spark_gold) == 2
    for a, b in zip(spark_gold, recomputed, strict=True):
        assert a[0] == b[0] and a[1] == b[1] and a[4:] == b[4:]
        assert a[2] == pytest.approx(b[2], abs=1e-9) and a[3] == pytest.approx(b[3], abs=1e-9)
    assert [(str(r[0]), r[1], round(r[2], 2), round(r[3], 2)) for r in spark_gold] == light_demo.RETAIL_GOLD


def test_three_python_engines_read_sparks_tables_identically(spark_tables, duck):
    t = spark_tables.load_table("silver.orders")
    ice = sorted(t.scan().to_arrow().select(["order_id", "status", "amount"]).to_pylist(), key=lambda r: r["order_id"])
    dk = [dict(zip(("order_id", "status", "amount"), r)) for r in
          duck.sql("SELECT order_id, status, amount FROM lakehouse.silver.orders ORDER BY order_id").fetchall()]
    pl = lc.polars_scan(t).select(["order_id", "status", "amount"]).sort("order_id").collect().to_dicts()
    assert len(ice) == 19 and ice == dk == pl


def test_sparks_day1_snapshot_time_travels_in_duckdb(spark_tables, duck):
    t = spark_tables.load_table("bronze.orders_raw")
    day1 = [s for s in t.metadata.snapshots if (s.summary or {}).get("ldl.batch") == "orders_day1"]
    assert day1, "02_ingest_bronze.py tags its day-1 append with ldl.batch"
    sid = day1[-1].snapshot_id
    assert duck.sql(f"SELECT count(*) FROM lakehouse.bronze.orders_raw AT (VERSION => {sid})").fetchone() == (10,)
    assert t.scan(snapshot_id=sid).to_arrow().num_rows == 10
    assert duck.sql("SELECT count(*) FROM lakehouse.bronze.orders_raw").fetchone() == (22,)


@pytest.fixture(scope="session")
def twin(spark_tables, monkeypatch_session):
    """The pandas twin of the same bronze, published by PyIceberg next to Spark's gold."""
    monkeypatch_session.setenv("CHURN_SAMPLE_DIR", CHURN_SAMPLE)
    return light_demo.churn()


@pytest.fixture(scope="session")
def monkeypatch_session():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_spark_churn_gold_equals_the_pandas_twin(twin):
    """Spark SQL gold (Spark-written) vs pandas gold (PyIceberg-written), joined in DuckDB.

    A NEW DuckDB connection: one attached before PyIceberg replaced the twin table fails with an
    internal error in duckdb-iceberg 1.5.6 ("Metadata-log exists but none of the entries were valid
    for the current transaction start time"). Attach after writes from other engines.
    """
    import light_demo as ld

    duck = lc.duckdb_connect()
    feats = [c for c in ld._load_twin().FEATURES if c not in ("plan_tier",)]
    cols = ", ".join(f"s.{c}::DOUBLE AS s_{c}, p.{c}::DOUBLE AS p_{c}" for c in feats)
    df = duck.sql(f"""SELECT s.user_id, s.route AS s_route, p.route AS p_route, s.outcome AS s_outcome,
                             p.outcome AS p_outcome, s.churned AS s_churned, p.churned AS p_churned,
                             s.plan_tier AS s_plan, p.plan_tier AS p_plan, {cols}
                      FROM lakehouse.gold.churn_renewal_features s
                      FULL JOIN lakehouse.gold.churn_renewal_features_twin p USING (user_id)""").df()
    assert len(df) == twin["renewals"], "same renewals on both sides"
    assert df["s_route"].notna().all() and df["p_route"].notna().all(), "no renewal only on one side"
    for c in ("route", "outcome", "churned", "plan"):
        assert (df[f"s_{c}"].astype(str) == df[f"p_{c}"].astype(str)).all(), c
    worst = {c: float(np.nanmax(np.abs(df[f"s_{c}"] - df[f"p_{c}"]))) for c in feats}
    print("max |spark - pandas| per feature:", {k: v for k, v in worst.items() if v})
    assert max(worst.values()) <= 1e-4 + 1e-12, worst   # 4-decimal rounding, as scripts/check_gold_parity.py


def test_spark_exports_meet_the_retention_radar_contract(spark_tables):
    p = subprocess.run([sys.executable, str(REPO / "scripts/check_churn_export.py"), "--strict"],
                       cwd=REPO, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr


@trino
def test_trino_reads_the_same_gold_and_time_travels(spark_tables):
    gold = trino_query("SELECT CAST(order_date AS varchar), orders, round(revenue, 2), round(avg_order_value, 2) "
                       "FROM lakehouse.gold.daily_order_metrics ORDER BY 1")
    assert [tuple(r) for r in gold] == light_demo.RETAIL_GOLD
    t = spark_tables.load_table("bronze.orders_raw")
    sid = [s for s in t.metadata.snapshots if (s.summary or {}).get("ldl.batch") == "orders_day1"][-1].snapshot_id
    assert trino_query(f"SELECT count(*) FROM lakehouse.bronze.orders_raw FOR VERSION AS OF {sid}") == [[10]]


@trino
def test_trino_route_counts_equal_duckdb(spark_tables):
    duck = lc.duckdb_connect()
    q = "SELECT route, count(*) FROM lakehouse.gold.churn_renewal_features GROUP BY route ORDER BY route"
    assert [tuple(r) for r in trino_query(q)] == duck.sql(q).fetchall()
