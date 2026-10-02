"""T0: host client helpers and the light-profile SQL contract on plain DuckDB (no catalog)."""
from __future__ import annotations

import duckdb
import light_demo
import pytest
from support.ldl import env_example

import lakehouse_client as lc


def test_catalog_properties_ask_for_vended_credentials_and_carry_no_keys(monkeypatch):
    monkeypatch.delenv("LAKEKEEPER_URL", raising=False)
    monkeypatch.delenv("LAKEKEEPER_WAREHOUSE", raising=False)
    props = lc.catalog_properties()
    assert props == {"type": "rest", "uri": "http://localhost:8181/catalog", "warehouse": "lakehouse",
                     "header.X-Iceberg-Access-Delegation": "vended-credentials"}
    assert props["warehouse"] == env_example()["LAKEKEEPER_WAREHOUSE"]
    monkeypatch.setenv("LAKEKEEPER_URL", "http://127.0.0.1:18181/")
    assert lc.catalog_url() == "http://127.0.0.1:18181/catalog"
    assert lc.catalog_url("http://x:1/catalog") == "http://x:1/catalog"


def test_duckdb_attach_statement():
    sql = lc.duckdb_attach_sql("http://localhost:8181", "lakehouse")
    assert sql == ("ATTACH 'lakehouse' AS lakehouse (TYPE ICEBERG, ENDPOINT 'http://localhost:8181/catalog', "
                   "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'vended_credentials')")
    with pytest.raises(ValueError):
        lc.duckdb_attach_sql("http://x'; DROP", "lakehouse")


def test_retail_medallion_sql_meets_the_contract_on_plain_duckdb():
    """The exact SQL the light demo runs against Iceberg, here on an in-memory DuckDB database."""
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")
    batches = []
    light_demo.retail_sql(con, "memory", after_batch=lambda b: batches.append(
        (b, con.sql("SELECT count(*) FROM memory.bronze.orders_raw").fetchone()[0])))
    assert batches == [("orders_day1", 10), ("orders_day2", 22)]
    assert con.sql("SELECT count(*) FROM memory.silver.orders").fetchone()[0] == 19
    gold = con.sql("""SELECT CAST(order_date AS VARCHAR), orders, round(revenue, 2), round(avg_order_value, 2),
                             cancelled_orders, refunded_orders FROM memory.gold.daily_order_metrics ORDER BY 1""").fetchall()
    assert gold == [("2024-03-01", 10, 424.94, 60.71, 2, 1), ("2024-03-02", 9, 537.94, 76.85, 1, 1)]
    dedup = con.sql("SELECT status FROM memory.silver.orders WHERE order_id IN ('o-1003', 'o-1006') ORDER BY order_id")
    assert dedup.fetchall() == [("shipped",), ("cancelled",)]
    assert con.sql("SELECT count(*) FROM memory.silver.orders WHERE status = 'pending'").fetchone()[0] == 0
    hero = con.sql("""SELECT c.name FROM memory.silver.orders o JOIN memory.silver.customers c USING (customer_id)
                      WHERE order_id = 'o-1001'""").fetchone()
    assert hero == ("Santosh Shinde",)


def test_churn_twin_arrow_has_iceberg_friendly_types(monkeypatch):
    import pyarrow as pa
    from support.ldl import REPO

    monkeypatch.setenv("CHURN_SAMPLE_DIR", str(REPO / "data/sample/churn/fixtures/tiny"))
    table, _sample = light_demo.churn_twin_arrow()
    assert table.num_rows == 121
    for f in table.schema:
        assert not pa.types.is_timestamp(f.type) or f.type.unit == "us", f
        assert not pa.types.is_large_string(f.type), f
    routes = dict(zip(*[table.group_by("route").aggregate([("route", "count")]).column(c).to_pylist()
                        for c in ("route", "route_count")]))
    assert routes.get("score_today") == 1
