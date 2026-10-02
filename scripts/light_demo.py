#!/usr/bin/env python3
"""The `light` profile demo: the lakehouse without a JVM (host DuckDB + PyIceberg + Polars).

    make up-light && make demo-light          # = python scripts/light_demo.py all

retail   bronze -> silver -> gold with DuckDB SQL, written as Iceberg tables through the Lakekeeper
         REST catalog (vended credentials). The same tables and the same contract as the Spark
         jobs (src/jobs/retail): bronze 22 rows -> silver 19, gold.daily_order_metrics
         2024-03-01 10 / 424.94 / 60.71 and 2024-03-02 9 / 537.94 / 76.85. Bronze is appended
         day 1 then day 2, so time travel reads an EARLIER snapshot (10 rows) next to the current
         one (22), with DuckDB (AT VERSION), PyIceberg (scan snapshot_id) and Polars.
churn    the pandas twin of the churn gold (scripts/build_churn_gold_local.py) on the bronze CSVs
         in CHURN_SAMPLE_DIR, published with PyIceberg as gold.churn_renewal_features_twin (the
         Spark job owns gold.churn_renewal_features) and summarised by DuckDB.
all      retail then churn.

Every step prints what it checks; a broken contract exits non-zero.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_client import catalog, duckdb_connect, polars_scan  # noqa: E402

SAMPLE = Path(os.environ.get("SAMPLE_DATA_DIR", ROOT / "data/sample"))
VALID_STATUS = ("paid", "shipped", "cancelled", "refunded")
RETAIL_GOLD = [("2024-03-01", 10, 424.94, 60.71), ("2024-03-02", 9, 537.94, 76.85)]


def _check(ok: bool, what: str) -> None:
    print(f"  {'OK  ' if ok else 'FAIL'} {what}")
    if not ok:
        raise SystemExit(f"contract broken: {what}")


def retail_sql(con, db: str = "lakehouse", after_batch=None) -> None:
    """Bronze -> silver -> gold in DuckDB SQL into ``db`` (the attached Iceberg catalog, or a plain
    DuckDB database in the T0 unit test). ``after_batch(name)`` runs after each bronze append."""
    for ns in ("bronze", "silver", "gold"):
        con.sql(f"CREATE SCHEMA IF NOT EXISTS {db}.{ns}")
    # Bronze, typed like the Spark job (Spark TIMESTAMP = Iceberg timestamptz = DuckDB TIMESTAMPTZ),
    # so either engine can run any stage against the same tables.
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze.orders_raw (
          order_id VARCHAR, customer_id VARCHAR, order_ts TIMESTAMPTZ, status VARCHAR, amount DOUBLE,
          _source_file VARCHAR, _ingested_at TIMESTAMPTZ)""")
    con.sql(f"DELETE FROM {db}.bronze.orders_raw")  # full refresh, like 02_ingest_bronze.py
    for batch in ("orders_day1", "orders_day2"):
        path = (SAMPLE / f"{batch}.csv").as_posix()
        con.execute(f"""
            INSERT INTO {db}.bronze.orders_raw
            SELECT order_id, customer_id, CAST(order_ts AS TIMESTAMP)::TIMESTAMPTZ, status,
                   CAST(amount AS DOUBLE), 'file://' || ?, now()
            FROM read_csv(?, header = true, all_varchar = true)""", [path, path])
        if after_batch is not None:
            after_batch(batch)
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze.customers_raw (
          customer_id VARCHAR, name VARCHAR, email VARCHAR, city VARCHAR, signup_date DATE,
          _source_file VARCHAR, _ingested_at TIMESTAMPTZ)""")
    con.sql(f"DELETE FROM {db}.bronze.customers_raw")
    cpath = (SAMPLE / "customers.csv").as_posix()
    con.execute(f"""
        INSERT INTO {db}.bronze.customers_raw
        SELECT customer_id, name, email, city, CAST(signup_date AS DATE), 'file://' || ?, now()
        FROM read_csv(?, header = true, all_varchar = true)""", [cpath, cpath])

    # Silver: latest event per order_id (order_ts, then _ingested_at, then _source_file as a tie-break), status normalised + allow-listed.
    valid = ", ".join(f"'{s}'" for s in VALID_STATUS)
    con.sql(f"DROP TABLE IF EXISTS {db}.silver.orders")
    con.sql(f"""
        CREATE TABLE {db}.silver.orders AS
        SELECT order_id, customer_id, order_ts, status, amount, _source_file, _ingested_at FROM (
          SELECT order_id, customer_id, order_ts, lower(trim(status)) AS status, amount, _source_file,
                 _ingested_at,
                 row_number() OVER (PARTITION BY order_id ORDER BY order_ts DESC, _ingested_at DESC,
                                                   _source_file DESC) AS rn
          FROM {db}.bronze.orders_raw)
        WHERE rn = 1 AND status IN ({valid})""")
    con.sql(f"DROP TABLE IF EXISTS {db}.silver.customers")
    con.sql(f"""
        CREATE TABLE {db}.silver.customers AS
        SELECT customer_id, name, email, city, signup_date, _source_file, _ingested_at FROM (
          SELECT customer_id, trim(name) AS name, lower(trim(email)) AS email, trim(city) AS city,
                 signup_date, _source_file, _ingested_at,
                 row_number() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC, _source_file DESC) AS rn
          FROM {db}.bronze.customers_raw)
        WHERE rn = 1""")
    # Gold: the Spark job's aggregates and types (counts BIGINT, money DOUBLE).
    con.sql(f"DROP TABLE IF EXISTS {db}.gold.daily_order_metrics")
    con.sql(f"CREATE TABLE {db}.gold.daily_order_metrics AS {GOLD_SQL.format(silver=db + '.silver')}")


# The gold aggregate over any silver.orders (also T3: DuckDB recomputes Spark's gold from Spark's silver).
GOLD_SQL = """
        SELECT CAST(order_ts AS DATE) AS order_date,
               COUNT(*)::BIGINT AS orders,
               SUM(CASE WHEN status IN ('paid', 'shipped') THEN amount ELSE 0.0 END) AS revenue,
               AVG(CASE WHEN status IN ('paid', 'shipped') THEN amount END) AS avg_order_value,
               SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)::BIGINT AS cancelled_orders,
               SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END)::BIGINT AS refunded_orders
        FROM {silver}.orders
        GROUP BY 1 ORDER BY 1"""


def retail() -> dict:
    t0 = time.perf_counter()
    con = duckdb_connect()
    cat = catalog()
    snaps = {}

    def remember(batch: str) -> None:
        snaps[batch] = cat.load_table("bronze.orders_raw").current_snapshot().snapshot_id

    retail_sql(con, "lakehouse", after_batch=remember)

    print("=== gold.daily_order_metrics (DuckDB) ===")
    gold = con.sql("""SELECT CAST(order_date AS VARCHAR), orders, round(revenue, 2), round(avg_order_value, 2)
                      FROM lakehouse.gold.daily_order_metrics ORDER BY 1""").fetchall()
    for row in gold:
        print("  ", row)
    n_bronze = con.sql("SELECT count(*) FROM lakehouse.bronze.orders_raw").fetchone()[0]
    n_silver = con.sql("SELECT count(*) FROM lakehouse.silver.orders").fetchone()[0]
    hero = con.sql("""SELECT c.name FROM lakehouse.silver.orders o JOIN lakehouse.silver.customers c
                      USING (customer_id) WHERE o.order_id = 'o-1001'""").fetchone()

    print("=== time travel: bronze.orders_raw ===")
    day1 = snaps["orders_day1"]
    duck_day1 = con.sql(f"SELECT count(*) FROM lakehouse.bronze.orders_raw AT (VERSION => {day1})").fetchone()[0]
    ice_day1 = cat.load_table("bronze.orders_raw").scan(snapshot_id=day1).to_arrow().num_rows
    pl_day1 = polars_scan("bronze.orders_raw", snapshot_id=day1).collect().height
    print(f"   snapshot {day1} (after day 1): DuckDB {duck_day1}, PyIceberg {ice_day1}, Polars {pl_day1}")
    print(f"   current snapshot {snaps['orders_day2']}: {n_bronze} rows")
    pl_gold = polars_scan("gold.daily_order_metrics").sort("order_date").collect()

    print("=== contract ===")
    _check(n_bronze == 22 and n_silver == 19, f"bronze {n_bronze} -> silver {n_silver} (22 -> 19)")
    _check([(d, o, float(r), float(a)) for d, o, r, a in gold] == RETAIL_GOLD, "gold daily metrics")
    _check(hero == ("Santosh Shinde",), "o-1001 belongs to Santosh Shinde (c-01)")
    _check(duck_day1 == ice_day1 == pl_day1 == 10, "time travel to the day-1 snapshot reads 10 rows in 3 engines")
    _check(pl_gold["orders"].to_list() == [10, 9], "Polars reads the same gold")
    secs = time.perf_counter() - t0
    print(f"Retail (light) OK in {secs:.1f} s.")
    return {"bronze": n_bronze, "silver": n_silver, "gold": gold, "day1_snapshot": day1, "seconds": round(secs, 1)}


def _load_twin():
    spec = importlib.util.spec_from_file_location("churn_local", ROOT / "scripts/build_churn_gold_local.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def churn_twin_arrow():
    """The pandas gold of CHURN_SAMPLE_DIR as Arrow with Iceberg-friendly types (us timestamps)."""
    import pyarrow as pa

    twin = _load_twin()
    if not (Path(twin.SAMPLE) / "subscription_snapshots.csv").is_file():
        raise SystemExit(f"light_demo churn: no churn sample in {twin.SAMPLE}: run `make churn-sample` first "
                         f"(or set CHURN_SAMPLE_DIR=data/sample/churn/fixtures/tiny)")
    s = twin.silver()
    g = twin.gold(s, s["snapshots"]["snapshot_date"].max()).sort_values("user_id").reset_index(drop=True)
    table = pa.Table.from_pandas(g, preserve_index=False)
    fields = []
    for f in table.schema:
        if pa.types.is_timestamp(f.type):
            fields.append(pa.field(f.name, pa.date32()))   # feature_as_of / renewal_date are dates
        elif pa.types.is_large_string(f.type) or pa.types.is_string_view(f.type):
            fields.append(pa.field(f.name, pa.string()))
        else:
            fields.append(f)
    return table.cast(pa.schema(fields)), twin.SAMPLE


def churn() -> dict:
    t0 = time.perf_counter()
    table, sample = churn_twin_arrow()
    cat = catalog()
    cat.create_namespace_if_not_exists("gold")
    ident = "gold.churn_renewal_features_twin"
    if cat.table_exists(ident):
        cat.drop_table(ident)
    cat.create_table(ident, schema=table.schema).append(table)
    con = duckdb_connect()
    print(f"=== {ident} (pandas twin of {sample}, written by PyIceberg, read by DuckDB) ===")
    rows = con.sql(f"""SELECT route, outcome, count(*) AS renewals, round(avg(churned), 3) AS lapse_rate
                       FROM lakehouse.{ident} GROUP BY ALL ORDER BY 1, 2""").fetchall()
    for r in rows:
        print("  ", r)
    routes = {}
    for route, _outcome, n, _rate in rows:
        routes[route] = routes.get(route, 0) + n
    _check(sum(routes.values()) == table.num_rows, f"{table.num_rows} renewals published and read back")
    _check(routes.get("score_today") == 1, "exactly one renewal is scored today (sub_maya)")
    secs = time.perf_counter() - t0
    print(f"Churn twin (light) OK in {secs:.1f} s.")
    return {"renewals": table.num_rows, "routes": routes, "seconds": round(secs, 1)}


def main(argv: list[str]) -> int:
    what = argv[0] if argv else "all"
    if what not in ("retail", "churn", "all"):
        print(__doc__)
        return 2
    if what in ("retail", "all"):
        retail()
    if what in ("churn", "all"):
        churn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
