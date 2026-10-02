"""Host-side clients for the local lakehouse (profile `light` or `full`): PyIceberg, DuckDB, Polars.

Every client talks to the Lakekeeper Iceberg REST catalog and asks for vended credentials, so the
host needs no S3 keys: Lakekeeper returns a short-lived key pair + session token and the S3
endpoint (http://objectstore.localhost:9000; *.localhost is loopback on the host, where the store's
port is published) with every table it loads.

Environment (defaults match .env.example and docker-compose.yml):
  LAKEKEEPER_URL        http://localhost:8181          (the REST endpoint is <url>/catalog)
  LAKEKEEPER_WAREHOUSE  lakehouse

    from lakehouse_client import catalog, duckdb_connect, polars_scan
    t = catalog().load_table("gold.daily_order_metrics")
    con = duckdb_connect()            # ATTACH ... AS lakehouse (TYPE ICEBERG)
    con.sql("SELECT * FROM lakehouse.gold.daily_order_metrics").show()
    polars_scan("gold.daily_order_metrics").collect()
"""
from __future__ import annotations

import os
from typing import Any

CATALOG_NAME = "lakehouse"
DEFAULT_URL = "http://localhost:8181"
DEFAULT_WAREHOUSE = "lakehouse"
VENDED = "vended-credentials"


def catalog_url(url: str | None = None) -> str:
    """REST endpoint of the catalog: <LAKEKEEPER_URL>/catalog."""
    base = (url or os.environ.get("LAKEKEEPER_URL") or DEFAULT_URL).rstrip("/")
    return base if base.endswith("/catalog") else base + "/catalog"


def warehouse(name: str | None = None) -> str:
    return name or os.environ.get("LAKEKEEPER_WAREHOUSE") or DEFAULT_WAREHOUSE


def catalog_properties(url: str | None = None, wh: str | None = None, **extra: Any) -> dict[str, str]:
    """PyIceberg properties of the REST catalog (no credentials: the catalog vends them)."""
    props = {
        "type": "rest",
        "uri": catalog_url(url),
        "warehouse": warehouse(wh),
        "header.X-Iceberg-Access-Delegation": VENDED,
    }
    props.update({k: str(v) for k, v in extra.items() if v is not None})
    return props


def catalog(url: str | None = None, wh: str | None = None, **extra: Any):
    """PyIceberg RestCatalog `lakehouse`.

    On the host the default PyArrowFileIO is used (its curl resolves *.localhost to loopback by
    itself). Inside a container pass py_io_impl="pyiceberg.io.fsspec.FsspecFileIO" (s3fs): there
    *.localhost must go through Docker's DNS to the objectstore alias, which curl short-circuits.
    """
    from pyiceberg.catalog import load_catalog

    io_impl = extra.pop("py_io_impl", None)
    if io_impl:
        extra["py-io-impl"] = io_impl
    return load_catalog(CATALOG_NAME, **catalog_properties(url, wh, **extra))


def duckdb_attach_sql(url: str | None = None, wh: str | None = None, alias: str = CATALOG_NAME) -> str:
    """The ATTACH statement DuckDB's iceberg extension needs for this catalog.

    AUTHORIZATION_TYPE 'none': Lakekeeper runs without authentication here (teaching stack).
    ACCESS_DELEGATION_MODE 'vended_credentials' is DuckDB's default and is spelled out on purpose.
    """
    for value in (catalog_url(url), warehouse(wh), alias):
        if "'" in value:
            raise ValueError(f"refusing a quote in {value!r}")
    return (f"ATTACH '{warehouse(wh)}' AS {alias} (TYPE ICEBERG, ENDPOINT '{catalog_url(url)}', "
            f"AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'vended_credentials')")


def duckdb_connect(url: str | None = None, wh: str | None = None, alias: str = CATALOG_NAME, database: str = ":memory:"):
    """In-memory DuckDB with the iceberg + httpfs extensions loaded and the catalog attached as `alias`."""
    import duckdb

    con = duckdb.connect(database)
    con.sql("SET TimeZone = 'UTC'")
    for ext in ("httpfs", "iceberg"):
        con.sql(f"INSTALL {ext}")
        con.sql(f"LOAD {ext}")
    con.sql(duckdb_attach_sql(url, wh, alias))
    return con


def polars_scan(identifier: str | Any, snapshot_id: int | None = None, url: str | None = None, wh: str | None = None):
    """Polars LazyFrame over an Iceberg table, read through PyIceberg.

    reader_override="pyiceberg": Polars' native Iceberg reader (1.44) ignores the vended
    credentials and probes the EC2 metadata endpoint 169.254.169.254 instead.
    """
    import polars as pl

    table = catalog(url, wh).load_table(identifier) if isinstance(identifier, str) else identifier
    return pl.scan_iceberg(table, snapshot_id=snapshot_id, reader_override="pyiceberg")
