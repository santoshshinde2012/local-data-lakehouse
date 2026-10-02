"""The lakehouse (Iceberg) as the graph's source, pinned by tag + snapshot id (PHASE 2c).

The Spark job src/jobs/graph/01_publish_gold_graph.py publishes the lakehouse-native twin of the
graph (lakehouse.gold.graph_nodes / graph_edges / graph_similar_to_scaler) and tags every table it
read or wrote ``graph_<build_id>``; gold.graph_build_manifest lists them with their snapshot ids.
``scripts/build_graph_local.py build --source iceberg`` then calls build_from_iceberg(), which

  1. opens catalog ``lakehouse`` with PyIceberg under every safety rule below: the Lakekeeper REST
     catalog of the Compose stack (type rest, vended credentials), or a SqlCatalog (the local
     SQLite harness of scripts/check_graph_parity.py);
  2. resolves the publish (the newest in gold.graph_build_manifest, or --iceberg-tag) and reads
     gold.churn_renewal_features + the ten silver inputs AT THE TAG, each checked against the
     manifest: tag exists and is a tag, tag -> the recorded snapshot id, the snapshot still exists,
     row count = the snapshot's total-records = the manifest's row count. Any mismatch raises
     ProvenanceUnavailable ("provenance unavailable: ..."): never a fallback to the current
     snapshot, never a read by timestamp;
  3. builds the graph with the canonical builder (lakehouse_graph.build.build_tables, unchanged) on
     those frames, so the Parquet has the local path's exact semantics (same schema, writer, ids);
  4. checks the published twin (graph tables at the same tag) against that build: every node and
     edge table equal, SIMILAR_TO with the same edge count and differences only at quantised ties
     (the twin's scaler is fitted in SQL, so its d2 differ in the last bits);
  5. compares with the local path on the profile's bronze CSVs when they are present: byte-identical
     Parquet keeps the bronze identity (business_build_id of the local path; an existing local build
     of the same id gets the provenance attached), anything else (Spark gold drifting from the
     pandas twin, or no local bronze to compare with: a NOTE says so) gets its own identity over the
     Iceberg pins, code, spec, versions and platform (iceberg_identity: never the local CSVs, which
     the build does not read), and the drift is recorded;
  6. writes the build like build.build_profile (lock, temp dir, Ladybug, atomic rename) with
     ``manifest["iceberg"]``: catalog, tag, lakehouse build id, table uuid / snapshot id /
     sequence number / rows of every table read, the twin comparison and the local comparison.

Safety rules (BRIEF 2, verified in apinotes spark-iceberg-harness / docker-airflow-ci):
  * REST (the stack): the catalog URI must be http(s), the warehouse is the Lakekeeper warehouse
    NAME, S3 access comes from the credentials the catalog vends per table (no S3 keys needed), and
    the catalog is probed with list_namespaces before use. The REST API is the only access path: the
    graph never connects to the catalog's database;
  * SQL (harness): catalog name ``lakehouse`` (it is the catalog_name column of iceberg_tables);
  * init_catalog_tables=false passed IN CODE (the environment variable is ignored by PyIceberg);
  * schema_version is never set (v1 ALTERs the catalog Spark owns) and a config that sets it is refused;
  * an s3 warehouse needs a local s3.endpoint (never *.amazonaws.com) AND s3.region, else PyIceberg
    asks real AWS for the bucket's region;
  * both catalog tables are probed before use (with init_catalog_tables=false a dead host, a wrong
    password or a missing grant would otherwise look like an empty catalog);
  * SQLite catalogs (the local harness) open read-only (``file:<path>?mode=ro``) after checking the
    file exists, so a wrong path never creates an empty database.

pyiceberg is imported lazily (requirements-graph-spark*.txt): the comparison helpers at the bottom
(table_diff, similar_diff, gold_drift, ...) need only numpy / pandas / pyarrow and are shared with
scripts/check_graph_parity.py.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import numpy as np
import pandas as pd
import pyarrow as pa

from . import build, spec, store
from . import manifest as mf

CATALOG = "lakehouse"
CATALOG_TABLES = ("iceberg_tables", "iceberg_namespace_properties")
FORBIDDEN_KEYS = ("schema_version", "schema-version")
TAG_RE = re.compile(r"^graph_[0-9a-f]{12}$")
NODES_TABLE = "gold.graph_nodes"
EDGES_TABLE = "gold.graph_edges"
SCALER_TABLE = "gold.graph_similar_to_scaler"
MANIFEST_TABLE = "gold.graph_build_manifest"
GOLD_TABLE = "gold.churn_renewal_features"
# pandas twin silver key -> (silver table, a natural sort key: Iceberg row order is not the CSV order)
SILVER = {
    "snapshots": ("silver.churn_subscription_snapshots", ["subscription_id", "snapshot_date"]),
    "usage": ("silver.churn_usage_daily", ["subscription_id", "activity_date"]),
    "invoices": ("silver.churn_invoices", ["subscription_id", "invoice_date", "attempt"]),
    "sub_events": ("silver.churn_subscription_events", ["subscription_id", "event_date", "event_type"]),
    "limits": ("silver.churn_limit_events", ["subscription_id", "hit_at", "limit_type"]),
    "overage_settings": ("silver.churn_overage_settings", ["subscription_id", "changed_at", "overage"]),
    "overage_charges": ("silver.churn_overage_charges", ["subscription_id", "charged_at", "amount_usd"]),
    "incidents": ("silver.churn_incidents", ["incident_id"]),
    "tickets": ("silver.churn_support_tickets", ["ticket_id"]),
    "pricing": ("silver.churn_pricing_changes", ["change_id"]),
}
PUBLISH_JOB = "src/jobs/graph/01_publish_gold_graph.py"
PUBLISH_SQL = ("sql/graph/nodes.sql", "sql/graph/edges.sql", "sql/graph/similar_to.sql")


class ProvenanceUnavailable(RuntimeError):
    """The catalog, a tag, a snapshot or a row count does not match what the build pins."""

    def __str__(self) -> str:
        return f"provenance unavailable: {super().__str__()}"


# --------------------------------------------------------------------------- catalog
# What a catalog URI may print after the scheme: host (or [IPv6]), port, database; nothing else.
_AUTHORITY = re.compile(r"(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(?::\d+)?(?:/[A-Za-z0-9._$-]*)?")
REDACTED_URI = "<redacted>"


def _redact(uri: str) -> str:
    """A catalog URI without credentials, query or fragment (scheme://host[:port][/db]), or the SQLite
    file path (it holds no credentials).

    Not urlsplit, and not "after the last '@'": a password in the userinfo may hold / : ? # % or @ (nothing
    enforces URL-safe passwords in a SQL catalog URI), and a query parameter may hold an '@' too (``?password=sec@ret``), so neither the first nor
    the last '@' is known to end the userinfo. Every reading is tried: no userinfo, or userinfo up to each
    '@'. A reading counts when what follows is exactly host[:port][/db], then the end, a '?' or a '#'.
    When the readings that count all print the same host[:port][/db], that is the answer; when they
    disagree (or none counts) the URI is ambiguous and nothing but the scheme is printed: a manifest or
    an error message never carries a fragment of what may be a secret."""
    if uri.startswith("sqlite:"):
        return uri
    scheme, sep, rest = uri.partition("://")
    if not sep:
        return "<catalog URI without a scheme>"
    readings = set()
    for at in [-1, *(i for i, ch in enumerate(rest) if ch == "@")]:
        tail = rest[at + 1:]
        m = _AUTHORITY.match(tail)
        if m and (m.end() == len(tail) or tail[m.end()] in "?#"):
            readings.add(m.group())
    return f"{scheme}://{readings.pop() if len(readings) == 1 else REDACTED_URI}"


def _sqlite_readonly(uri: str) -> str:
    """sqlite:////abs/catalog.db -> sqlite:///file:/abs/catalog.db?mode=ro&uri=true (file must exist)."""
    if "mode=ro" in uri:
        path = uri.split("file:", 1)[-1].split("?", 1)[0]
    else:
        path = uri[len("sqlite:///"):]
    if not path.startswith("/"):
        raise ProvenanceUnavailable(f"SQLite catalog URI must hold an absolute path (sqlite:////abs/path): {uri}")
    if not Path(path).is_file():
        raise ProvenanceUnavailable(f"SQLite catalog {path} does not exist (run the Spark publish first)")
    return f"sqlite:///file:{path}?mode=ro&uri=true"


def catalog_config(uri: str | None = None, warehouse: str | None = None, **props: str) -> dict[str, str]:
    """PyIceberg config of catalog ``lakehouse`` (PYICEBERG_CATALOG__LAKEHOUSE__* / .pyiceberg.yaml)
    overridden by the arguments, checked against the safety rules. Raises ProvenanceUnavailable."""
    from pyiceberg.utils.config import Config

    cfg = {str(k): str(v) for k, v in (Config().get_catalog_config(CATALOG) or {}).items()}
    cfg.update({k: str(v) for k, v in props.items() if v is not None})
    if uri:
        cfg["uri"] = uri
    if warehouse:
        cfg["warehouse"] = warehouse
    bad = [k for k in FORBIDDEN_KEYS if k in cfg]
    if bad:
        raise ProvenanceUnavailable(f"refusing to open catalog {CATALOG!r}: {bad[0]} is set (it would ALTER the "
                                    f"catalog Spark owns)")
    kind = cfg.get("type", "sql")
    if kind not in ("sql", "rest"):
        raise ProvenanceUnavailable(f"catalog {CATALOG!r} must be the Iceberg REST catalog (type rest) or a "
                                    f"SqlCatalog (type sql, the local harness), got type {kind!r}")
    if not cfg.get("uri"):
        raise ProvenanceUnavailable(f"catalog {CATALOG!r} is not configured: pass --catalog-uri or set "
                                    f"PYICEBERG_CATALOG__LAKEHOUSE__URI")
    if kind == "rest":
        if not cfg["uri"].startswith(("http://", "https://")):
            raise ProvenanceUnavailable(f"REST catalog URI must be http(s)://...: {_redact(cfg['uri'])}")
        if not cfg.get("warehouse"):
            raise ProvenanceUnavailable(f"REST catalog {CATALOG!r} needs the warehouse name "
                                        f"(PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE, e.g. lakehouse)")
        endpoint = cfg.get("s3.endpoint", "")
        if endpoint and (urlsplit(endpoint).hostname or "").endswith("amazonaws.com"):
            raise ProvenanceUnavailable(f"s3.endpoint {endpoint} is AWS: the lakehouse endpoint must be local")
        # Vended credentials: the catalog hands out short-lived S3 credentials and the endpoint per table.
        cfg.setdefault("header.X-Iceberg-Access-Delegation", "vended-credentials")
        return cfg
    wh = cfg.get("warehouse", "")
    endpoint = cfg.get("s3.endpoint", "")
    if wh.startswith(("s3://", "s3a://", "s3n://")) and not endpoint:
        raise ProvenanceUnavailable(f"warehouse {wh} needs the local s3.endpoint (this stack never talks to AWS)")
    if endpoint:
        host = urlsplit(endpoint).hostname or ""
        if host.endswith("amazonaws.com"):
            raise ProvenanceUnavailable(f"s3.endpoint {endpoint} is AWS: the lakehouse endpoint must be local")
        if not cfg.get("s3.region"):
            raise ProvenanceUnavailable("s3.endpoint is set without s3.region (PyIceberg would ask AWS for the "
                                        "bucket's region): set PYICEBERG_CATALOG__LAKEHOUSE__S3__REGION=us-east-1")
    if cfg["uri"].startswith("sqlite:"):
        cfg["uri"] = _sqlite_readonly(cfg["uri"])
    cfg["init_catalog_tables"] = "false"      # in code: PYICEBERG_..._INIT_CATALOG_TABLES is ignored
    cfg.pop("type", None)
    return cfg


def open_catalog(uri: str | None = None, warehouse: str | None = None, **props: str):
    """PyIceberg catalog ``lakehouse``, probed before use: the REST catalog (Lakekeeper) of the stack,
    or a SqlCatalog over the local harness's SQLite JDBC catalog (read-only). close_catalog() it after."""
    cfg = catalog_config(uri, warehouse, **props)
    if cfg.get("type") == "rest":
        return _open_rest(cfg)
    from pyiceberg.catalog.sql import SqlCatalog
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError

    logging.getLogger("pyiceberg.catalog.sql").setLevel(logging.ERROR)   # "detected a v0 schema" on every open
    try:
        catalog = SqlCatalog(CATALOG, **cfg)
    except SQLAlchemyError as e:
        raise ProvenanceUnavailable(_explain(e, cfg["uri"])) from e
    try:
        with catalog.engine.connect() as conn:
            for table in CATALOG_TABLES:
                conn.execute(text(f"SELECT 1 FROM {table} LIMIT 0"))
    except SQLAlchemyError as e:
        catalog.engine.dispose()
        raise ProvenanceUnavailable(_explain(e, cfg["uri"])) from e
    return catalog


def _open_rest(cfg: dict[str, str]):
    """RestCatalog + a list_namespaces probe: a dead host, a wrong warehouse or a refused token fails
    here with a clear message instead of looking like an empty catalog."""
    from pyiceberg.catalog.rest import RestCatalog

    try:
        catalog = RestCatalog(CATALOG, **cfg)
        catalog.list_namespaces()
    except Exception as e:  # noqa: BLE001 - requests / pyiceberg raise many types; all mean "unusable"
        raise ProvenanceUnavailable(_explain(e, cfg["uri"])) from e
    return catalog


def close_catalog(catalog) -> None:
    """Release what open_catalog() opened (SQLAlchemy pool or HTTP session)."""
    engine = getattr(catalog, "engine", None)
    if engine is not None:
        engine.dispose()
    session = getattr(catalog, "_session", None)
    if session is not None:
        session.close()


def _explain(exc: Exception, uri: str) -> str:
    where = _redact(uri)
    msg = (str(getattr(exc, "orig", exc)).strip().splitlines() or [type(exc).__name__])[0]
    low = msg.lower()
    if ("does not exist" in low and "relation" in low) or "no such table" in low:
        return f"the Iceberg catalog at {where} has no catalog tables yet (run the Spark publish first)"
    if "permission denied" in low:
        return f"the catalog role lacks SELECT on {' and '.join(CATALOG_TABLES)} at {where}"
    if "password authentication failed" in low or ("role" in low and "does not exist" in low):
        return f"catalog login rejected at {where}"
    if "warehouse" in low and ("not found" in low or "does not exist" in low or "404" in low):
        return f"the REST catalog at {where} has no such warehouse (run lakehouse-init / make up-full)"
    return f"cannot reach the Iceberg catalog at {where}: {msg[:200]}"


def catalog_schema(catalog) -> list:
    """What the read must not change, read before and after it.

    SqlCatalog: the catalog database's own schema (tables, columns): proves no ALTER.
    REST: the namespaces and the tables in them (the REST API exposes no database schema; the graph
    only ever calls GET endpoints, so this proves no create / drop happened through this client).
    """
    if getattr(catalog, "engine", None) is None:
        return [[".".join(ns), sorted(".".join(t) for t in catalog.list_tables(ns))]
                for ns in sorted(catalog.list_namespaces())]
    from sqlalchemy import inspect

    insp = inspect(catalog.engine)
    return [[t, [(c["name"], str(c["type"])) for c in insp.get_columns(t)]] for t in sorted(insp.get_table_names())]


# --------------------------------------------------------------------------- pinned reads
def _ident(table: str) -> str:
    """lakehouse.gold.x -> gold.x (PyIceberg identifiers carry no catalog name)."""
    return table[len(CATALOG) + 1:] if table.startswith(CATALOG + ".") else table


def _summary(snap) -> dict:
    s = snap.summary
    extra = getattr(s, "additional_properties", None)
    return dict(extra) if extra is not None else {k: s[k] for k in s}


def read_pinned(catalog, identifier: str, tag: str, snapshot_id: int | None = None,
                expected_rows: int | None = None) -> tuple[pa.Table, dict]:
    """Read ``identifier`` at the snapshot ``tag`` points to, and only if everything matches.

    Raises ProvenanceUnavailable when the table or tag is missing, the ref is not a tag, the tag
    points elsewhere than ``snapshot_id``, the snapshot is gone, or the rows read differ from the
    snapshot's total-records / ``expected_rows``. Never falls back to the current snapshot.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.table.refs import SnapshotRefType

    ident = _ident(identifier)
    try:
        table = catalog.load_table(ident)
    except (NoSuchTableError, NoSuchNamespaceError) as e:
        raise ProvenanceUnavailable(f"table {ident} is not in catalog {CATALOG!r}") from e
    except FileNotFoundError as e:
        raise ProvenanceUnavailable(f"the metadata of {ident} is missing from the warehouse ({e})") from e
    ref = table.refs().get(tag)
    if ref is None:
        raise ProvenanceUnavailable(f"tag {tag} is not on {ident} (never created, dropped, or another build)")
    if ref.snapshot_ref_type != SnapshotRefType.TAG:
        raise ProvenanceUnavailable(f"{tag} on {ident} is a {ref.snapshot_ref_type}, not a tag")
    if snapshot_id is not None and ref.snapshot_id != int(snapshot_id):
        raise ProvenanceUnavailable(f"tag {tag} on {ident} points at snapshot {ref.snapshot_id}, but the build "
                                    f"pins {snapshot_id}")
    snap = table.snapshot_by_id(ref.snapshot_id)
    if snap is None:
        raise ProvenanceUnavailable(f"snapshot {ref.snapshot_id} of {ident} (tag {tag}) has been expired")
    data = table.scan(snapshot_id=snap.snapshot_id).to_arrow()
    summary = _summary(snap)
    total = int(summary.get("total-records", -1))
    if data.num_rows != total:
        raise ProvenanceUnavailable(f"{ident}@{tag}: read {data.num_rows:,} rows, the snapshot records {total:,}")
    if expected_rows is not None and data.num_rows != int(expected_rows):
        raise ProvenanceUnavailable(f"{ident}@{tag}: {data.num_rows:,} rows, the build manifest says "
                                    f"{int(expected_rows):,}")
    return data, {"table": f"{CATALOG}.{ident}", "table_uuid": str(table.metadata.table_uuid), "tag": tag,
                  "snapshot_id": int(snap.snapshot_id), "sequence_number": int(snap.sequence_number),
                  "timestamp_ms": int(snap.timestamp_ms), "rows": data.num_rows,
                  "operation": str(getattr(snap.summary, "operation", "")).rsplit(".", 1)[-1].lower(),
                  "spark_app_id": summary.get("spark.app.id")}


@dataclass
class Publish:
    """One publish of the Spark graph job, as gold.graph_build_manifest records it (at its tag)."""
    build_id: str
    tag: str
    tables: dict[str, dict] = field(default_factory=dict)   # lakehouse.<ns>.<t> -> {role, snapshot_id, rows, parts}
    info: dict = field(default_factory=dict)                 # spec, scaler_kind, code_sha256, spark_*, published_at
    manifest: dict = field(default_factory=dict)             # provenance of the manifest table read

    def pin(self, table: str) -> dict:
        key = table if table.startswith(CATALOG + ".") else f"{CATALOG}.{table}"
        if key not in self.tables:
            raise ProvenanceUnavailable(f"publish {self.tag} does not list {key} in {CATALOG}.{MANIFEST_TABLE}")
        return self.tables[key]


def resolve_publish(catalog, tag: str | None = None) -> Publish:
    """The publish to read: ``tag`` (graph_<12 hex>), or the newest one recorded in the manifest.

    The newest publish is looked up in the manifest table's current snapshot (only its tag name is
    taken from there); everything is then read at that tag and checked against the manifest rows
    read AT THE TAG.
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    if tag is None:
        try:
            current = catalog.load_table(MANIFEST_TABLE).scan(
                selected_fields=("build_id", "tag", "published_at")).to_arrow()
        except (NoSuchTableError, NoSuchNamespaceError) as e:
            raise ProvenanceUnavailable(f"{CATALOG}.{MANIFEST_TABLE} does not exist: run the Spark publish "
                                        f"(src/jobs/graph/01_publish_gold_graph.py) first") from e
        if current.num_rows == 0:
            raise ProvenanceUnavailable(f"{CATALOG}.{MANIFEST_TABLE} records no publish yet")
        df = current.to_pandas().sort_values(["published_at", "build_id"], kind="mergesort")
        tag = str(df["tag"].iloc[-1])
    if not TAG_RE.match(tag or ""):
        raise ProvenanceUnavailable(f"{tag!r} is not a graph build tag (graph_<12 hex>)")
    data, prov = read_pinned(catalog, MANIFEST_TABLE, tag)
    rows = [r for r in data.to_pylist() if r["tag"] == tag]
    if not rows:
        raise ProvenanceUnavailable(f"{CATALOG}.{MANIFEST_TABLE} at {tag} holds no row for that build")
    first = rows[0]
    pub = Publish(build_id=first["build_id"], tag=tag, manifest=prov, info={
        "spec": {"graph": first["spec_graph"], "similar_to": first["spec_similar_to"]},
        "scaler_kind": first["scaler_kind"], "code_sha256": json.loads(first["code_sha256"] or "{}"),
        "spark_version": first["spark_version"], "spark_app_id": first["spark_app_id"],
        "published_at": str(first["published_at"])})
    for r in rows:
        if r["table_name"] in pub.tables:
            raise ProvenanceUnavailable(f"{MANIFEST_TABLE} lists {r['table_name']} twice for {tag}")
        pub.tables[r["table_name"]] = {"role": r["role"], "snapshot_id": int(r["snapshot_id"]),
                                       "rows": int(r["row_count"]),
                                       "parts": json.loads(r["parts"]) if r["parts"] else None}
    want = {"graph": spec.GRAPH_SPEC_VERSION, "similar_to": spec.SIMILAR_TO_SPEC_VERSION}
    if pub.info["spec"] != want:
        raise ProvenanceUnavailable(f"{tag} was published with spec {pub.info['spec']}, this checkout reads {want}")
    return pub


def to_twin_frame(arrow: pa.Table, sort: list[str]) -> pd.DataFrame:
    """Iceberg Arrow -> the dtypes scripts/build_churn_gold_local.silver() / gold() produce from CSV.

    DATE -> datetime64 (midnight), TIMESTAMP (UTC) -> naive, decimal -> float via float(Decimal)
    (pyarrow's decimal -> float64 cast is off by one ulp on about a third of 4-decimal values),
    INT -> int64; rows sorted by ``sort``.
    """
    unit = pd.to_datetime(pd.Series(["2026-01-01"])).dtype    # what read_csv + to_datetime give here
    cols = {}
    for f in arrow.schema:
        col = arrow.column(f.name)
        if pa.types.is_date(f.type):
            days = np.asarray(col.to_numpy(zero_copy_only=False)).astype("datetime64[us]")
            cols[f.name] = pd.Series(days).astype(unit)
        elif pa.types.is_timestamp(f.type):
            s = col.to_pandas()
            s = s.dt.tz_convert("UTC").dt.tz_localize(None) if getattr(s.dt, "tz", None) is not None else s
            cols[f.name] = s.astype(unit)
        elif pa.types.is_decimal(f.type):
            cols[f.name] = pd.Series([None if v is None else float(v) for v in col.to_pylist()], dtype="float64")
        elif pa.types.is_integer(f.type):
            cols[f.name] = col.cast(pa.int64()).to_pandas()
        else:
            cols[f.name] = col.to_pandas()
    return pd.DataFrame(cols).sort_values(sort, kind="mergesort").reset_index(drop=True)


def read_inputs(catalog, pub: Publish) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, list[dict]]:
    """(silver frames keyed like build_churn_gold_local.silver(), gold frame, provenance) at the tag."""
    prov, silver = [], {}
    for key, (table, sort) in SILVER.items():
        pin = pub.pin(table)
        arrow, p = read_pinned(catalog, table, pub.tag, pin["snapshot_id"], pin["rows"])
        silver[key] = to_twin_frame(arrow, sort)
        prov.append({**p, "role": "input"})
    pin = pub.pin(GOLD_TABLE)
    arrow, p = read_pinned(catalog, GOLD_TABLE, pub.tag, pin["snapshot_id"], pin["rows"])
    prov.append({**p, "role": "input"})
    gold = to_twin_frame(arrow, ["user_id"]).drop(columns=["built_at"], errors="ignore")
    return silver, gold, prov


def _split(wide: pa.Table, kind_col: str, name: str, schema: pa.Schema) -> pa.Table:
    import pyarrow.compute as pc

    part = wide.filter(pc.equal(wide.column(kind_col), name))
    arrays = []
    for f in schema:
        if f.name not in part.column_names:
            raise ProvenanceUnavailable(f"the twin has no column {f.name} for {name}")
        a = part.column(f.name).cast(f.type)
        if not f.nullable and a.null_count:
            raise ProvenanceUnavailable(f"the twin's {name}.{f.name} holds {a.null_count} nulls")
        arrays.append(a)
    return pa.Table.from_arrays(arrays, schema=schema)


def read_twin(catalog, pub: Publish) -> tuple[dict[str, pa.Table], pd.DataFrame, list[dict]]:
    """The published graph tables at the tag, split into the builder's per-label / per-type schemas."""
    out, prov = {}, []
    for table, kind_col, specs in ((NODES_TABLE, "label", spec.NODE_SCHEMA), (EDGES_TABLE, "rel_type",
                                                                                   spec.EDGE_SCHEMA)):
        pin = pub.pin(table)
        wide, p = read_pinned(catalog, table, pub.tag, pin["snapshot_id"], pin["rows"])
        prov.append({**p, "role": "output"})
        for name, s in specs.items():
            out[name] = _split(wide, kind_col, name, s.schema)
        got = {name: out[name].num_rows for name in specs}
        want = {name: int((pin.get("parts") or {}).get(name, 0)) for name in specs}
        if got != want:
            raise ProvenanceUnavailable(f"{table}@{pub.tag}: rows per {kind_col} {got} != manifest {want}")
    pin = pub.pin(SCALER_TABLE)
    arrow, p = read_pinned(catalog, SCALER_TABLE, pub.tag, pin["snapshot_id"], pin["rows"])
    prov.append({**p, "role": "output"})
    order = {f: i for i, f in enumerate(spec.FEATURES)}
    scaler = arrow.to_pandas()
    scaler = scaler.assign(_o=scaler["feature"].map(order)).sort_values("_o").drop(columns="_o").reset_index(drop=True)
    return out, scaler[list(spec.SCALER_SCHEMA.names)], prov


# --------------------------------------------------------------------------- comparisons (shared)
def _sorted(tbl: pa.Table, keys: list[str]) -> pa.Table:
    return tbl.sort_by([(k, "ascending") for k in keys])


def _cells(col: pa.ChunkedArray) -> np.ndarray:
    """Comparable cells: float64 as their int64 bit patterns (NaN one pattern), the rest as objects."""
    if pa.types.is_floating(col.type):
        x = np.asarray(col.to_numpy(zero_copy_only=False), dtype=np.float64)
        return np.where(np.isnan(x), np.int64(0x7FF8000000000001), x.view(np.int64))
    return np.array(col.to_pylist(), dtype=object)


def table_diff(local: pa.Table, twin: pa.Table, keys: list[str]) -> dict:
    """Cell-by-cell difference after sorting both by ``keys`` then every column (floats bit for bit)."""
    order = keys + [c for c in local.column_names if c not in keys]
    a, b = _sorted(local, order), _sorted(twin, order)
    out: dict = {"rows": [a.num_rows, b.num_rows], "columns": {}}
    if a.num_rows != b.num_rows or a.column_names != b.column_names:
        out["columns"] = {"__shape__": {"local": [a.num_rows, a.column_names], "twin": [b.num_rows, b.column_names]}}
        return out
    for c in a.column_names:
        bad = _cells(a.column(c)) != _cells(b.column(c))
        if bad.any():
            i = int(np.argmax(bad))
            out["columns"][c] = {"cells": int(bad.sum()), "example": {
                "key": {k: a.column(k)[i].as_py() for k in keys}, "local": a.column(c)[i].as_py(),
                "twin": b.column(c)[i].as_py()}}
    return out


def table_keys(name: str) -> list[str]:
    if name in spec.NODE_SCHEMA:
        return [spec.NODE_SCHEMA[name].key]
    return ["src", "rank"] if name == "SIMILAR_TO" else ["src", "dst"]


def numpy_pair_d2q(renewals: pd.DataFrame, scaler: pd.DataFrame, pairs: list[tuple[str, str]]) -> dict:
    """numpy d2_q of arbitrary (src, dst) pairs with ``scaler`` (the builder's arithmetic)."""
    if not pairs:
        return {}
    ren = renewals.sort_values("renewal_id", kind="mergesort").reset_index(drop=True)
    z = build.zscore(ren, scaler)
    pos = {r: i for i, r in enumerate(ren["renewal_id"])}
    src = np.array([pos[s] for s, _ in pairs])
    dst = np.array([pos[d] for _, d in pairs])
    d2 = np.zeros(len(pairs), dtype=np.float64)
    for f in range(z.shape[1]):   # left to right in feature order, like build.knn
        diff = z[src, f] - z[dst, f]
        d2 = d2 + diff * diff
    return dict(zip(pairs, spec.d2_quantise(d2).tolist(), strict=True))


def similar_diff(local: pd.DataFrame, twin: pd.DataFrame, renewals: pd.DataFrame, scaler: pd.DataFrame) -> dict:
    """SIMILAR_TO differences: edge set, ranks, d2 bits, and whether each source differs only at ties.

    Per source the two ranked dst lists are compared position by position; a position that holds
    different dsts is a tie when both dsts have numpy d2_q (``scaler``) within 1 quantum of each other.
    A source is tie-only when every differing position is a tie and both lists have the same length.
    """
    a = local.set_index(["src", "dst"])
    b = twin.set_index(["src", "dst"])
    only_local = sorted(set(a.index) - set(b.index))
    only_twin = sorted(set(b.index) - set(a.index))
    common = a.index.intersection(b.index)
    x = a.loc[common, "d2"].to_numpy(np.float64)
    y = b.loc[common, "d2"].to_numpy(np.float64)
    rank_diff = int((a.loc[common, "rank"].to_numpy() != b.loc[common, "rank"].to_numpy()).sum())
    q_diff = int((a.loc[common, "d2_q"].to_numpy() != b.loc[common, "d2_q"].to_numpy()).sum())
    la = local.sort_values(["src", "rank"], kind="mergesort").groupby("src", sort=True)["dst"].apply(list)
    lb = twin.sort_values(["src", "rank"], kind="mergesort").groupby("src", sort=True)["dst"].apply(list)
    affected, not_tie, checks = [], set(), []
    for s in sorted(set(la.index) | set(lb.index)):
        da, db = la.get(s, []), lb.get(s, [])
        if da == db:
            continue
        affected.append(s)
        if len(da) != len(db):
            not_tie.add(s)
            continue
        checks += [(s, u, v) for u, v in zip(da, db, strict=True) if u != v]
    q = numpy_pair_d2q(renewals, scaler, sorted({(s, d) for s, u, v in checks for d in (u, v)}))
    not_tie |= {s for s, u, v in checks if abs(q[(s, u)] - q[(s, v)]) > 1}
    return {"edges": [len(local), len(twin)], "only_local": len(only_local), "only_twin": len(only_twin),
            "rank_differences": rank_diff, "sources_affected": len(affected),
            "d2_bits_differ": int((x.view(np.int64) != y.view(np.int64)).sum()),
            "max_abs_d2_delta": float(np.max(np.abs(x - y))) if len(x) else 0.0, "d2_q_differ": q_diff,
            "tie_only": not not_tie, "sources_not_tie_only": sorted(not_tie)[:20],
            "examples": {"only_local": [list(p) for p in only_local[:3]],
                         "only_twin": [list(p) for p in only_twin[:3]]}}


def scaler_deltas(a: pd.DataFrame, b: pd.DataFrame) -> dict:
    """Per-feature scaler differences (a = numpy / persisted, b = the other), bit-equal counts."""
    x, y = a.set_index("feature"), b.set_index("feature").reindex(a["feature"])
    xm, ym = x["mean"].to_numpy(np.float64), y["mean"].to_numpy(np.float64)
    xs, ys = x["std"].to_numpy(np.float64), y["std"].to_numpy(np.float64)
    return {"features": len(x), "mean_bit_equal": int((xm.view(np.int64) == ym.view(np.int64)).sum()),
            "std_bit_equal": int((xs.view(np.int64) == ys.view(np.int64)).sum()),
            "max_abs_mean_delta": float(np.nanmax(np.abs(xm - ym))),
            "max_abs_std_delta": float(np.nanmax(np.abs(xs - ys))),
            "n_ref": [int(x["n_ref"].iloc[0]), int(y["n_ref"].iloc[0])]}


def gold_drift(other_gold: pd.DataFrame, pandas_gold: pd.DataFrame) -> dict:
    """Per-feature max |other - pandas| by user_id (Spark decimals via float(Decimal)); label cells too."""
    a = other_gold.set_index("user_id").sort_index()
    b = pandas_gold.set_index("user_id").reindex(a.index)
    out, cells = {}, 0
    for f in spec.NUMERIC_FEATURES:
        x = np.array([float(v) for v in a[f]], dtype=np.float64)
        y = b[f].to_numpy(dtype=np.float64)
        n = int((x.view(np.int64) != y.view(np.int64)).sum())
        cells += n
        if n:
            out[f] = {"cells": n, "max_abs_delta": float(np.nanmax(np.abs(x - y)))}
    labels = {c: int((a[c].astype(str).to_numpy() != b[c].astype(str).to_numpy()).sum())
              for c in ("outcome", "route", "churned")}
    return {"rows": [len(a), len(pandas_gold)], "cells_differ": cells, "by_feature": out,
            "label_cells_differ": {k: v for k, v in labels.items() if v},
            "max_abs_delta": max((v["max_abs_delta"] for v in out.values()), default=0.0)}


def local_arrow(tables: dict) -> dict[str, pa.Table]:
    """The builder's node / edge frames as Arrow with the spec schemas (build.to_arrow)."""
    out = {label: build.to_arrow(tables[label], ns.schema) for label, ns in spec.NODE_SCHEMA.items()}
    out.update({rel: build.to_arrow(tables[rel], es.schema) for rel, es in spec.EDGE_SCHEMA.items()})
    return out


def compare_twin(tables: dict, twin: dict[str, pa.Table], twin_scaler: pd.DataFrame) -> dict:
    """The published twin vs the builder's tables on the same inputs: every node / edge table equal,
    SIMILAR_TO same count with differences only at quantised ties (the twin fits its scaler in SQL)."""
    local = local_arrow(tables)
    diffs = {n: table_diff(local[n], twin[n], table_keys(n)) for n in local if n != "SIMILAR_TO"}
    bad = {n: {c: x.get("cells", x) for c, x in d["columns"].items()} for n, d in diffs.items() if d["columns"]}
    sim = similar_diff(tables["SIMILAR_TO"], twin["SIMILAR_TO"].to_pandas(), tables["Renewal"],
                       tables["similar_to_scaler"])
    counts_equal = all(local[n].num_rows == twin[n].num_rows for n in local)
    ok = not bad and counts_equal and sim["tie_only"] and sim["edges"][0] == sim["edges"][1]
    return {"ok": ok, "counts_equal": counts_equal, "tables_equal": len(local) - 1 - len(bad),
            "tables_differ": bad, "similar_to": sim, "scaler": scaler_deltas(tables["similar_to_scaler"], twin_scaler)}


# --------------------------------------------------------------------------- build
def _check_guard(before: dict) -> None:
    """Non-interference: the user's bronze and exports are byte-identical to before the build."""
    after = mf.guarded_hashes()
    if after != before:
        changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
        raise build.GraphBuildError(f"non-interference violated: guarded files changed during the build: {changed}")


def _max_rss_bytes() -> int:
    import resource
    import sys

    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(r if sys.platform == "darwin" else r * 1024)   # macOS: bytes; Linux: KiB


def _code_status(pub: Publish) -> dict:
    root = spec.repo_root()
    now = {rel: (mf.sha256_file(root / rel) if (root / rel).is_file() else "missing")
           for rel in (PUBLISH_JOB, *PUBLISH_SQL)}
    return {"published": pub.info.get("code_sha256", {}), "checkout": now,
            "matches_checkout": pub.info.get("code_sha256", {}) == now}


def identity_params() -> dict:
    """The SIMILAR_TO parameters every identity hashes (the ``params`` of manifest.build_identity)."""
    return {"k": spec.K, "quant": spec.QUANT, "block": spec.BLOCK, "features": list(spec.FEATURES),
            "reference_route": spec.REFERENCE_ROUTE}


def iceberg_identity(sample_dir: Path | None, pins: dict[str, int]) -> dict:
    """Identity of a build whose content is NOT the local path's: sha256 over the Iceberg input pins
    (table -> snapshot id), the content code + this module, spec versions, SIMILAR_TO parameters,
    package versions and platform: what the build read and what shaped it.

    The local bronze CSVs are not part of it, and none is read here: the build never reads them for
    its content (only the comparison with the local path and the contract's gold-drift record do, and
    the manifest keeps their sha256 under ``inputs`` as information). So the same tag, pins and code
    give the same id on every host, with or without a sample dir. ``sample_dir`` is accepted for the
    callers' signature (manifest.current_identity, verify_build) and not used.
    """
    del sample_dir
    payload = {"code": {**mf.code_hashes(), "src/lakehouse_graph/iceberg_source.py": mf.sha256_file(Path(__file__))},
               "spec": dict(spec.SPEC_VERSIONS), "params": identity_params(), "versions": mf.package_versions(),
               "platform": mf.platform_tag(), "iceberg_inputs": dict(sorted(pins.items()))}
    return {"business_build_id": mf.sha256_json(payload)[:12], "payload": payload}


def _manifest_ident(ident: dict, sample_dir: Path) -> dict:
    """``ident`` as manifest.assemble_manifest reads it: an Iceberg identity gets the local bronze sha256
    as ``inputs`` (information for the contract: no part of the id); a bronze identity has them already."""
    if "inputs" in ident["payload"]:
        return ident
    return {**ident, "payload": {**ident["payload"], "inputs": mf.input_hashes(sample_dir)}}


def _local_path(sdir: Path, tables: dict, iceberg_gold: pd.DataFrame, scratch: Path, log) -> dict:
    """The local (CSV) path on the same profile, Parquet sha256 against the Iceberg-sourced tables."""
    if not all((sdir / f).is_file() for f in spec.BRONZE_FILES):
        where = mf.display_path(sdir)
        log(f"    NOTE: no bronze CSVs in {where}: the comparison with the local path and the gold-drift record are "
            f"skipped, so the build gets its own identity over the Iceberg pins and the source-aware contract can "
            f"neither apply nor derive a golden for it (it warns); pass --sample-dir with the profile's bronze to "
            f"compare")
        return {"compared": False, "reason": f"no bronze CSVs in {where}"}
    t0 = time.perf_counter()
    mod = build.load_gold_twin(sdir)
    s, g, today = build.run_gold(mod)
    local = build.build_tables(s, g, today, build.gold_constants(mod))
    mine = build.write_tables(tables, scratch / "iceberg")
    theirs = build.write_tables(local, scratch / "local")
    differ = sorted(k for k in mine if mine[k]["sha256"] != theirs[k]["sha256"])
    drift = gold_drift(iceberg_gold, g)
    out = {"compared": True, "byte_identical": not differ, "files_differ": differ, "gold_drift": drift,
           "seconds": round(time.perf_counter() - t0, 2)}
    if differ:
        graph_tables = {**spec.NODE_SCHEMA, **spec.EDGE_SCHEMA}
        cells = {}
        for name, rel, schema in build.table_files():
            if rel in differ and name in graph_tables:
                d = table_diff(build.to_arrow(tables[name], schema), build.to_arrow(local[name], schema),
                               table_keys(name))
                cells[name] = {c: x.get("cells", x) for c, x in d["columns"].items()}
        out["cells_differ"] = cells
        out["similar_to_vs_local"] = similar_diff(local["SIMILAR_TO"], tables["SIMILAR_TO"], local["Renewal"],
                                                  local["similar_to_scaler"])
        log(f"    NOTE: the Iceberg inputs do not reproduce the local build byte for byte ({len(differ)} files: "
            f"{', '.join(differ[:4])}{'...' if len(differ) > 4 else ''}); Spark gold vs pandas gold: "
            f"{drift['cells_differ']} cells, max |delta| {drift['max_abs_delta']:.3g}. This build gets its own "
            f"identity over the Iceberg input pins; the source-aware contract reports the gold drift (info when "
            f"it is rounding, a warning beyond) and re-reads the pins for its determinism check")
    return out


def build_from_iceberg(profile: str = "default", graph_root: str | os.PathLike | None = None, *,
                       tag: str | None = None, catalog_uri: str | None = None, warehouse: str | None = None,
                       sample_dir: str | os.PathLike | None = None, export_dir: str | os.PathLike | None = None,
                       rebuild: bool = False, verify_seed: bool = False, lock_timeout: float = 600.0,
                       catalog_props: dict | None = None, log=print) -> tuple[Path, dict]:
    """Build one profile from the Iceberg lakehouse pinned by tag + snapshot (see the module docstring).

    Raises ProvenanceUnavailable (catalog / tag / snapshot / row count), build.GraphBuildError (the
    twin disagrees with the build, determinism, locking) or TimeoutError.
    """
    t_start = time.perf_counter()
    spec.check_profile(profile)
    root = spec.graph_root(graph_root)
    sdir = Path(sample_dir).absolute() if sample_dir else spec.sample_dir(profile, root)
    edir = Path(export_dir).absolute() if export_dir else spec.export_dir(profile, root)
    guard_before = mf.guarded_hashes()
    catalog = open_catalog(catalog_uri, warehouse, **(catalog_props or {}))
    schema_before = catalog_schema(catalog)
    pub = resolve_publish(catalog, tag)
    log(f"==> graph build from Iceberg: profile {profile}, {CATALOG}.{MANIFEST_TABLE} -> {pub.tag} (published "
        f"{pub.info['published_at']}, Spark {pub.info['spark_version']}, {len(pub.tables)} tables pinned)")
    t0 = time.perf_counter()
    silver, gold, prov_in = read_inputs(catalog, pub)
    twin, twin_scaler, prov_out = read_twin(catalog, pub)
    read_s = time.perf_counter() - t0
    if catalog_schema(catalog) != schema_before:
        raise ProvenanceUnavailable("the Iceberg catalog's own schema changed while it was read")
    close_catalog(catalog)
    code = _code_status(pub)
    if not code["matches_checkout"]:
        log(f"    NOTE: {pub.tag} was published by other job / SQL bytes than this checkout's")
    today = silver["snapshots"]["snapshot_date"].max()
    try:
        tables = build.build_tables(silver, gold, today, build.gold_constants(build.load_gold_twin(sdir)))
    except (KeyError, ValueError, TypeError) as e:
        raise build.GraphBuildError(f"the Iceberg inputs at {pub.tag} are not what the builder reads "
                                    f"({type(e).__name__}: {str(e)[:300]})") from e
    twin_report = compare_twin(tables, twin, twin_scaler)
    sim = twin_report["similar_to"]
    if not twin_report["ok"]:
        raise build.GraphBuildError(
            f"the lakehouse twin at {pub.tag} disagrees with the graph built from the same Iceberg inputs: tables "
            f"differ {twin_report['tables_differ'] or 'none'}; SIMILAR_TO {sim['only_local']} edges only local / "
            f"{sim['only_twin']} only twin, tie-only {sim['tie_only']} (sources {sim['sources_not_tie_only'][:5]})")
    log(f"    twin check: {twin_report['tables_equal']} node/edge tables equal cell for cell; SIMILAR_TO "
        f"{sim['edges'][0]:,} = {sim['edges'][1]:,} edges, {sim['only_local']} set differences, "
        f"{sim['rank_differences']} rank differences (tie-only {sim['tie_only']}), d2 last-bit differences "
        f"{sim['d2_bits_differ']:,} (max {sim['max_abs_d2_delta']:.2e}; the twin's scaler is fitted in SQL)")
    diagnostics = build.similar_to_diagnostics(tables["SIMILAR_TO"], tables["similar_to_cut"])
    counts = build.graph_counts(tables)
    pins = {p["table"]: p["snapshot_id"] for p in prov_in}
    iceberg = {"catalog": CATALOG, "catalog_type": "rest" if getattr(catalog, "engine", None) is None else "sql",
               "catalog_uri": _redact(str(getattr(catalog, "properties", {}).get("uri", ""))),
               "warehouse": str(getattr(catalog, "properties", {}).get("warehouse", "")), "tag": pub.tag,
               "lakehouse_build_id": pub.build_id, "published_at": pub.info["published_at"],
               "spark_version": pub.info["spark_version"], "spark_app_id": pub.info["spark_app_id"],
               "scaler_kind": pub.info["scaler_kind"], "code": code, "manifest_table": pub.manifest,
               "tables": {p["table"]: p for p in (*prov_in, *prov_out)}, "catalog_schema_unchanged": True,
               "read_s": round(read_s, 2), "twin": twin_report}
    with store.BuildLock(root, timeout=lock_timeout, log=log):
        scratch = spec.builds_dir(profile, root) / f".iceberg-cmp-{os.getpid()}"
        shutil.rmtree(scratch, ignore_errors=True)
        try:
            iceberg["local_path"] = _local_path(sdir, tables, gold, scratch, log)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        same = iceberg["local_path"].get("byte_identical") is True
        ident = mf.build_identity(sdir) if same else iceberg_identity(sdir, pins)
        iceberg["identity"] = "bronze (byte-identical to the local path)" if same else "iceberg inputs"
        if not same:   # what verify_build() (or a source-aware contract) recomputes the id from
            iceberg["identity_payload"] = ident["payload"]
        bid = ident["business_build_id"]
        bdir = spec.builds_dir(profile, root) / bid
        for name in store.restore_orphans(spec.builds_dir(profile, root)):
            log(f"    restored build {name}: an interrupted replacement had left it in {store.TRASH_PREFIX}*")
        tmp = spec.builds_dir(profile, root) / f".tmp-{bid}-{os.getpid()}"
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        try:
            files = build.write_tables(tables, tmp)
            build_s = time.perf_counter() - t_start
            if bdir.exists() and not rebuild:
                old = mf.read_manifest(bdir)
                old_files = {k: v["sha256"] for k, v in old.get("files", {}).items()}
                if old_files != {k: v["sha256"] for k, v in files.items()}:
                    raise build.GraphBuildError(f"determinism violation: {bdir} exists with the same business_build_id "
                                                f"but different Parquet bytes (rerun with --rebuild to replace it)")
                if (bdir / store.DB_FILE).is_file():
                    shutil.rmtree(tmp)
                    man = build.repin(bdir, old, profile=profile, sample_dir=sdir, export_dir=edir, graph_root=root,
                                      guard=guard_before, verify_seed=verify_seed, log=log)
                    # The same keys as a fresh Iceberg build (the source-aware contract reads "iceberg").
                    man = {**man, "source": "iceberg", "iceberg": iceberg, "repinned_at": mf.utc_now()}
                    mf.write_manifest(bdir, man)
                    store.update_link(spec.latest_link(profile, root), bdir)
                    log(f"    unchanged: the Parquet from {pub.tag} is byte-identical to the existing build {bid}; "
                        f"kept it and recorded the Iceberg provenance in its manifest")
                    _check_guard(guard_before)
                    return bdir, man
            ld = build.load_ladybug_subprocess(tmp / "parquet", tmp / store.DB_FILE)
            want = {**counts["nodes"], **counts["edges"]}
            bad = {k: (want[k], ld["counts"].get(k)) for k in want if ld["counts"].get(k) != want[k]}
            if bad:
                raise build.GraphBuildError(f"Ladybug counts differ from Parquet counts: {bad}")
            seed = mf.seed_info(profile, sdir, root, verify=verify_seed, scratch=spec.builds_dir(profile, root))
            man = mf.assemble_manifest(
                ident=_manifest_ident(ident, sdir), profile=profile, sample_dir=sdir, export_dir=edir, files=files,
                counts=counts, today=today, diagnostics=diagnostics, seed=seed, guard=guard_before, ladybug=ld,
                builder={"seconds": round(build_s, 2), "max_rss_bytes": _max_rss_bytes()},
                built_at=mf.utc_now())
            man = {**man, "source": "iceberg", "iceberg": iceberg}
            mf.write_manifest(tmp, man)
            _check_guard(guard_before)
            if bdir.exists():
                held = store.live_pids(bdir)
                if held:
                    raise build.GraphBuildError(f"replacing build {bid} refused: held by live pid(s) {held}")
                kept, _dropped, keys = build._carry_over(bdir, tmp, files)
                if keys:
                    man = {**man, **keys}
                    mf.write_manifest(tmp, man)
                how = store.replace_dir(tmp, bdir)
                log(f"    replaced the existing build {bid} atomically ({how}); kept {', '.join(kept) or 'nothing'}")
            else:
                os.replace(tmp, bdir)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        store.update_link(spec.latest_link(profile, root), bdir)
    log(f"    {counts['total_nodes']:,} nodes / {counts['total_edges']:,} edges from {pub.tag} "
        f"({len(iceberg['tables'])} tables read by tag + snapshot in {read_s:.2f} s); identity: {iceberg['identity']}; "
        f"Ladybug {ld['db_bytes'] / 1e6:.1f} MB -> {bdir}")
    return bdir, man


def verify_build(build_dir: str | os.PathLike, catalog_uri: str | None = None, warehouse: str | None = None,
                 catalog_props: dict | None = None) -> dict:
    """Re-read an Iceberg-sourced build's inputs at its recorded tag + snapshot ids, rebuild, compare.

    The determinism check for ``--source iceberg`` builds: the same pins and code must give the same
    Parquet bytes. Raises ProvenanceUnavailable when a tag moved, a snapshot expired or a row count
    changed since the build (the build can then no longer be reproduced from the lakehouse).
    """
    import tempfile

    bdir = Path(build_dir)
    man = mf.read_manifest(bdir)
    ice = man.get("iceberg")
    if not ice:
        raise ValueError(f"{bdir} holds no Iceberg provenance (built from CSV)")
    catalog = open_catalog(catalog_uri, warehouse, **(catalog_props or {}))
    pub = resolve_publish(catalog, ice["tag"])
    for table, p in ice["tables"].items():
        pin = pub.pin(table)
        if pin["snapshot_id"] != p["snapshot_id"] or pin["rows"] != p["rows"]:
            raise ProvenanceUnavailable(f"{table}: the manifest at {ice['tag']} now pins snapshot {pin['snapshot_id']} "
                                        f"({pin['rows']} rows), the build read {p['snapshot_id']} ({p['rows']} rows)")
    silver, gold, _prov = read_inputs(catalog, pub)
    close_catalog(catalog)
    sdir = mf.resolve_path(man["inputs"]["sample_dir"])
    tables = build.build_tables(silver, gold, silver["snapshots"]["snapshot_date"].max(),
                                build.gold_constants(build.load_gold_twin(sdir)))
    with tempfile.TemporaryDirectory(prefix=".iceberg-verify-", dir=bdir.parent) as td:
        files = build.write_tables(tables, Path(td))
    differ = sorted(k for k in set(files) | set(man["files"])
                    if files.get(k, {}).get("sha256") != man["files"].get(k, {}).get("sha256"))
    payload = ice.get("identity_payload")
    now = iceberg_identity(sdir, payload["iceberg_inputs"]) if payload is not None else mf.build_identity(sdir)
    fresh = now["business_build_id"] == man["business_build_id"]
    return {"tag": ice["tag"], "byte_identical": not differ, "files_differ": differ, "fresh": fresh}


def provenance_line(man: dict) -> str:
    """One line for CLIs: tag, lakehouse build id and the gold snapshot the build was read at."""
    ice = man.get("iceberg") or {}
    gold = (ice.get("tables") or {}).get(f"{CATALOG}.{GOLD_TABLE}", {})
    return (f"iceberg tag {ice.get('tag')} (lakehouse build {ice.get('lakehouse_build_id')}), "
            f"{CATALOG}.{GOLD_TABLE} snapshot {gold.get('snapshot_id')} (table uuid {gold.get('table_uuid')}), "
            f"identity {ice.get('identity')}")
