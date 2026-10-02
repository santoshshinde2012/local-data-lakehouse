# Catalog: Apache Iceberg REST catalog (Lakekeeper)

**Selected:** [Lakekeeper](https://github.com/lakekeeper/lakekeeper) **v0.13.6** (Apache-2.0), an Iceberg
REST catalog, with its state in **Postgres 18.6**. Table data and metadata files live in the object store
(RustFS by default, SILO with `STORE=silo`) under `s3://lake/warehouse/`.

## Why a REST catalog

- **One catalog for every engine.** Spark 4.1.3, Trino 483, DuckDB 1.5.6, PyIceberg 0.12.0 and Polars
  1.44.2 (through PyIceberg) all speak the Iceberg REST protocol; no engine needs a JDBC driver or SQL
  access to the catalog database.
- **Vended credentials.** Clients send `X-Iceberg-Access-Delegation: vended-credentials`; Lakekeeper
  answers each table load with short-lived STS credentials (AssumeRole on the object store) and the S3
  endpoint. Only Lakekeeper (and `lakehouse-init`, which creates the bucket) hold the store's keys.
- **Laptop-sized.** One Rust binary (about 21 MiB idle, 30–36 MiB after the demos, measured), plus a
  one-shot `lakekeeper-migrate` service that applies its schema migrations.

The previous design (Iceberg JDBC catalog tables in Postgres 16, root S3 keys in every client) is
described in [MIGRATION.md](../MIGRATION.md).

## Wiring

| Setting | Value | Where |
|---|---|---|
| REST endpoint (containers) | `http://lakekeeper:8181/catalog` | `config/spark-defaults.conf`, `config/trino/catalog/lakehouse.properties`, `docker-compose.graph.yml` |
| REST endpoint (host) | `http://localhost:8181/catalog` (`LAKEKEEPER_PORT`) | `src/lakehouse_client/__init__.py` (`LAKEKEEPER_URL` overrides) |
| Warehouse | `lakehouse` (`LAKEKEEPER_WAREHOUSE`) | `.env.example`; created by `docker/init/bootstrap.sh` |
| Storage | bucket `lake` (`S3_BUCKET`), key prefix `warehouse`, path-style, `sts-enabled` | `docker/init/bootstrap.sh` |
| Endpoint vended to engines | `http://objectstore.localhost:9000` (`S3_ENDPOINT`) | same URL inside `ldl-net` and on the host ([docs/object-store.md](../docs/object-store.md)) |
| Catalog name in engines | `lakehouse` (Spark `spark.sql.defaultCatalog`, Trino catalog, DuckDB `ATTACH … AS lakehouse`, PyIceberg) | |
| Spark FileIO | `org.apache.iceberg.aws.s3.S3FileIO` (iceberg-aws-bundle 1.12.0; no hadoop-aws) | `config/spark-defaults.conf` |
| Authentication | none (local teaching stack; Lakekeeper's API is open on port 8181) | `docker-compose.yml` |

Spark, for example:

```properties
spark.sql.catalog.lakehouse                                  org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type                             rest
spark.sql.catalog.lakehouse.uri                              http://lakekeeper:8181/catalog
spark.sql.catalog.lakehouse.warehouse                        lakehouse
spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation vended-credentials
spark.sql.catalog.lakehouse.io-impl                          org.apache.iceberg.aws.s3.S3FileIO
```

The host client (`lakehouse_client.catalog_properties()`):
`{"type": "rest", "uri": "http://localhost:8181/catalog", "warehouse": "lakehouse",
"header.X-Iceberg-Access-Delegation": "vended-credentials"}`.

## Namespaces and tables

Jobs create their namespaces with `CREATE NAMESPACE IF NOT EXISTS` (the init service creates only the
bucket and the warehouse). Fully qualified names are `lakehouse.<namespace>.<table>`.

| Namespace | Table | Written by |
|---|---|---|
| `bronze` | `smoke_demo` | `src/jobs/retail/01_smoke_test.py` |
| `bronze` | `orders_raw`, `customers_raw` | `src/jobs/retail/02_ingest_bronze.py` (one append per orders file, snapshot property `ldl.batch`); `scripts/light_demo.py` (DuckDB) |
| `bronze` | `churn_subscription_snapshots_raw`, `churn_invoices_raw`, `churn_subscription_events_raw`, `churn_usage_raw`, `churn_limit_events_raw`, `churn_overage_settings_raw`, `churn_overage_charges_raw`, `churn_incidents_raw`, `churn_support_tickets_raw`, `churn_pricing_changes_raw` | `src/jobs/churn/01_ingest_bronze.py` |
| `silver` | `orders`, `customers` | `src/jobs/retail/03_transform_silver.py`; `scripts/light_demo.py` (DuckDB) |
| `silver` | `churn_subscription_snapshots`, `churn_usage_daily`, `churn_invoices`, `churn_subscription_events`, `churn_limit_events`, `churn_overage_settings`, `churn_overage_charges`, `churn_incidents`, `churn_support_tickets`, `churn_pricing_changes` | `src/jobs/churn/02_transform_silver.py` |
| `gold` | `daily_order_metrics` | `src/jobs/retail/04_publish_gold.py`; `scripts/light_demo.py` (DuckDB) |
| `gold` | `churn_renewal_features` | `src/jobs/churn/03_publish_gold_features.py` (Spark owns it; `sql/churn/gold_renewal_features.sql`) |
| `gold` | `churn_renewal_features_twin` | `scripts/light_demo.py` and T3: the pandas twin (`scripts/build_churn_gold_local.py`) published with PyIceberg |
| `gold` | `graph_nodes`, `graph_edges`, `graph_similar_to_scaler`, `graph_build_manifest` | `src/jobs/graph/01_publish_gold_graph.py` (graph overlay; tagged per build, read back by `ldl-graph` by tag + snapshot) |

The light demo writes the retail tables with Spark-compatible types (DuckDB `TIMESTAMPTZ` = Iceberg
`timestamptz` = Spark `TIMESTAMP`), so `make e2e` can refresh the same tables afterwards.

## Engine notes

- **DuckDB 1.5.6:** `ATTACH 'lakehouse' AS lakehouse (TYPE ICEBERG, ENDPOINT 'http://localhost:8181/catalog', …)`.
  A connection attached before another engine replaced a table can fail with "Metadata-log exists but
  none of the entries were valid…": open a fresh connection. DuckDB 2.0 is due 21 Oct 2026 and 1.5 is end
  of life on 1 Nov 2026; re-test the iceberg extension before bumping.
- **Polars 1.44.2:** read with `reader_override="pyiceberg"` (`lakehouse_client.polars_scan`); the native
  reader ignores vended credentials and probes the EC2 metadata service.
- **Trino 483:** `iceberg.catalog.type=rest`, `iceberg.rest-catalog.vended-credentials-enabled=true`,
  native S3 filesystem; no keys in the catalog file.
- **Spark 4.1.3 / Iceberg 1.12.0:** time travel with `VERSION AS OF <snapshot>` in SQL or
  `option("versionAsOf", …)` in the DataFrame API (`option("snapshot-id")` is rejected since 1.11).
- **Vended credentials expire.** If the laptop sleeps during a long job, the job fails with S3 `400 Bad
  Request` and `S3FileIO: Failed to refresh storage credentials … Invalid credentials endpoint: null`
  (Lakekeeper advertises no refresh endpoint). Re-run the job.

## Alternatives (not used here)

- **Iceberg JDBC catalog** (the previous design): no extra service, but every client needs database
  access and root S3 keys, and DuckDB / Trino cannot share it as easily.
- **Apache Polaris 1.8.0:** a JVM REST catalog; about 315 MB idle and a 3 s start with in-memory
  persistence (measured on x86 Linux in a research cross-check, not on this stack). A real setup adds its
  own database.
- **Gravitino, Nessie:** heavier (JVM) REST catalogs for a laptop stack.
- **Hive Metastore:** classic; heavier and not REST.
