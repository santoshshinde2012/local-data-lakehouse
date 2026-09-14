# Catalog choice for this repo

**Selected:** Apache Iceberg **JDBC catalog** backed by **Postgres**.

## Why JDBC + Postgres

- Easy to run in Docker Compose (one `postgres` service).
- Works with Spark’s Iceberg runtime without a separate REST process.
- Warehouse data still lives in Silo (S3-compatible) at `s3a://lake/warehouse`; Postgres only stores Iceberg metadata pointers (namespaces, table metadata locations).

## Spark wiring

See `spark-defaults.conf`:

- Catalog name: `lakehouse` (also `spark.sql.defaultCatalog`)
- `catalog-impl`: `org.apache.iceberg.jdbc.JdbcCatalog`
- JDBC URI: `jdbc:postgresql://postgres:5432/iceberg`
- Warehouse: `s3a://lake/warehouse`

## Namespaces / tables used by jobs

| Namespace | Tables |
|-----------|--------|
| `bronze`  | `orders_raw`, `customers_raw` |
| `silver`  | `orders`, `customers` |
| `gold`    | `daily_order_metrics` |

Fully qualified example: `lakehouse.bronze.orders_raw` (or `bronze.orders_raw` when default catalog is set).

## Alternatives (not used here)

- **Iceberg REST catalog** — nicer multi-engine story; add a REST service later if you introduce Trino/DuckDB readers.
- **Hive Metastore** — classic; heavier for a teaching laptop.
- **Hadoop catalog** (filesystem-only) — simplest, but weaker multi-process coordination.
