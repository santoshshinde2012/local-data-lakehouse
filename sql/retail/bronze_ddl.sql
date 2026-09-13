-- Bronze DDL (Iceberg) — matches jobs/02_ingest_bronze.py
-- Catalog: lakehouse (spark.sql.defaultCatalog)

CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_raw (
  order_id     STRING,
  customer_id  STRING,
  order_ts     TIMESTAMP,
  status       STRING,
  amount       DOUBLE,
  _source_file STRING,
  _ingested_at TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers_raw (
  customer_id  STRING,
  name         STRING,
  email        STRING,
  city         STRING,
  signup_date  DATE,
  _source_file STRING,
  _ingested_at TIMESTAMP
) USING iceberg;
