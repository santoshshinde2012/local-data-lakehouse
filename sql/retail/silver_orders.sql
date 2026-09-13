-- Silver transforms — matches jobs/03_transform_silver.py

CREATE NAMESPACE IF NOT EXISTS lakehouse.silver;

-- Deduped, typed, validated orders (latest event per order_id)
CREATE OR REPLACE TABLE lakehouse.silver.orders AS
SELECT
  order_id,
  customer_id,
  CAST(order_ts AS TIMESTAMP) AS order_ts,
  LOWER(TRIM(status)) AS status,
  CAST(amount AS DOUBLE) AS amount,
  _source_file,
  _ingested_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_ts DESC, _ingested_at DESC
    ) AS rn
  FROM lakehouse.bronze.orders_raw
)
WHERE rn = 1
  AND LOWER(TRIM(status)) IN ('paid', 'shipped', 'cancelled', 'refunded');

CREATE OR REPLACE TABLE lakehouse.silver.customers AS
SELECT
  customer_id,
  TRIM(name) AS name,
  LOWER(TRIM(email)) AS email,
  TRIM(city) AS city,
  CAST(signup_date AS DATE) AS signup_date,
  _source_file,
  _ingested_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY _ingested_at DESC
    ) AS rn
  FROM lakehouse.bronze.customers_raw
)
WHERE rn = 1;
