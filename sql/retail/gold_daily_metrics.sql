-- Gold metrics — matches jobs/04_publish_gold.py

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

CREATE OR REPLACE TABLE lakehouse.gold.daily_order_metrics AS
SELECT
  CAST(order_ts AS DATE) AS order_date,
  COUNT(*) AS orders,
  SUM(CASE WHEN status IN ('paid', 'shipped') THEN amount ELSE 0 END) AS revenue,
  AVG(CASE WHEN status IN ('paid', 'shipped') THEN amount END) AS avg_order_value,
  SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
  SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders
FROM lakehouse.silver.orders
GROUP BY CAST(order_ts AS DATE)
ORDER BY order_date;
