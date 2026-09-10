CREATE SCHEMA IF NOT EXISTS `{{project}}.{{dataset_curated}}`
OPTIONS (location = "{{location}}");

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset_curated}}.{{table}}` (
  order_id STRING NOT NULL,
  order_ts TIMESTAMP,
  business_date DATE NOT NULL,
  customer_id STRING,
  sku STRING,
  qty INT64,
  unit_price NUMERIC,
  amount NUMERIC,
  currency STRING,
  country STRING,
  status STRING,
  amount_ok BOOL,
  source_file STRING,
  curated_at TIMESTAMP
)
PARTITION BY business_date
OPTIONS (
  description = "Curated orders. One business_date partition is rebuilt per run."
);
