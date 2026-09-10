-- Raw landing table. One file / one business_date maps to one partition.
-- Load with WRITE_TRUNCATE on that partition so a rerun replaces the day.

CREATE SCHEMA IF NOT EXISTS `{{project}}.{{dataset_raw}}`
OPTIONS (location = "{{location}}");

CREATE TABLE IF NOT EXISTS `{{project}}.{{dataset_raw}}.{{table}}` (
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
  source_file STRING,
  ingested_at TIMESTAMP
)
PARTITION BY business_date
OPTIONS (
  description = "Raw orders. Partitioned by business_date from the file, not by DAG run time."
);
