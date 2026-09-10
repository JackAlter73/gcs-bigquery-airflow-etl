-- Replace one business_date only.

DELETE FROM `{{project}}.{{dataset_curated}}.{{table}}`
WHERE business_date = DATE "{{business_date}}";

INSERT INTO `{{project}}.{{dataset_curated}}.{{table}}` (
  order_id,
  order_ts,
  business_date,
  customer_id,
  sku,
  qty,
  unit_price,
  amount,
  currency,
  country,
  status,
  amount_ok,
  source_file,
  curated_at
)
SELECT
  order_id,
  order_ts,
  business_date,
  customer_id,
  sku,
  qty,
  unit_price,
  amount,
  UPPER(currency) AS currency,
  UPPER(country) AS country,
  LOWER(status) AS status,
  ABS(amount - (qty * unit_price)) <= 0.01 AS amount_ok,
  source_file,
  CURRENT_TIMESTAMP() AS curated_at
FROM `{{project}}.{{dataset_raw}}.{{table}}`
WHERE business_date = DATE "{{business_date}}"
  AND status IS NOT NULL
  AND qty > 0;
