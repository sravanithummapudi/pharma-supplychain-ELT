{{ config(materialized='view') }}

SELECT
  country,
  shipment_mode,
  COUNT(*) AS total_shipments,
  ROUND(AVG(delivery_lag), 2) AS avg_delivery_lag,
  SUM(line_item_value) AS total_value,
  ROUND(AVG(value_per_kg), 2) AS avg_value_per_kg
FROM {{ ref('scms_clean_model') }}
GROUP BY country, shipment_mode
