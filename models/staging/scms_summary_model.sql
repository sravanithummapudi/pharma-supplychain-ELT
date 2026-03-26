-- Summary Model: scms_summary_model.sql
-- Description: Aggregates SCMS supply chain data for dashboard KPIs by country, shipment_mode, vendor, etc.

{{ config(materialized='view') }}

SELECT
  country,
  shipment_mode,
  vendor,
  product_group,
  brand,

  COUNT(*) AS total_shipments,
  SUM(line_item_quantity) AS total_quantity,
  SUM(line_item_value) AS total_value_usd,

  ROUND(AVG(value_per_kg), 2) AS avg_value_per_kg,
  ROUND(AVG(delivery_lag), 2) AS avg_delivery_lag,
  ROUND(AVG(Weight_in_Kg), 2) AS avg_weight_kg,
  ROUND(AVG(Freight_Cost_in_USD), 2) AS avg_freight_cost_usd

FROM {{ ref('scms_merged_model') }}
GROUP BY
  country,
  shipment_mode,
  vendor,
  product_group,
  brand
