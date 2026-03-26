{{ config(materialized='view') }}

SELECT
  id,
  project_code,
  country,
  shipment_mode,
  pq_first_sent_to_client_date,
  scheduled_delivery_date,
  delivered_to_client_date,

  -- PII Masking using macros
  {{ mask_email(email) }} AS masked_email,
  {{ mask_phone(phone) }} AS masked_phone,
  {{ mask_aadhaar(aadhaar) }} AS masked_aadhaar,
  {{ mask_dob(dob) }} AS masked_dob,

  -- Derived Fields
  DATE_DIFF(
    CAST(delivered_to_client_date AS DATE),
    CAST(scheduled_delivery_date AS DATE),
    DAY
  ) AS delivery_lag,

  SAFE_DIVIDE(line_item_value, SAFE_CAST(weight_kilograms AS FLOAT64)) AS value_per_kg,

  line_item_value,
  weight_kilograms,
  freight_cost_usd

FROM {{ ref('scms_raw_model') }}
WHERE country IS NOT NULL
  AND shipment_mode IS NOT NULL
  AND delivered_to_client_date IS NOT NULL
  AND scheduled_delivery_date IS NOT NULL
