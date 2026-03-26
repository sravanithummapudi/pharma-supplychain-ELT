-- Final Unified DBT Model: scms_merged_model.sql
-- Description: Cleans, imputes, masks, and enriches SCMS supply chain data with test-aware transformations

{{ config(materialized='view') }}

-- ==========================
-- SECTION 1: RAW IMPORT & INITIAL CLEANING
-- ==========================
WITH raw_data AS (
  SELECT *
  FROM `civic-outlet-463614-m3.POC_dbt_dataset.supply_chain_dataset`
),

-- ==========================
-- SECTION 2: CLEAN WEIGHT & FREIGHT COLUMNS (only numeric values)
-- ==========================
cleaned_data AS (
  SELECT
    *,
    CASE 
      WHEN REGEXP_CONTAINS(Weight_in_Kg, r'^\d+(\.\d+)?$') THEN SAFE_CAST(Weight_in_Kg AS FLOAT64)
      ELSE NULL
    END AS weight_cleaned,

    CASE 
      WHEN REGEXP_CONTAINS(Freight_Cost_in_USD, r'^\d+(\.\d+)?$') THEN SAFE_CAST(Freight_Cost_in_USD AS FLOAT64)
      ELSE NULL
    END AS freight_cleaned
  FROM raw_data
),

-- ==========================
-- SECTION 3: CALCULATE GROUPED + GLOBAL MEDIANS
-- ==========================
grouped_medians AS (
  SELECT
    country,
    shipment_mode,
    APPROX_QUANTILES(weight_cleaned, 2)[OFFSET(1)] AS median_weight,
    APPROX_QUANTILES(freight_cleaned, 2)[OFFSET(1)] AS median_freight
  FROM cleaned_data
  WHERE weight_cleaned IS NOT NULL AND freight_cleaned IS NOT NULL
  GROUP BY country, shipment_mode
),

global_medians AS (
  SELECT
    APPROX_QUANTILES(weight_cleaned, 2)[OFFSET(1)] AS median_weight,
    APPROX_QUANTILES(freight_cleaned, 2)[OFFSET(1)] AS median_freight
  FROM cleaned_data
  WHERE weight_cleaned IS NOT NULL AND freight_cleaned IS NOT NULL
),

-- ==========================
-- SECTION 4: IMPUTE MISSING VALUES (with safe fallback)
-- ==========================
imputed_data AS (
  SELECT
    c.* EXCEPT(weight_cleaned, freight_cleaned, Weight_in_Kg, Freight_Cost_in_USD, line_item_insurance_usd),
    COALESCE(c.weight_cleaned, g.median_weight, gm.median_weight, 1) AS Weight_in_Kg,
    COALESCE(c.freight_cleaned, g.median_freight, gm.median_freight, 1) AS Freight_Cost_in_USD,
    COALESCE(c.line_item_insurance_usd, 0) AS line_item_insurance_usd
  FROM cleaned_data c
  LEFT JOIN grouped_medians g ON c.country = g.country AND c.shipment_mode = g.shipment_mode
  CROSS JOIN global_medians gm
),

-- ==========================
-- SECTION 5: HANDLE NULLS IN TEXT COLUMNS
-- ==========================
null_handled_data AS (
  SELECT
    * EXCEPT(shipment_mode, dosage_form),
    COALESCE(shipment_mode, 'NA') AS shipment_mode,
    COALESCE(dosage_form, 'NA') AS dosage_form
  FROM imputed_data
),

-- ==========================
-- SECTION 6: DROP UNNECESSARY COLUMNS (optional)
-- ==========================
dropped_columns AS (
  SELECT
    * EXCEPT(Item_Description, Molecule_Test_Type)
  FROM null_handled_data
),

-- ==========================
-- SECTION 7: APPLY MASKING USING MACRO
-- ==========================
masked_data AS (
  SELECT
    * EXCEPT(Contact_Name, Contact_Email, Contact_Number),
    {{ mask_column('Contact_Number', 'phone') }} AS Contact_Number,
    {{ mask_column('Contact_Email', 'email') }} AS Contact_Email,
    {{ mask_column('Contact_Name', 'name') }} AS Contact_Name
  FROM dropped_columns
),

-- ==========================
-- SECTION 8: DERIVE DELIVERY LAG
-- ==========================
lagged_data AS (
  SELECT
    *,
    CASE 
      WHEN SAFE_CAST(delivered_to_client_date AS DATE) IS NOT NULL 
        AND SAFE_CAST(scheduled_delivery_date AS DATE) IS NOT NULL THEN
        DATE_DIFF(CAST(delivered_to_client_date AS DATE), CAST(scheduled_delivery_date AS DATE), DAY)
      ELSE NULL
    END AS delivery_lag
  FROM masked_data
)

-- ==========================
-- SECTION 9: FINAL SELECT (filter bad lag, handle value_per_kg)
-- ==========================
SELECT
  *,
  CASE
    WHEN Weight_in_Kg IS NOT NULL AND Weight_in_Kg > 0 AND line_item_value IS NOT NULL THEN line_item_value / Weight_in_Kg
    ELSE 0
  END AS value_per_kg
FROM lagged_data
WHERE delivery_lag IS NULL OR (delivery_lag BETWEEN 0 AND 365)
