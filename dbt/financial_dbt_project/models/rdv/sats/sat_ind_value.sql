{{
  config(
    materialized='table',
    unique_key='hub_ind_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.sat_ind_value"
  )
}}

-- Satellite для показателя (hub_ind) с значениями
-- Атрибуты: period_dt (период) и value_amt (значение)

WITH hub AS (
  SELECT hub_ind_pk, hub_ind_bk
  FROM {{ ref('hub_ind') }}
),

staging AS (
  SELECT DISTINCT
    unit_name,
    data_type,
    metric,
    discreteness,
    value_type,
    org_name,
    eat_name,
    asset_name,
    ind_code,
    quarter,
    year,
    value_current_year,
    load_dttm,
    source_nm,
    CONCAT_WS('|', 
      COALESCE(CAST(unit_name AS CHAR), ''),
      COALESCE(CAST(data_type AS CHAR), ''),
      COALESCE(CAST(metric AS CHAR), ''),
      COALESCE(CAST(discreteness AS CHAR), ''),
      COALESCE(CAST(value_type AS CHAR), ''),
      COALESCE(CAST(org_name AS CHAR), ''),
      COALESCE(CAST(eat_name AS CHAR), ''),
      COALESCE(CAST(asset_name AS CHAR), ''),
      COALESCE(CAST(ind_code AS CHAR), '')
    ) as hub_ind_bk
  FROM {{ ref('stg_source_data') }}
  WHERE unit_name IS NOT NULL 
    AND data_type IS NOT NULL
    AND metric IS NOT NULL
    AND discreteness IS NOT NULL
    AND value_type IS NOT NULL
    AND org_name IS NOT NULL 
    AND eat_name IS NOT NULL 
    AND asset_name IS NOT NULL
    AND ind_code IS NOT NULL
    AND quarter IS NOT NULL
    AND year IS NOT NULL
    AND value_current_year IS NOT NULL
),

staging_with_date AS (
  SELECT
    hub_ind_bk,
    quarter,
    year,
    value_current_year,
    load_dttm,
    source_nm,
    CASE 
      WHEN quarter = 1 THEN STR_TO_DATE(CONCAT(year, '-01-01'), '%Y-%m-%d')
      WHEN quarter = 2 THEN STR_TO_DATE(CONCAT(year, '-04-01'), '%Y-%m-%d')
      WHEN quarter = 3 THEN STR_TO_DATE(CONCAT(year, '-07-01'), '%Y-%m-%d')
      WHEN quarter = 4 THEN STR_TO_DATE(CONCAT(year, '-10-01'), '%Y-%m-%d')
      ELSE STR_TO_DATE(CONCAT(year, '-01-01'), '%Y-%m-%d')
    END as period_dt
  FROM staging
)

SELECT
  h.hub_ind_pk,
  s.period_dt,
  s.value_current_year as value_amt,
  s.load_dttm,
  s.source_nm
FROM hub h
INNER JOIN staging_with_date s ON h.hub_ind_bk = s.hub_ind_bk

