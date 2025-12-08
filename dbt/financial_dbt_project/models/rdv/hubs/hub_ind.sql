{{
  config(
    materialized='table',
    unique_key='hub_ind_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_ind"
  )
}}

WITH staging AS (
  SELECT DISTINCT
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
    ) as hub_ind_bk,
    load_dttm,
    source_nm
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
),

hub AS (
  SELECT
    {{ generate_hash(['hub_ind_bk']) }} as hub_ind_pk,
    hub_ind_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub



