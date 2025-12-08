{{
  config(
    materialized='table',
    unique_key='hub_ind_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.sat_ind"
  )
}}

-- Satellite для показателя (hub_ind)
-- Атрибуты показателя: name и ind_cd (код показателя)

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
),

latest_staging AS (
  SELECT
    hub_ind_bk,
    ind_code,
    load_dttm,
    source_nm,
    ROW_NUMBER() OVER (PARTITION BY hub_ind_bk ORDER BY load_dttm DESC) as rn
  FROM staging
)

SELECT
  h.hub_ind_pk,
  h.hub_ind_bk as name,
  s.ind_code as ind_cd,
  s.load_dttm as valid_from,
  CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
  s.load_dttm,
  s.source_nm
FROM hub h
INNER JOIN latest_staging s ON h.hub_ind_bk = s.hub_ind_bk
WHERE s.rn = 1

