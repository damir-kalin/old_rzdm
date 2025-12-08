{{
  config(
    materialized='table',
    unique_key='link_ind_data_type_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.link_ind_data_type"
  )
}}

-- Link между показателем и типом данных

WITH hub_ind AS (
  SELECT hub_ind_pk, hub_ind_bk
  FROM {{ ref('hub_ind') }}
),

hub_data_type AS (
  SELECT hub_data_type_pk, hub_data_type_bk
  FROM {{ ref('hub_data_type') }}
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
)

SELECT
  {{ generate_hash(['hi.hub_ind_pk', 'hdt.hub_data_type_pk']) }} as link_ind_data_type_pk,
  hi.hub_ind_pk,
  hdt.hub_data_type_pk,
  s.load_dttm as valid_from,
  CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
  s.load_dttm,
  s.source_nm
FROM staging s
INNER JOIN hub_ind hi ON s.hub_ind_bk = hi.hub_ind_bk
INNER JOIN hub_data_type hdt ON s.data_type = hdt.hub_data_type_bk

