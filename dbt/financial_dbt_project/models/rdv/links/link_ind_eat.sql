{{
  config(
    materialized='table',
    unique_key='link_ind_eat_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.link_ind_eat"
  )
}}

WITH hub_ind AS (
  SELECT hub_ind_pk, hub_ind_bk
  FROM {{ ref('hub_ind') }}
),

hub_eat AS (
  SELECT hub_eat_pk, hub_eat_bk
  FROM {{ ref('hub_eat') }}
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
  {{ generate_hash(['hi.hub_ind_pk', 'he.hub_eat_pk']) }} as link_ind_eat_pk,
  hi.hub_ind_pk,
  he.hub_eat_pk,
  s.load_dttm as valid_from,
  CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
  s.load_dttm,
  s.source_nm
FROM staging s
INNER JOIN hub_ind hi ON s.hub_ind_bk = hi.hub_ind_bk
INNER JOIN hub_eat he ON s.eat_name = he.hub_eat_bk



