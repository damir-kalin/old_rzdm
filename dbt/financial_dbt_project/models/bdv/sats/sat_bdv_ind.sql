{{
  config(
    alias='sat_bdv__ind',
    materialized='table',
    table_type='DUPLICATE',
    keys=['hub_ind_pk', 'load_dttm'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- BDV Satellite для показателей: RDV + расчётные calc из stg_calc_facts

WITH rdv AS (
  SELECT
    hub_ind_pk,
    load_dttm,
    name,
    code,
    valid_from,
    valid_to,
    source_nm
  FROM {{ ref('sat_rdv_ind') }}
),

calc_ind AS (
  SELECT DISTINCT
    MD5(LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      CAST(bk_hub_ind_pk AS VARCHAR)
    ))) AS hub_ind_pk,
    load_dttm,
    CAST(bk_hub_ind_pk AS VARCHAR) AS name,
    CAST(bk_hub_ind_pk AS VARCHAR) AS code,
    load_dttm AS valid_from,
    CAST('9999-12-31 23:59:59' AS DATETIME) AS valid_to,
    source_nm
  FROM {{ ref('stg_calc_facts') }}
)

SELECT DISTINCT
  hub_ind_pk,
  load_dttm,
  name,
  code,
  valid_from,
  valid_to,
  source_nm
FROM rdv

UNION ALL

SELECT DISTINCT
  hub_ind_pk,
  load_dttm,
  name,
  code,
  valid_from,
  valid_to,
  source_nm
FROM calc_ind

