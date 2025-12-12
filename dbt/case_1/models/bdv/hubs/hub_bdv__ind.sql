{{
  config(
    alias='hub_bdv__ind',
    materialized='incremental',
    unique_key='hub_ind_pk',
    table_type='PRIMARY',
    keys=['hub_ind_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- BDV Hub для показателей: RDV + расчётные calc из stg_calc_facts
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH rdv AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    load_dttm,
    source_nm
  FROM {{ ref('hub_rdv__ind') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

calc_values AS (
  SELECT DISTINCT
    CAST(bk_hub_ind_pk AS VARCHAR) AS ind_code,
    load_dttm,
    source_nm
  FROM {{ ref('stg_calc_facts') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
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
      ind_code
    ))) AS hub_ind_pk,
    LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    )) AS hub_ind_bk,
    load_dttm,
    source_nm
  FROM calc_values
)

SELECT DISTINCT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM rdv

UNION ALL

SELECT DISTINCT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM calc_ind

