{{
  config(
    materialized='incremental',
    unique_key='hub_ind_pk',
    incremental_strategy='default',
    table_type='PRIMARY',
    keys=['hub_ind_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    on_schema_change='append_new_columns'
  )
}}

-- Hub для показателей
-- Business Key формируется из hub_ind_bk_base и hub_ind_bk_dynamic
-- Append-only: добавляются только новые записи

WITH source_data AS (
  SELECT DISTINCT
    CASE 
      WHEN hub_ind_bk_dynamic IS NOT NULL AND hub_ind_bk_dynamic != '' 
      THEN CONCAT_WS('|', hub_ind_bk_base, hub_ind_bk_dynamic)
      ELSE hub_ind_bk_base
    END as hub_ind_bk_raw,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data_2') }} s
  WHERE hub_ind_bk_base IS NOT NULL
    AND hub_ind_bk_base != ''
  {% if is_incremental() %}
    AND load_dttm > (
      SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
      FROM {{ this }} 
      WHERE source_nm = s.source_nm
    )
  {% endif %}
),

normalized_data AS (
  SELECT
    LOWER(hub_ind_bk_raw) as hub_ind_bk,
    hub_ind_bk_raw,
    load_dttm,
    source_nm
  FROM source_data
),

hub_data AS (
  SELECT
    MD5(hub_ind_bk) as hub_ind_pk,
    hub_ind_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM normalized_data
  GROUP BY hub_ind_bk
)

SELECT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM hub_data

