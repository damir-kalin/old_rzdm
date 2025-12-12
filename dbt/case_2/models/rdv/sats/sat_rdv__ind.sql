{{ 
  config(
    materialized='incremental',
    unique_key=['hub_ind_pk', 'load_dttm'],
    table_type='DUPLICATE',
    keys=['hub_ind_pk', 'load_dttm'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Satellite для показателей (hub_ind)
-- Append-only: добавляются только новые записи
-- Бизнес-ключи сохраняются в оригинальном виде (как в источнике)

WITH hub_ind_data AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    load_dttm,
    source_nm
  FROM {{ ref('hub_rdv__ind') }} h
  {% if is_incremental() %}
  WHERE h.load_dttm > (
    SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
    FROM {{ this }} 
    WHERE source_nm = h.source_nm
  )
  {% endif %}
),

source_data AS (
  SELECT DISTINCT
    CASE 
      WHEN hub_ind_bk_dynamic IS NOT NULL AND hub_ind_bk_dynamic != '' 
      THEN CONCAT_WS('|', hub_ind_bk_base, hub_ind_bk_dynamic)
      ELSE hub_ind_bk_base
    END as hub_ind_bk_original,
    LOWER(CASE 
      WHEN hub_ind_bk_dynamic IS NOT NULL AND hub_ind_bk_dynamic != '' 
      THEN CONCAT_WS('|', hub_ind_bk_base, hub_ind_bk_dynamic)
      ELSE hub_ind_bk_base
    END) as hub_ind_bk_lower,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data_2') }}
  WHERE hub_ind_bk_base IS NOT NULL
    AND hub_ind_bk_base != ''
),

sat_data AS (
  SELECT
    h.hub_ind_pk,
    h.load_dttm,
    ANY_VALUE(s.hub_ind_bk_original) as name,
    CAST(NULL AS VARCHAR(100)) as code,
    '1900-01-01' as valid_from,
    CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
    h.source_nm
  FROM hub_ind_data h
  INNER JOIN source_data s ON s.hub_ind_bk_lower = h.hub_ind_bk
  GROUP BY h.hub_ind_pk, h.load_dttm, h.source_nm
)

SELECT
  hub_ind_pk,
  load_dttm,
  MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(load_dttm AS CHAR))) as sat_ind_pk,
  name,
  code,
  valid_from,
  valid_to,
  source_nm
FROM sat_data

