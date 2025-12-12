{{ 
  config(
    alias='sat_rdv__ind',
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
-- Атрибуты показателя: name (формируется по алгоритму из всех полей) и code (код показателя)
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH source_data AS (
  SELECT DISTINCT
    s.unit_name,
    s.data_type,
    s.discreteness,
    s.value_type,
    s.org_name,
    s.eat_name,
    s.asset_name,
    s.ind_code,
    s.load_dttm,
    s.source_nm,
    {{ generate_hub_accounting_fin_indicator_bk('s.') }} as name,
    LOWER(s.hub_ind_bk) as hub_ind_bk
  FROM stage.stg_source_data s
  WHERE s.ind_code IS NOT NULL
    AND s.unit_name IS NOT NULL 
    AND s.data_type IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.asset_name IS NOT NULL
    -- eat_name может быть NULL для данных из Формы 2
  {% if is_incremental() %}
    AND s.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

hub_ind_data AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    source_nm
  FROM {{ ref('hub_rdv__ind') }}
),

sat_data AS (
  SELECT
    h.hub_ind_pk,
    s.name,
    s.ind_code as code,
    s.load_dttm as valid_from,
    CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
    s.load_dttm,
    s.source_nm
  FROM source_data s
  INNER JOIN hub_ind_data h ON 
    LOWER(s.hub_ind_bk) = h.hub_ind_bk  -- hub хранит BK в lowercase, sat использует оригинальный регистр
    AND s.source_nm = h.source_nm
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
FROM (
  SELECT
    hub_ind_pk,
    load_dttm,
    name,
    code,
    valid_from,
    valid_to,
    source_nm,
    ROW_NUMBER() OVER (PARTITION BY hub_ind_pk, load_dttm ORDER BY load_dttm DESC) as rn
  FROM sat_data
) t
WHERE rn = 1

