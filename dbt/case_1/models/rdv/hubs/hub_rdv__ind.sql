{{
  config(
    alias='hub_rdv__ind',
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

-- Hub для показателей
-- Business Key формируется из составного ключа: hub_unit_pk|hub_data_type_pk|hub_date_type_pk|hub_value_type_pk|hub_organization_pk|hub_economic_activity_type_pk|hub_asset_pk|ind_code
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH source_data AS (
  SELECT
    LOWER(s.hub_ind_bk) as hub_ind_bk_lower,  -- приводим BK к lowercase для единого ключа
    s.load_dttm,
    s.source_nm
  FROM {{ ref('stg_source_data') }} s
  WHERE s.hub_ind_bk IS NOT NULL
  {% if is_incremental() %}
    AND s.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

hub_data AS (
  SELECT
    MD5(s.hub_ind_bk_lower) as hub_ind_pk,  -- hash из lowercase BK
    s.hub_ind_bk_lower as hub_ind_bk,       -- сохраняем BK в lowercase
    s.load_dttm,
    s.source_nm
  FROM source_data s
),

deduplicated_hubs AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    MAX(load_dttm) as load_dttm,
    source_nm  -- Сохраняем source_nm для правильного матчинга
  FROM hub_data
  GROUP BY hub_ind_pk, hub_ind_bk, source_nm  -- Группируем также по source_nm
)

SELECT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs


