{{
  config(
    materialized='incremental',
    unique_key='link_ind_personnel_category_pk',
    incremental_strategy='default',
    table_type='PRIMARY',
    keys=['link_ind_personnel_category_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    on_schema_change='append_new_columns'
  )
}}

-- Link между показателем и категорией персонала
-- PK формируется как MD5(hub_ind_pk|hub_personnel_category_pk)
-- Append-only: добавляются только новые записи

WITH staging_with_ind AS (
  SELECT 
    s.*,
    hi.hub_ind_pk
  FROM {{ ref('stg_source_data_2') }} s
  INNER JOIN {{ ref('hub_rdv__ind') }} hi ON 
    LOWER(CASE 
      WHEN s.hub_ind_bk_dynamic IS NOT NULL AND s.hub_ind_bk_dynamic != '' 
      THEN CONCAT_WS('|', s.hub_ind_bk_base, s.hub_ind_bk_dynamic)
      ELSE s.hub_ind_bk_base
    END) = hi.hub_ind_bk
  WHERE s.personnel_category IS NOT NULL
    AND s.personnel_category != ''
    AND s.personnel_category != '$$$$'
    {% if is_incremental() %}
    -- Добавляем только новые записи
    AND s.load_dttm > (
      SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
      FROM {{ this }} 
      WHERE source_nm = s.source_nm
    )
    {% endif %}
),

staging_normalized AS (
  SELECT
    TRIM(REPLACE(s.personnel_category, CHAR(160), '')) as personnel_category_norm,
    s.hub_ind_pk,
    MAX(s.load_dttm) as load_dttm,
    ANY_VALUE(s.source_nm) as source_nm
  FROM staging_with_ind s
  GROUP BY 
    TRIM(REPLACE(s.personnel_category, CHAR(160), '')),
    s.hub_ind_pk
),

links_with_hubs AS (
  SELECT
    s.hub_ind_pk,
    hpc.personnel_category_hk as hub_personnel_category_pk,
    s.load_dttm,
    s.source_nm
  FROM staging_normalized s
  INNER JOIN {{ source('unverified_rdv', 'hub_rdv__personnel_category') }} hpc ON LOWER(hpc.fullname) = LOWER(s.personnel_category_norm)
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_personnel_category_pk AS CHAR))) as link_ind_personnel_category_pk,
    hub_ind_pk,
    hub_personnel_category_pk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM links_with_hubs
  GROUP BY hub_ind_pk, hub_personnel_category_pk
)

SELECT
  link_ind_personnel_category_pk,
  hub_ind_pk,
  hub_personnel_category_pk,
  load_dttm,
  source_nm
FROM link_data

