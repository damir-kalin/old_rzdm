{{ 
  config(
    alias='link_bdv__ind_fact',
    materialized='incremental',
    unique_key='link_ind_fact_pk',
    table_type='PRIMARY',
    keys=['link_ind_fact_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Link между показателем и фактом в BDV слое
-- PK формируется как MD5(hub_ind_pk|hub_fact_pk)
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH hub_fact_data AS (
  SELECT
    hub_fact_pk,
    bk_hub_ind_pk as hub_ind_pk,
    load_dttm,
    source_nm
  FROM {{ ref('hub_bdv__fact') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_fact_pk AS CHAR))) as link_ind_fact_pk,
    hub_ind_pk,
    hub_fact_pk,
    load_dttm,
    source_nm
  FROM hub_fact_data
),

deduplicated_links AS (
  SELECT
    link_ind_fact_pk,
    hub_ind_pk,
    hub_fact_pk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM link_data
  GROUP BY link_ind_fact_pk, hub_ind_pk, hub_fact_pk
)

SELECT
  link_ind_fact_pk,
  hub_ind_pk,
  hub_fact_pk,
  load_dttm,
  source_nm
FROM deduplicated_links







