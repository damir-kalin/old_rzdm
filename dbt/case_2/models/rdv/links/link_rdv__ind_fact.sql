{{
  config(
    materialized='incremental',
    unique_key='link_ind_fact_pk',
    incremental_strategy='default',
    table_type='PRIMARY',
    keys=['link_ind_fact_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    on_schema_change='append_new_columns'
  )
}}

-- Link между показателем и фактом
-- PK формируется как MD5(hub_ind_pk|hub_fact_pk)
-- Append-only: добавляются только новые записи

WITH hub_fact_data AS (
  SELECT
    hub_fact_pk,
    bk_hub_ind_pk as hub_ind_pk,
    load_dttm,
    source_nm
  FROM {{ ref('hub_rdv__fact') }} hf
  {% if is_incremental() %}
  WHERE hf.load_dttm > (
    SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
    FROM {{ this }} 
    WHERE source_nm = hf.source_nm
  )
  {% endif %}
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_fact_pk AS CHAR))) as link_ind_fact_pk,
    hub_ind_pk,
    hub_fact_pk,
    MAX(load_dttm) as load_dttm,
    MAX(source_nm) as source_nm
  FROM hub_fact_data
  GROUP BY hub_ind_pk, hub_fact_pk
)

SELECT
  link_ind_fact_pk,
  hub_ind_pk,
  hub_fact_pk,
  load_dttm,
  source_nm
FROM link_data

