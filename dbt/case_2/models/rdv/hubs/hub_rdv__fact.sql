{{
  config(
    materialized='incremental',
    unique_key='hub_fact_pk',
    incremental_strategy='default',
    table_type='PRIMARY',
    keys=['hub_fact_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    on_schema_change='append_new_columns'
  )
}}

-- Hub для фактов
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
  WHERE s.period_dt IS NOT NULL
  {% if is_incremental() %}
    AND s.load_dttm > (
      SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
      FROM {{ this }} 
      WHERE source_nm = s.source_nm
    )
  {% endif %}
),

fact_bk_raw AS (
  SELECT
    hub_ind_pk,
    period_dt,
    COALESCE(report_date, '1900-01-01') as report_date,
    CONCAT_WS('|',
      CAST(hub_ind_pk AS CHAR),
      CAST(period_dt AS CHAR),
      COALESCE(CAST(report_date AS CHAR), '1900-01-01')
    ) as hub_fact_bk_raw,
    load_dttm,
    source_nm
  FROM staging_with_ind
),

fact_data AS (
  SELECT
    MD5(LOWER(hub_fact_bk_raw)) as hub_fact_pk,
    LOWER(hub_fact_bk_raw) as hub_fact_bk,
    hub_ind_pk as bk_hub_ind_pk,
    period_dt as bk_period_dt,
    report_date as bk_report_date,
    MAX(load_dttm) as load_dttm,
    MAX(source_nm) as source_nm
  FROM fact_bk_raw
  GROUP BY 
    hub_ind_pk,
    period_dt,
    report_date,
    hub_fact_bk_raw
)

SELECT
  hub_fact_pk,
  hub_fact_bk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm
FROM fact_data

