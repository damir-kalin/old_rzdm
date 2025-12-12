{{ 
  config(
    materialized='incremental',
    unique_key=['hub_fact_pk', 'load_date'],
    table_type='DUPLICATE',
    keys=['hub_fact_pk', 'load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Satellite для фактов
-- Append-only: добавляются только новые записи

WITH hub_fact_data AS (
  SELECT
    hf.hub_fact_pk,
    hf.bk_hub_ind_pk,
    hf.bk_period_dt,
    hf.bk_report_date,
    hf.source_nm,
    hf.load_dttm
  FROM {{ ref('hub_rdv__fact') }} hf
  {% if is_incremental() %}
  WHERE hf.bk_report_date > (
    SELECT COALESCE(MAX(load_date), '1900-01-01') 
    FROM {{ this }} 
    WHERE source_nm = hf.source_nm
  )
  {% endif %}
)

SELECT DISTINCT
  hf.hub_fact_pk,
  hf.bk_report_date as load_date,
  hf.bk_period_dt as period_dt,
  hf.bk_report_date as report_date,
  hf.source_nm
FROM hub_fact_data hf

