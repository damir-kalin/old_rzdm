{{ 
  config(
    alias='sat_rdv__fact',
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
-- Атрибуты факта: период, дата отчета
-- Полное пересоздание таблицы при каждом запуске

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
    s.year,
    s.quarter,
    DATE(s.load_dttm) as load_date,
    s.load_dttm,
    s.source_nm,
    {{ calculate_period_dt('s.year', 's.quarter') }} as period_dt
  FROM t1_stage.stg_source_data_new s
  WHERE s.ind_code IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.year IS NOT NULL
    AND s.quarter IS NOT NULL
    AND s.unit_name IS NOT NULL
    AND s.data_type IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.eat_name IS NOT NULL 
    AND s.asset_name IS NOT NULL
  {% if is_incremental() %}
    AND s.load_dttm > (
      SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ ref('hub_rdv_fact') }}
    )
  {% endif %}
),

source_normalized AS (
  SELECT
    TRIM(REPLACE(s.unit_name, CHAR(160), '')) as unit_name_norm,
    TRIM(REPLACE(s.data_type, CHAR(160), '')) as data_type_norm,
    TRIM(REPLACE(s.discreteness, CHAR(160), '')) as discreteness_norm,
    TRIM(REPLACE(s.value_type, CHAR(160), '')) as value_type_norm,
    TRIM(REPLACE(s.org_name, CHAR(160), '')) as org_name_norm,
    TRIM(REPLACE(s.eat_name, CHAR(160), '')) as eat_name_norm,
    TRIM(REPLACE(s.asset_name, CHAR(160), '')) as asset_name_norm,
    TRIM(REPLACE(s.ind_code, CHAR(160), '')) as ind_code_norm,
    s.year,
    s.quarter,
    s.load_date,
    s.load_dttm,
    s.source_nm,
    s.period_dt
  FROM source_data s
),

hub_ind_data AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    source_nm
  FROM {{ ref('hub_rdv_ind') }}
),

source_with_hub_ind AS (
  SELECT
    h.hub_ind_pk,
    s.year,
    s.quarter,
    s.load_date,
    s.load_dttm,
    s.source_nm,
    s.period_dt
  FROM source_normalized s
  INNER JOIN hub_ind_data h ON 
    LOWER(CONCAT_WS('|',
      s.unit_name_norm,
      s.data_type_norm,
      s.discreteness_norm,
      s.value_type_norm,
      s.org_name_norm,
      s.eat_name_norm,
      s.asset_name_norm,
      s.ind_code_norm
    )) = h.hub_ind_bk  -- hub хранит BK в lowercase
    AND s.source_nm = h.source_nm
),

hub_fact_data AS (
  SELECT
    hub_fact_pk,
    bk_hub_ind_pk,
    bk_period_dt,
    bk_report_date,
    source_nm
  FROM {{ ref('hub_rdv_fact') }}
),

sat_data AS (
  SELECT
    hf.hub_fact_pk,
    s.period_dt,
    s.load_date as report_date,
    s.load_date,
    s.source_nm
  FROM source_with_hub_ind s
  INNER JOIN hub_fact_data hf ON 
    s.hub_ind_pk = hf.bk_hub_ind_pk
    AND s.period_dt = hf.bk_period_dt
    AND s.load_date = hf.bk_report_date
    AND s.source_nm = hf.source_nm
)

SELECT DISTINCT
  hub_fact_pk,
  load_date,
  period_dt,
  report_date,
  source_nm
FROM sat_data

