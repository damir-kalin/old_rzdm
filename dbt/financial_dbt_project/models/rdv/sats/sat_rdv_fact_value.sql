{{ 
  config(
    alias='sat_rdv__fact_value',
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

-- Satellite для значений фактов
-- Атрибуты: значение показателя
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
    s.value_current_year,
    DATE(s.load_dttm) as load_date,
    s.load_dttm,
    s.source_nm,
    {{ calculate_period_dt('s.year', 's.quarter') }} as period_dt
  FROM t1_stage.stg_source_data_new s
  WHERE s.value_current_year IS NOT NULL
    AND s.unit_name IS NOT NULL
    AND s.data_type IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.eat_name IS NOT NULL 
    AND s.asset_name IS NOT NULL
    AND s.ind_code IS NOT NULL
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
    s.value_current_year,
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
    s.value_current_year,
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

source_with_fact_pk AS (
  SELECT
    s.*,
    hf.hub_fact_pk
  FROM source_with_hub_ind s
  INNER JOIN hub_fact_data hf ON 
    s.hub_ind_pk = hf.bk_hub_ind_pk
    AND s.period_dt = hf.bk_period_dt
    AND s.load_date = hf.bk_report_date
    AND s.source_nm = hf.source_nm
),

row_counts AS (
  SELECT
    source_nm,
    COUNT(*) as file_row_number
  FROM source_with_fact_pk
  GROUP BY source_nm
)

SELECT DISTINCT
  s.hub_fact_pk,
  s.load_date,
  s.value_current_year as value_amt,
  s.source_nm,
  rc.file_row_number,
  CAST(NULL AS VARCHAR(100)) as file_code,
  s.source_nm as file_name,
  CAST(NULL AS VARCHAR(100)) as ind_source_name,
  CAST(NULL AS VARCHAR(100)) as ind_model_code,
  'rdv' as ind_model_name
FROM source_with_fact_pk s
CROSS JOIN row_counts rc
WHERE s.source_nm = rc.source_nm

