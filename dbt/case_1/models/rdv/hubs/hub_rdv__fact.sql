{{
  config(
    alias='hub_rdv__fact',
    materialized='incremental',
    unique_key='hub_fact_pk',
    table_type='PRIMARY',
    keys=['hub_fact_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Hub для фактов
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH source_data AS (
  SELECT
    s.hub_ind_bk,  -- Используем предварительно сформированный hub_ind_bk из stg_source_data
    s.year,
    s.quarter,
    {{ calculate_period_dt('s.year', 's.quarter') }} as period_dt,
    DATE(s.load_dttm) as report_date,
    s.load_dttm,
    s.source_nm
  FROM stage.stg_source_data s
  WHERE s.ind_code IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.year IS NOT NULL
    AND s.quarter IS NOT NULL
    AND s.unit_name IS NOT NULL
    AND s.data_type IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.asset_name IS NOT NULL
    AND s.hub_ind_bk IS NOT NULL
    -- eat_name может быть NULL для данных из Формы 2
  {% if is_incremental() %}
    AND s.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

source_with_hub_ind AS (
  SELECT
    s.hub_ind_bk,
    hi.hub_ind_pk,  -- Используем hub_ind_pk из hub_rdv__ind
    s.period_dt,
    s.report_date,
    s.load_dttm,
    s.source_nm
  FROM source_data s
  INNER JOIN {{ ref('hub_rdv__ind') }} hi ON 
    s.hub_ind_bk = hi.hub_ind_bk
    AND s.source_nm = hi.source_nm
),

normalized_data AS (
  SELECT
    hub_ind_pk,
    period_dt,
    report_date,
    load_dttm,
    source_nm
  FROM source_with_hub_ind
),

fact_data AS (
  SELECT
    {{ generate_hub_fact_pk('hub_ind_pk', 'CAST(period_dt AS CHAR)', 'CAST(report_date AS CHAR)') }} as hub_fact_pk,
    {{ generate_hub_fact_bk('hub_ind_pk', 'CAST(period_dt AS CHAR)', 'CAST(report_date AS CHAR)') }} as hub_fact_bk,
    hub_ind_pk as bk_hub_ind_pk,
    period_dt as bk_period_dt,
    report_date as bk_report_date,
    load_dttm,
    source_nm
  FROM normalized_data
),

deduplicated_hubs AS (
  SELECT
    hub_fact_pk,
    hub_fact_bk,
    bk_hub_ind_pk,
    bk_period_dt,
    bk_report_date,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM fact_data
  GROUP BY hub_fact_pk, hub_fact_bk, bk_hub_ind_pk, bk_period_dt, bk_report_date
)

SELECT
  hub_fact_pk,
  hub_fact_bk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm
FROM deduplicated_hubs


