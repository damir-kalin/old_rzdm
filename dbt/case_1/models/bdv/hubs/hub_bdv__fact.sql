{{
  config(
    alias='hub_bdv__fact',
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

-- BDV Hub для фактов: RDV + расчётные calc из stg_calc_facts
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH rdv AS (
  SELECT
    hub_fact_pk,
    bk_hub_ind_pk,
    bk_period_dt,
    bk_report_date,
    load_dttm,
    source_nm
  FROM {{ ref('hub_rdv__fact') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

calc_values AS (
  SELECT
    CAST(bk_hub_ind_pk AS VARCHAR) AS ind_code,
    bk_period_dt AS period_dt,
    bk_report_date AS report_date,
    load_dttm,
    source_nm
  FROM {{ ref('stg_calc_facts') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

calc_ind AS (
  SELECT DISTINCT
    MD5(LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    ))) AS hub_ind_pk,
    LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    )) AS hub_ind_bk
  FROM calc_values
),

calc_fact AS (
  SELECT DISTINCT
    {{ generate_hub_fact_pk('ci.hub_ind_pk', 'CAST(cv.period_dt AS CHAR)', 'CAST(cv.report_date AS CHAR)') }} AS hub_fact_pk,
    ci.hub_ind_pk AS bk_hub_ind_pk,
    cv.period_dt AS bk_period_dt,
    cv.report_date AS bk_report_date,
    cv.load_dttm,
    cv.source_nm
  FROM calc_values cv
  JOIN calc_ind ci ON LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      cv.ind_code
    )) = ci.hub_ind_bk
)

SELECT DISTINCT
  hub_fact_pk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm
FROM rdv

UNION ALL

SELECT DISTINCT
  hub_fact_pk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm
FROM calc_fact

