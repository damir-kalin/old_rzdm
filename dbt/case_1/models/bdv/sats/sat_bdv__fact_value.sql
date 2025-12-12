{{
  config(
    alias='sat_bdv__fact_value',
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

-- BDV Satellite для значений фактов: RDV + расчётные calc из stg_calc_facts
-- Incremental модель: добавляются только новые записи с load_date больше максимального в таблице

WITH rdv AS (
  SELECT
    hub_fact_pk,
    load_date,
    value_amt,
    source_nm
  FROM {{ ref('sat_rdv__fact_value') }} s
  {% if is_incremental() %}
    WHERE s.load_date > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
  {% endif %}
),

calc_values AS (
  SELECT
    CAST(bk_hub_ind_pk AS VARCHAR) AS ind_code,
    bk_period_dt AS period_dt,
    bk_report_date AS load_date,
    value_amt,
    source_nm
  FROM {{ ref('stg_calc_facts') }}
  WHERE value_amt IS NOT NULL
),

calc_ind AS (
  SELECT DISTINCT
    LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    )) AS hub_ind_bk,
    MD5(LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    ))) AS hub_ind_pk
  FROM calc_values
),

calc_fact AS (
  SELECT DISTINCT
    {{ generate_hub_fact_pk('ci.hub_ind_pk', 'CAST(cv.period_dt AS CHAR)', 'CAST(cv.load_date AS CHAR)') }} AS hub_fact_pk,
    cv.load_date,
    cv.value_amt,
    cv.source_nm
  FROM calc_values cv
  JOIN calc_ind ci ON ci.hub_ind_bk = LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      cv.ind_code
    ))
  {% if is_incremental() %}
    WHERE cv.load_date > (
      SELECT COALESCE(MAX(load_date), '1900-01-01') 
      FROM {{ this }} t
      WHERE t.source_nm LIKE '%calc%'
    )
  {% endif %}
)

SELECT DISTINCT
  hub_fact_pk,
  load_date,
  value_amt,
  source_nm
FROM rdv

UNION ALL

SELECT DISTINCT
  hub_fact_pk,
  load_date,
  value_amt,
  source_nm
FROM calc_fact

