

-- BDV Satellite для значений фактов: RDV + расчётные calc из stg_calc_facts

WITH rdv AS (
  SELECT
    hub_fact_pk,
    load_date,
    value_amt,
    source_nm
  FROM `unverified`.`sat_rdv__fact_value`
),

calc_values AS (
  SELECT
    CAST(bk_hub_ind_pk AS VARCHAR) AS ind_code,
    bk_period_dt AS period_dt,
    bk_report_date AS load_date,
    value_amt,
    source_nm
  FROM `stage`.`stg_calc_facts`
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
    
  MD5(
    CONCAT_WS('|', 
      TRIM(REPLACE(COALESCE(CAST(ci.hub_ind_pk AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST(CAST(cv.period_dt AS CHAR) AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST(CAST(cv.load_date AS CHAR) AS CHAR), ''), CHAR(160), ''))
    )
  )
 AS hub_fact_pk,
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