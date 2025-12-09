

-- Hub для фактов
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
    
  CASE 
    WHEN s.quarter = 1 THEN STR_TO_DATE(CONCAT(s.year, '-01-01'), '%Y-%m-%d')
    WHEN s.quarter = 2 THEN STR_TO_DATE(CONCAT(s.year, '-04-01'), '%Y-%m-%d')
    WHEN s.quarter = 3 THEN STR_TO_DATE(CONCAT(s.year, '-07-01'), '%Y-%m-%d')
    WHEN s.quarter = 4 THEN STR_TO_DATE(CONCAT(s.year, '-10-01'), '%Y-%m-%d')
    ELSE STR_TO_DATE(CONCAT(s.year, '-01-01'), '%Y-%m-%d')
  END
 as period_dt,
    DATE(s.load_dttm) as report_date,
    s.load_dttm,
    s.source_nm
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
  
),

normalized_data AS (
  SELECT
    -- формируем BK показателя в lowercase, чтобы хэши были стабильны между слоями
    LOWER(CONCAT_WS('|',
      TRIM(REPLACE(unit_name, CHAR(160), '')),
      TRIM(REPLACE(data_type, CHAR(160), '')),
      TRIM(REPLACE(discreteness, CHAR(160), '')),
      TRIM(REPLACE(value_type, CHAR(160), '')),
      TRIM(REPLACE(org_name, CHAR(160), '')),
      TRIM(REPLACE(eat_name, CHAR(160), '')),
      TRIM(REPLACE(asset_name, CHAR(160), '')),
      TRIM(REPLACE(ind_code, CHAR(160), ''))
    )) as hub_ind_bk_lower,
    MD5(LOWER(CONCAT_WS('|',
      TRIM(REPLACE(unit_name, CHAR(160), '')),
      TRIM(REPLACE(data_type, CHAR(160), '')),
      TRIM(REPLACE(discreteness, CHAR(160), '')),
      TRIM(REPLACE(value_type, CHAR(160), '')),
      TRIM(REPLACE(org_name, CHAR(160), '')),
      TRIM(REPLACE(eat_name, CHAR(160), '')),
      TRIM(REPLACE(asset_name, CHAR(160), '')),
      TRIM(REPLACE(ind_code, CHAR(160), ''))
    ))) as hub_ind_pk,
    period_dt,
    report_date,
    load_dttm,
    source_nm
  FROM source_data
),

fact_data AS (
  SELECT
    
  MD5(
    CONCAT_WS('|', 
      TRIM(REPLACE(COALESCE(CAST(hub_ind_pk AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST(CAST(period_dt AS CHAR) AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST(CAST(report_date AS CHAR) AS CHAR), ''), CHAR(160), ''))
    )
  )
 as hub_fact_pk,
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
    bk_hub_ind_pk,
    bk_period_dt,
    bk_report_date,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM fact_data
  GROUP BY hub_fact_pk, bk_hub_ind_pk, bk_period_dt, bk_report_date
)

SELECT
  hub_fact_pk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm
FROM deduplicated_hubs
LIMIT 25000