{{
  config(
    materialized='table',
    schema='stage',
    table_type='DUPLICATE',
    keys=['hub_fact_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Временная таблица с расчетными показателями
-- Создается напрямую из stg_source_data

WITH 
-- Получаем значения базовых показателей по кодам для Формы 1
form_1_base_values AS (
  SELECT 
    s.year,
    s.quarter,
    {{ calculate_period_dt('s.year', 's.quarter') }} as period_dt,
    DATE(s.load_dttm) as report_date,
    s.source_nm,
    s.load_dttm,
    s.ind_code,
    s.value_current_year as value_amt
  FROM {{ ref('stg_source_data') }} s
  WHERE s.source_nm = 'raw.accounting_balance_form_1'
    AND s.ind_code IN ('1250', '1170', '1410', '1510', '1230', '1520', '1440', '1450', '1550', '1232', '1240', '1252', '1232.3', '1232.4')
    AND s.value_current_year IS NOT NULL
    AND s.year IS NOT NULL
    AND s.quarter IS NOT NULL
),

-- Pivot значений по кодам для вычисления формул (Форма 1)
form_1_pivoted AS (
  SELECT 
    year,
    quarter,
    period_dt,
    report_date,
    source_nm,
    MAX(load_dttm) as load_dttm,
    SUM(CASE WHEN ind_code = '1250' THEN value_amt ELSE 0 END) as val_1250,
    SUM(CASE WHEN ind_code = '1170' THEN value_amt ELSE 0 END) as val_1170,
    SUM(CASE WHEN ind_code = '1410' THEN value_amt ELSE 0 END) as val_1410,
    SUM(CASE WHEN ind_code = '1510' THEN value_amt ELSE 0 END) as val_1510,
    SUM(CASE WHEN ind_code = '1230' THEN value_amt ELSE 0 END) as val_1230,
    SUM(CASE WHEN ind_code = '1520' THEN value_amt ELSE 0 END) as val_1520,
    SUM(CASE WHEN ind_code = '1440' THEN value_amt ELSE 0 END) as val_1440,
    SUM(CASE WHEN ind_code = '1450' THEN value_amt ELSE 0 END) as val_1450,
    SUM(CASE WHEN ind_code = '1550' THEN value_amt ELSE 0 END) as val_1550,
    SUM(CASE WHEN ind_code = '1232' THEN value_amt ELSE 0 END) as val_1232,
    SUM(CASE WHEN ind_code = '1240' THEN value_amt ELSE 0 END) as val_1240,
    SUM(CASE WHEN ind_code = '1252' THEN value_amt ELSE 0 END) as val_1252,
    SUM(CASE WHEN ind_code = '1232.3' THEN value_amt ELSE 0 END) as val_1232_3,
    SUM(CASE WHEN ind_code = '1232.4' THEN value_amt ELSE 0 END) as val_1232_4
  FROM form_1_base_values
  GROUP BY year, quarter, period_dt, report_date, source_nm
),

-- Получаем значения базовых показателей по кодам для Формы 2
form_2_base_values AS (
  SELECT 
    s.year,
    s.quarter,
    {{ calculate_period_dt('s.year', 's.quarter') }} as period_dt,
    DATE(s.load_dttm) as report_date,
    s.source_nm,
    s.load_dttm,
    s.ind_code,
    s.value_current_year as value_amt
  FROM {{ ref('stg_source_data') }} s
  WHERE s.source_nm = 'raw.accounting_balance_form_2'
    AND s.ind_code IN ('2400', '2120', '2110')
    AND s.value_current_year IS NOT NULL
    AND s.year IS NOT NULL
    AND s.quarter IS NOT NULL
),

-- Pivot значений по кодам для вычисления формул (Форма 2)
form_2_pivoted AS (
  SELECT 
    year,
    quarter,
    period_dt,
    report_date,
    source_nm,
    MAX(load_dttm) as load_dttm,
    SUM(CASE WHEN ind_code = '2400' THEN value_amt ELSE 0 END) as val_2400,
    SUM(CASE WHEN ind_code = '2120' THEN value_amt ELSE 0 END) as val_2120,
    SUM(CASE WHEN ind_code = '2110' THEN value_amt ELSE 0 END) as val_2110
  FROM form_2_base_values
  GROUP BY year, quarter, period_dt, report_date, source_nm
),

-- Вычисляем расчетные показатели для Формы 1
form_1_calculated AS (
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10107' AS VARCHAR(500)) as calc_ind_code,
    val_1250 as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE val_1250 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10108' AS VARCHAR(500)) as calc_ind_code,
    val_1170 as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE val_1170 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10109' AS VARCHAR(500)) as calc_ind_code,
    COALESCE(val_1410, 0) + COALESCE(val_1510, 0) as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE val_1410 IS NOT NULL OR val_1510 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10110' AS VARCHAR(500)) as calc_ind_code,
    val_1230 as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE val_1230 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10111' AS VARCHAR(500)) as calc_ind_code,
    COALESCE(val_1520, 0) + COALESCE(val_1440, 0) + COALESCE(val_1450, 0) + COALESCE(val_1550, 0) as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE val_1520 IS NOT NULL OR val_1440 IS NOT NULL OR val_1450 IS NOT NULL OR val_1550 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10117' AS VARCHAR(500)) as calc_ind_code,
    CASE 
      WHEN (COALESCE(val_1510, 0) + COALESCE(val_1520, 0)) != 0 
      THEN (COALESCE(val_1250, 0) + COALESCE(val_1232, 0) + COALESCE(val_1240, 0)) / (COALESCE(val_1510, 0) + COALESCE(val_1520, 0))
      ELSE NULL
    END as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE (val_1250 IS NOT NULL OR val_1232 IS NOT NULL OR val_1240 IS NOT NULL)
    AND (val_1510 IS NOT NULL OR val_1520 IS NOT NULL)
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10118' AS VARCHAR(500)) as calc_ind_code,
    CASE 
      WHEN COALESCE(val_1520, 0) != 0 
      THEN (COALESCE(val_1232, 0) + COALESCE(val_1252, 0)) / val_1520
      ELSE NULL
    END as calc_value,
    'raw.accounting_balance_form_1' as calc_source_nm
  FROM form_1_pivoted
  WHERE (val_1232 IS NOT NULL OR val_1252 IS NOT NULL)
    AND val_1520 IS NOT NULL
),

-- Вычисляем расчетные показатели для Формы 2
form_2_calculated AS (
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10112' AS VARCHAR(500)) as calc_ind_code,
    val_2400 as calc_value,
    'raw.accounting_balance_form_2' as calc_source_nm
  FROM form_2_pivoted
  WHERE val_2400 IS NOT NULL
  
  UNION ALL
  
  SELECT
    period_dt,
    report_date,
    source_nm,
    load_dttm,
    CAST('10113' AS VARCHAR(500)) as calc_ind_code,
    val_2120 as calc_value,
    'raw.accounting_balance_form_2' as calc_source_nm
  FROM form_2_pivoted
  WHERE val_2120 IS NOT NULL
),

-- Нарастающий итог по кварталам с начала года для Формы 1
form_1_cumulative AS (
  SELECT 
    f1.year,
    f1.quarter,
    f1.period_dt,
    f1.report_date,
    f1.source_nm,
    f1.load_dttm,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_1520 ELSE 0 END), f1.val_1520) as val_1520_cumulative,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_1232 ELSE 0 END), f1.val_1232) as val_1232_cumulative,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_1232_3 ELSE 0 END), f1.val_1232_3) as val_1232_3_cumulative,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_1232_4 ELSE 0 END), f1.val_1232_4) as val_1232_4_cumulative
  FROM form_1_pivoted f1
  LEFT JOIN form_1_pivoted f2 ON f2.year = f1.year AND f2.quarter <= f1.quarter
  GROUP BY f1.year, f1.quarter, f1.period_dt, f1.report_date, f1.source_nm, f1.load_dttm, f1.val_1520, f1.val_1232, f1.val_1232_3, f1.val_1232_4
),

-- Данные прошлого года (Q4) для Формы 1
form_1_prev_year AS (
  SELECT 
    year + 1 as current_year,
    val_1520 as val_1520_prev_year,
    val_1232 as val_1232_prev_year,
    val_1232_3 as val_1232_3_prev_year,
    val_1232_4 as val_1232_4_prev_year
  FROM form_1_pivoted
  WHERE quarter = 4
),

-- Нарастающий итог по кварталам с начала года для Формы 2
form_2_cumulative AS (
  SELECT 
    f1.year,
    f1.quarter,
    f1.period_dt,
    f1.report_date,
    f1.source_nm,
    f1.load_dttm,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_2120 ELSE 0 END), f1.val_2120) as val_2120_cumulative,
    COALESCE(SUM(CASE WHEN f2.quarter <= f1.quarter AND f2.year = f1.year THEN f2.val_2110 ELSE 0 END), f1.val_2110) as val_2110_cumulative
  FROM form_2_pivoted f1
  LEFT JOIN form_2_pivoted f2 ON f2.year = f1.year AND f2.quarter <= f1.quarter
  GROUP BY f1.year, f1.quarter, f1.period_dt, f1.report_date, f1.source_nm, f1.load_dttm, f1.val_2120, f1.val_2110
),

-- Расчетные показатели с нарастающим итогом (10119, 10120)
cumulative_calculated AS (
  SELECT
    f2c.period_dt,
    f2c.report_date,
    f2c.source_nm,
    f2c.load_dttm,
    CAST('10119' AS VARCHAR(500)) as calc_ind_code,
    CASE 
      WHEN (COALESCE(f1c.val_1520_cumulative, 0) + COALESCE(f1py.val_1520_prev_year, 0)) != 0
      THEN f2c.val_2120_cumulative / ((f1c.val_1520_cumulative + COALESCE(f1py.val_1520_prev_year, 0)) / 2)
      ELSE NULL
    END as calc_value,
    'raw.accounting_balance_form_2' as calc_source_nm
  FROM form_2_cumulative f2c
  INNER JOIN form_1_cumulative f1c ON f2c.year = f1c.year AND f2c.quarter = f1c.quarter
  LEFT JOIN form_1_prev_year f1py ON f2c.year = f1py.current_year
  WHERE f2c.val_2120_cumulative IS NOT NULL
    AND f1c.val_1520_cumulative IS NOT NULL
  
  UNION ALL
  
  SELECT
    f2c.period_dt,
    f2c.report_date,
    f2c.source_nm,
    f2c.load_dttm,
    CAST('10120' AS VARCHAR(500)) as calc_ind_code,
    CASE 
      WHEN ((COALESCE(f1c.val_1232_cumulative, 0) - COALESCE(f1c.val_1232_3_cumulative, 0) - COALESCE(f1c.val_1232_4_cumulative, 0)) + 
            (COALESCE(f1py.val_1232_prev_year, 0) - COALESCE(f1py.val_1232_3_prev_year, 0) - COALESCE(f1py.val_1232_4_prev_year, 0))) != 0
      THEN f2c.val_2110_cumulative / (((f1c.val_1232_cumulative - COALESCE(f1c.val_1232_3_cumulative, 0) - COALESCE(f1c.val_1232_4_cumulative, 0)) + 
                                       (COALESCE(f1py.val_1232_prev_year, 0) - COALESCE(f1py.val_1232_3_prev_year, 0) - COALESCE(f1py.val_1232_4_prev_year, 0))) / 2)
      ELSE NULL
    END as calc_value,
    'raw.accounting_balance_form_2' as calc_source_nm
  FROM form_2_cumulative f2c
  INNER JOIN form_1_cumulative f1c ON f2c.year = f1c.year AND f2c.quarter = f1c.quarter
  LEFT JOIN form_1_prev_year f1py ON f2c.year = f1py.current_year
  WHERE f2c.val_2110_cumulative IS NOT NULL
    AND f1c.val_1232_cumulative IS NOT NULL
),

-- Объединяем все расчетные показатели
all_calculated AS (
  SELECT * FROM form_1_calculated
  UNION ALL
  SELECT * FROM form_2_calculated
  UNION ALL
  SELECT * FROM cumulative_calculated
),

-- Создаем hub_fact_bk и hub_fact_pk для расчетных показателей
calc_fact_bk_with_bk AS (
  SELECT
    c.period_dt,
    c.report_date,
    c.load_dttm,
    c.calc_source_nm as source_nm,
    c.calc_ind_code,
    c.calc_value,
    {{ generate_hub_fact_bk('c.calc_ind_code', 'c.period_dt', 'c.report_date') }} as hub_fact_bk
  FROM all_calculated c
  WHERE c.calc_value IS NOT NULL
),

calc_fact_bk_grouped AS (
  SELECT
    hub_fact_bk,
    MAX(period_dt) as period_dt,
    MAX(report_date) as report_date,
    MAX(load_dttm) as load_dttm,
    MAX(source_nm) as source_nm,
    MAX(calc_ind_code) as calc_ind_code,
    MAX(calc_value) as calc_value
  FROM calc_fact_bk_with_bk
  GROUP BY hub_fact_bk
),

calc_fact_bk AS (
  SELECT
    c.hub_fact_bk,
    MD5(c.hub_fact_bk) as hub_fact_pk,
    c.calc_ind_code as bk_hub_ind_pk,
    c.period_dt as bk_period_dt,
    c.report_date as bk_report_date,
    c.load_dttm,
    c.source_nm,
    c.calc_value
  FROM calc_fact_bk_grouped c
)

-- Формируем записи для временной таблицы
SELECT
  hub_fact_pk,
  bk_hub_ind_pk,
  bk_period_dt,
  bk_report_date,
  load_dttm,
  source_nm,
  calc_value as value_amt
FROM calc_fact_bk


