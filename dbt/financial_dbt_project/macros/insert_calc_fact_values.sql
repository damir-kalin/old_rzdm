{% macro insert_calc_fact_values(target_table) %}
  -- Макрос для вставки расчетных значений в sat_bdv_fact_value
  -- ВАЖНО: Расчетные значения создаются напрямую из stg_source_data
  -- и НЕ попадают под LIMIT базовых данных
  
  INSERT INTO {{ target_table }}
  SELECT
    hub_fact_pk,
    value_amt,
    load_date,
    source_nm,
    file_row_number,
    file_code,
    file_name,
    ind_source_name,
    ind_model_code,
    ind_model_name
  FROM (
    -- Используем упрощенную версию расчета (только основные показатели для теста)
    -- Полная логика находится в sat_bdv_fact_value.sql
    WITH calc_simple AS (
      SELECT
        CASE 
          WHEN s.quarter = 1 THEN STR_TO_DATE(CONCAT(s.year, '-01-01'), '%Y-%m-%d')
          WHEN s.quarter = 2 THEN STR_TO_DATE(CONCAT(s.year, '-04-01'), '%Y-%m-%d')
          WHEN s.quarter = 3 THEN STR_TO_DATE(CONCAT(s.year, '-07-01'), '%Y-%m-%d')
          WHEN s.quarter = 4 THEN STR_TO_DATE(CONCAT(s.year, '-10-01'), '%Y-%m-%d')
          ELSE STR_TO_DATE(CONCAT(s.year, '-01-01'), '%Y-%m-%d')
        END as period_dt,
        DATE(s.load_dttm) as report_date,
        MAX(s.load_dttm) as load_dttm,
        s.source_nm,
        CAST('10107' AS VARCHAR(500)) as calc_ind_code,
        SUM(CASE WHEN s.ind_code = '1250' THEN s.value_current_year ELSE 0 END) as calc_value
      FROM t1_stage.stg_source_data s
      WHERE s.source_nm = 'raw.accounting_balance_form_1'
      AND s.ind_code = '1250'
      AND s.value_current_year IS NOT NULL
      AND s.year IS NOT NULL
      AND s.quarter IS NOT NULL
      GROUP BY s.year, s.quarter, period_dt, report_date, s.source_nm
      HAVING calc_value IS NOT NULL
    ),
    calc_fact_bk AS (
      SELECT
        CONCAT_WS('|', 
          TRIM(REPLACE(COALESCE(CAST(calc_ind_code AS CHAR), ''), CHAR(160), '')),
          TRIM(REPLACE(COALESCE(CAST(period_dt AS CHAR), ''), CHAR(160), '')),
          TRIM(REPLACE(COALESCE(CAST(report_date AS CHAR), ''), CHAR(160), ''))
        ) as hub_fact_bk,
        calc_value,
        load_dttm,
        source_nm
      FROM calc_simple
    )
    SELECT
      MD5(hub_fact_bk) as hub_fact_pk,
      calc_value as value_amt,
      DATE(load_dttm) as load_date,
      source_nm,
      CAST(NULL AS INT) as file_row_number,
      CAST(NULL AS VARCHAR(100)) as file_code,
      CAST(NULL AS VARCHAR(100)) as file_name,
      CAST(NULL AS VARCHAR(100)) as ind_source_name,
      CAST(NULL AS VARCHAR(100)) as ind_model_code,
      CAST(NULL AS VARCHAR(100)) as ind_model_name
    FROM calc_fact_bk
  ) calc_insert
  WHERE NOT EXISTS (
    SELECT 1 FROM {{ target_table }} t 
    WHERE t.hub_fact_pk = calc_insert.hub_fact_pk 
    AND t.load_date = calc_insert.load_date
  )
{% endmacro %}

