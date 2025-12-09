{{
  config(
    materialized='table',
    schema='stage',
    table_type='DUPLICATE',
    keys=['ind_code'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- Staging модель для объединения данных из form_1 и form_2
-- Источники из t1_stage

WITH form_1 AS (
  SELECT
    `АКТИВ` as asset_name,
    CAST(`Код_показателя` AS VARCHAR) as ind_code,
    `Значение` as value_current_year,
    `Период` as period_text,
    `Тип_отчета` as report_type,
    `Организация` as org_name,
    REPLACE(
      REPLACE(
        TRIM(`Вид_экономической_деятельности`),
        'медицина',
        'Медицинская деятельность'
      ),
      'Медицина',
      'Медицинская деятельность'
    ) as eat_name,
    `Еденица_измерения` as unit_name,
    `Тип_значения` as value_type,
    REPLACE(`Тип_данных`, 'Утвержденный', 'утвержденный') as data_type,
    `Метрика` as metric,
    REPLACE(`Дискретность`, 'Квартал', 'квартал') as discreteness,
    `Квартал` as quarter,
    `Год` as year,
    now() as load_dttm,
    'raw.accounting_balance_form_1' as source_nm
  FROM t1_stage.financial_Форма_1_xls_rzdm_dev_buinu_sys_bucket
),

form_2 AS (
  SELECT
    `Наименование_показателя` as asset_name,
    CAST(`Код` AS VARCHAR) as ind_code,
    `Значение` as value_current_year,
    `Период` as period_text,
    `Тип_отчета` as report_type,
    `Организация` as org_name,
    REPLACE(
      REPLACE(
        TRIM(`Вид_экономической_деятельности`),
        'медицина',
        'Медицинская деятельность'
      ),
      'Медицина',
      'Медицинская деятельность'
    ) as eat_name,
    `Еденица_измерения` as unit_name,
    `Тип_значения` as value_type,
    REPLACE(`Тип_данных`, 'Утвержденный', 'утвержденный') as data_type,
    `Метрика` as metric,
    REPLACE(`Дискретность`, 'Квартал', 'квартал') as discreteness,
    `Квартал` as quarter,
    `Год` as year,
    now() as load_dttm,
    'raw.accounting_balance_form_2' as source_nm
  FROM t1_stage.financial_Форма_2_xls_rzdm_dev_buinu_sys_bucket
),

unified_data AS (
  SELECT 
    asset_name,
    ind_code,
    value_current_year,
    period_text,
    report_type,
    org_name,
    eat_name,
    unit_name,
    value_type,
    data_type,
    metric,
    discreteness,
    quarter,
    year,
    load_dttm,
    source_nm
  FROM form_1

  UNION ALL

  SELECT 
    asset_name,
    ind_code,
    value_current_year,
    period_text,
    report_type,
    org_name,
    eat_name,
    unit_name,
    value_type,
    data_type,
    metric,
    discreteness,
    quarter,
    year,
    load_dttm,
    source_nm
  FROM form_2
),

-- Нормализация полей с удалением CHAR(160) и TRIM
normalized_data AS (
  SELECT
    asset_name,
    ind_code,
    value_current_year,
    period_text,
    report_type,
    TRIM(REPLACE(org_name, CHAR(160), '')) as org_name_norm,
    TRIM(REPLACE(eat_name, CHAR(160), '')) as eat_name_norm,
    TRIM(REPLACE(unit_name, CHAR(160), '')) as unit_name_norm,
    TRIM(REPLACE(value_type, CHAR(160), '')) as value_type_norm,
    TRIM(REPLACE(data_type, CHAR(160), '')) as data_type_norm,
    TRIM(REPLACE(metric, CHAR(160), '')) as metric_norm,
    TRIM(REPLACE(discreteness, CHAR(160), '')) as discreteness_norm,
    TRIM(REPLACE(asset_name, CHAR(160), '')) as asset_name_norm,
    TRIM(REPLACE(ind_code, CHAR(160), '')) as ind_code_norm,
    quarter,
    year,
    load_dttm,
    source_nm
  FROM unified_data
  WHERE unit_name IS NOT NULL 
    AND data_type IS NOT NULL
    AND discreteness IS NOT NULL
    AND value_type IS NOT NULL
    AND org_name IS NOT NULL 
    AND eat_name IS NOT NULL 
    AND asset_name IS NOT NULL
    AND ind_code IS NOT NULL
)

SELECT
  ind_code_norm as ind_code,
  asset_name_norm as asset_name,
  value_current_year,
  period_text,
  report_type,
  org_name_norm as org_name,
  eat_name_norm as eat_name,
  unit_name_norm as unit_name,
  value_type_norm as value_type,
  data_type_norm as data_type,
  metric_norm as metric,
  discreteness_norm as discreteness,
  quarter,
  year,
  load_dttm,
  source_nm,
  -- Предварительно сформированный hub_ind_bk
  -- Убрали CAST, так как StarRocks не поддерживает CHARACTER SET в CAST
  CONCAT_WS('|',
    unit_name_norm,
    data_type_norm,
    discreteness_norm,
    value_type_norm,
    org_name_norm,
    eat_name_norm,
    asset_name_norm,
    ind_code_norm
  ) as hub_ind_bk
FROM normalized_data
