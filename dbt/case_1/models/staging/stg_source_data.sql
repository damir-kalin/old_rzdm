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

-- Новая staging модель для объединения данных из form_1 и form_2
-- Источники из stg_buinu


-- FORM 1: Трансформация трех полей (На текущий год, На 31 декабря прошлого года, На 31 декабря позапрошлого года) при трансформации происходит трехратное увеличение записей
-- в отдельные строки с полем "Признак периода"
WITH form_1 AS (
  SELECT
    `АКТИВ` as asset_name,
    CAST(`Код показателя` AS VARCHAR) as ind_code,
    `На текущий год` as value_current_year,
    'На текущий год' as period_indicator,
    `Отчетный период` as period_text,
    `Название отчета` as report_type,
    `Организация` as org_name,
    REPLACE(
      REPLACE(
        TRIM(`Вид экономической деятельности`),
        'медицина',
        'Медицинская деятельность'
      ),
      'Медицина',
      'Медицинская деятельность'
    ) as eat_name,
    `Единица измерения` as unit_name,
    -- Тип_значения = 'Итог' (константа)
    'Итог' as value_type,
    -- Тип_данных = 'утвержденный' (константа)
           'утвержденный' as data_type,
           -- Метрика = 'Факт' (константа)
           'Факт' as metric,
           -- Дискретность: 'квартал' если период содержит месяцы/кварталы И (period_indicator = 'На текущий год' ИЛИ form_2), иначе 'год'
           CASE
             WHEN (`Отчетный период` LIKE '%квартал%' OR `Отчетный период` LIKE '%месяц%' OR `Отчетный период` LIKE '%месяцев%')
               THEN 'квартал'  -- period_indicator = 'На текущий год' для этого SELECT
             ELSE 'год'
           END as discreteness,
    -- Квартал: извлечь из 'Отчетный период' (1 квартал → 1, 6 месяцев → 2, 9 месяцев → 3, иначе → 4)
    CASE
      WHEN `Отчетный период` LIKE '%1 квартал%' THEN 1
      WHEN `Отчетный период` LIKE '%6 месяцев%' THEN 2
      WHEN `Отчетный период` LIKE '%9 месяцев%' THEN 3
      ELSE 4
    END as quarter,
    -- Год: извлечь из 'Отчетный период' (2025 г. → 2025)
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(`Отчетный период`, ' ', -2), ' ', 1) AS BIGINT) as year,
    now() as load_dttm,
    -- Маппинг source_nm для совместимости с существующими моделями
    CASE
      WHEN file_name LIKE '%Форма 1%' THEN 'raw.accounting_balance_form_1'
      WHEN file_name LIKE '%Форма 2%' THEN 'raw.accounting_balance_form_2'
      ELSE file_name
    END as source_nm
  FROM stg_buinu.form_1_assets
  WHERE `На текущий год` IS NOT NULL

  UNION ALL

  SELECT
    `АКТИВ` as asset_name,
    CAST(`Код показателя` AS VARCHAR) as ind_code,
    `На 31 декабря прошлого года` as value_current_year,
    'На 31 декабря прошлого года' as period_indicator,
    `Отчетный период` as period_text,
    `Название отчета` as report_type,
    `Организация` as org_name,
    REPLACE(
      REPLACE(
        TRIM(`Вид экономической деятельности`),
        'медицина',
        'Медицинская деятельность'
      ),
      'Медицина',
      'Медицинская деятельность'
    ) as eat_name,
    `Единица измерения` as unit_name,
    'Итог' as value_type,
    'утвержденный' as data_type,
    'Факт' as metric,
    -- Дискретность: 'квартал' если период содержит месяцы/кварталы И (period_indicator = 'На текущий год' ИЛИ form_2), иначе 'год'
    'год' as discreteness,  -- period_indicator = 'На 31 декабря прошлого года' (не 'На текущий год' и не form_2)
    -- Квартал: извлечь из 'Отчетный период' (1 квартал → 1, 6 месяцев → 2, 9 месяцев → 3, иначе → 4)
    CASE
      WHEN `Отчетный период` LIKE '%1 квартал%' THEN 1
      WHEN `Отчетный период` LIKE '%6 месяцев%' THEN 2
      WHEN `Отчетный период` LIKE '%9 месяцев%' THEN 3
      ELSE 4
    END as quarter,
    -- Год: извлечь из 'Отчетный период' (2025 г. → 2025)
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(`Отчетный период`, ' ', -2), ' ', 1) AS BIGINT) as year,
    now() as load_dttm,
    -- Маппинг source_nm для совместимости с существующими моделями
    CASE
      WHEN file_name LIKE '%Форма 1%' THEN 'raw.accounting_balance_form_1'
      WHEN file_name LIKE '%Форма 2%' THEN 'raw.accounting_balance_form_2'
      ELSE file_name
    END as source_nm
  FROM stg_buinu.form_1_assets
  WHERE `На 31 декабря прошлого года` IS NOT NULL

  UNION ALL

  SELECT
    `АКТИВ` as asset_name,
    CAST(`Код показателя` AS VARCHAR) as ind_code,
    `На 31 декабря позапрошлого года` as value_current_year,
    'На 31 декабря позапрошлого года' as period_indicator,
    `Отчетный период` as period_text,
    `Название отчета` as report_type,
    `Организация` as org_name,
    REPLACE(
      REPLACE(
        TRIM(`Вид экономической деятельности`),
        'медицина',
        'Медицинская деятельность'
      ),
      'Медицина',
      'Медицинская деятельность'
    ) as eat_name,
    `Единица измерения` as unit_name,
    'Итог' as value_type,
    'утвержденный' as data_type,
    'Факт' as metric,
    -- Дискретность: 'квартал' если период содержит месяцы/кварталы И (period_indicator = 'На текущий год' ИЛИ form_2), иначе 'год'
    'год' as discreteness,  -- period_indicator = 'На 31 декабря позапрошлого года' (не 'На текущий год' и не form_2)
    -- Квартал: извлечь из 'Отчетный период' (1 квартал → 1, 6 месяцев → 2, 9 месяцев → 3, иначе → 4)
    CASE
      WHEN `Отчетный период` LIKE '%1 квартал%' THEN 1
      WHEN `Отчетный период` LIKE '%6 месяцев%' THEN 2
      WHEN `Отчетный период` LIKE '%9 месяцев%' THEN 3
      ELSE 4
    END as quarter,
    -- Год: извлечь из 'Отчетный период' (2025 г. → 2025)
    CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(`Отчетный период`, ' ', -2), ' ', 1) AS BIGINT) as year,
    now() as load_dttm,
    -- Маппинг source_nm для совместимости с существующими моделями
    CASE
      WHEN file_name LIKE '%Форма 1%' THEN 'raw.accounting_balance_form_1'
      WHEN file_name LIKE '%Форма 2%' THEN 'raw.accounting_balance_form_2'
      ELSE file_name
    END as source_nm
  FROM stg_buinu.form_1_assets
  WHERE `На 31 декабря позапрошлого года` IS NOT NULL
),

-- FORM 2: Трансформация двух полей (За текущий период, За текущий период прошлого года)
-- в отдельные строки с полем "Признак периода"
form_2 AS (
  SELECT
    `Показатель` as asset_name,
    CAST(`Код` AS VARCHAR) as ind_code,
    `За текущий период` as value_current_year,
    'За текущий период' as period_indicator,
    `Отчетный период` as period_text,
    `Название отчета` as report_type,
    `Организация` as org_name,
    CAST(NULL AS VARCHAR(500)) as eat_name,
    `Единица измерения` as unit_name,
    -- Тип_значения: если period_text содержит только год (например "2024 г."), то 'Итог', иначе 'Нарастающий итог по кварталам с начала года'
    CASE
      WHEN `Отчетный период` NOT LIKE '%квартал%' 
        AND `Отчетный период` NOT LIKE '%месяц%'
        AND `Отчетный период` NOT LIKE '%месяцев%'
        AND `Отчетный период` NOT LIKE '%полугодие%'
        AND `Отчетный период` NOT LIKE '%сутки%'
        THEN 'Итог'
      ELSE 'Нарастающий итог по кварталам с начала года'
    END as value_type,
           -- Тип_данных = 'утвержденный' (константа)
           'утвержденный' as data_type,
           -- Метрика = 'Факт' (константа)
           'Факт' as metric,
           -- Дискретность: 'квартал' если период содержит месяцы/кварталы И (period_indicator = 'На текущий год' ИЛИ form_2), иначе 'год'
           CASE
             WHEN (`Отчетный период` LIKE '%квартал%' OR `Отчетный период` LIKE '%месяц%' OR `Отчетный период` LIKE '%месяцев%')
               THEN 'квартал'  -- form_2 всегда 'квартал' если период содержит кварталы/месяцы
             ELSE 'год'
           END as discreteness,
           -- Квартал: извлечь из 'Отчетный период' (1 квартал → 1, 6 месяцев → 2, 9 месяцев → 3, иначе → 4)
           CASE
             WHEN `Отчетный период` LIKE '%1 квартал%' THEN 1
             WHEN `Отчетный период` LIKE '%6 месяцев%' THEN 2
             WHEN `Отчетный период` LIKE '%9 месяцев%' THEN 3
             ELSE 4
           END as quarter,
           -- Год: извлечь из 'Отчетный период' (2025 г. → 2025)
           CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(`Отчетный период`, ' ', -2), ' ', 1) AS BIGINT) as year,
           now() as load_dttm,
           -- Маппинг source_nm для совместимости с существующими моделями
           CASE
             WHEN file_name LIKE '%Форма 1%' THEN 'raw.accounting_balance_form_1'
             WHEN file_name LIKE '%Форма 2%' THEN 'raw.accounting_balance_form_2'
             ELSE file_name
           END as source_nm
         FROM stg_buinu.form_2_financial
         WHERE `За текущий период` IS NOT NULL

  UNION ALL

  SELECT
    `Показатель` as asset_name,
    CAST(`Код` AS VARCHAR) as ind_code,
    `За текущий период прошлого года` as value_current_year,
    'За текущий период прошлого года' as period_indicator,
    `Отчетный период` as period_text,
    `Название отчета` as report_type,
    `Организация` as org_name,
    CAST(NULL AS VARCHAR(500)) as eat_name,
    `Единица измерения` as unit_name,
    -- Тип_значения: если period_text содержит только год (например "2024 г."), то 'Итог', иначе 'Нарастающий итог по кварталам с начала года'
    CASE
      WHEN `Отчетный период` NOT LIKE '%квартал%' 
        AND `Отчетный период` NOT LIKE '%месяц%'
        AND `Отчетный период` NOT LIKE '%месяцев%'
        AND `Отчетный период` NOT LIKE '%полугодие%'
        AND `Отчетный период` NOT LIKE '%сутки%'
        THEN 'Итог'
      ELSE 'Нарастающий итог по кварталам с начала года'
    END as value_type,
    'утвержденный' as data_type,
    'Факт' as metric,
    -- Дискретность: 'квартал' если период содержит месяцы/кварталы И (period_indicator = 'На текущий год' ИЛИ form_2), иначе 'год'
    CASE
      WHEN (`Отчетный период` LIKE '%квартал%' OR `Отчетный период` LIKE '%месяц%' OR `Отчетный период` LIKE '%месяцев%')
        THEN 'квартал'  -- form_2 всегда 'квартал' если период содержит кварталы/месяцы
      ELSE 'год'
    END as discreteness,
    -- Квартал: извлечь из 'Отчетный период' (1 квартал → 1, 6 месяцев → 2, 9 месяцев → 3, иначе → 4)
    CASE
      WHEN `Отчетный период` LIKE '%1 квартал%' THEN 1
      WHEN `Отчетный период` LIKE '%6 месяцев%' THEN 2
      WHEN `Отчетный период` LIKE '%9 месяцев%' THEN 3
      ELSE 4
    END as quarter,
    -- Год: извлечь из 'Отчетный период' (2025 г. → 2025)
           CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(`Отчетный период`, ' ', -2), ' ', 1) AS BIGINT) as year,
           now() as load_dttm,
           -- Маппинг source_nm для совместимости с существующими моделями
           CASE
             WHEN file_name LIKE '%Форма 1%' THEN 'raw.accounting_balance_form_1'
             WHEN file_name LIKE '%Форма 2%' THEN 'raw.accounting_balance_form_2'
             ELSE file_name
           END as source_nm
         FROM stg_buinu.form_2_financial
  WHERE `За текущий период прошлого года` IS NOT NULL
),

unified_data AS (
  SELECT 
    asset_name,
    ind_code,
    value_current_year,
    period_indicator,
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
    period_indicator,
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
    period_indicator,
    period_text,
    report_type,
    TRIM(REPLACE(REPLACE(REPLACE(org_name, CHAR(160), ''), CHAR(13), ''), CHAR(10), '')) as org_name_norm,
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
    AND org_name IS NOT NULL 
    AND asset_name IS NOT NULL
    AND ind_code IS NOT NULL
)

SELECT
  ind_code_norm as ind_code,
  asset_name_norm as asset_name,
  CAST(value_current_year AS DECIMAL(20,2)) as value_current_year,
  period_indicator,
  CAST(period_text AS VARCHAR(65533)) as period_text,
  CAST(report_type AS VARCHAR(65533)) as report_type,
  org_name_norm as org_name,
  eat_name_norm as eat_name,
  unit_name_norm as unit_name,
  value_type_norm as value_type,
  data_type_norm as data_type,
  metric_norm as metric,
  discreteness_norm as discreteness,
  CAST(quarter AS BIGINT) as quarter,
  year,
  load_dttm,
  -- Маппинг source_nm уже применен в CTE, просто приводим к нужному типу
  CAST(source_nm AS VARCHAR(1048576)) as source_nm,
  -- Предварительно сформированный hub_ind_bk в lowercase для совместимости с hub_rdv__ind
  -- ВАЖНО: hub_ind_bk должен быть в lowercase, так как hub_rdv__ind хранит его в lowercase
  LOWER(CONCAT_WS('|',
    unit_name_norm,
    data_type_norm,
    discreteness_norm,
    value_type_norm,
    org_name_norm,
    COALESCE(eat_name_norm, ''),
    asset_name_norm,
    ind_code_norm
  )) as hub_ind_bk
FROM normalized_data


