{{
  config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
  )
}}

-- Staging модель для исходных данных
-- Источник: таблица, созданная из файла Russian Financial Forms

SELECT
  `АКТИВ` as asset_name,
  `Код показателя` as ind_code,
  CAST(`Значение` AS DOUBLE) as value_current_year,
  CAST(NULL AS DOUBLE) as value_prev_year,
  CAST(NULL AS DOUBLE) as value_prev2_year,
  `Период` as period,
  `Организация` as org_name,
  `Вид экономической деятельности` as eat_name,
  `Еденица измерения` as unit_name,
  `Тип отчета` as report_type,
  `Квартал` as quarter,
  `Год` as year,
  `Тип значения` as value_type,
  `Тип данных` as data_type,
  `Метрика` as metric,
  `Дискретность` as discreteness,
  -- Генерируем идентификаторы для Data Vault
  MD5(CONCAT_WS('||', COALESCE(CAST(`АКТИВ` AS CHAR), ''))) as asset_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Код показателя` AS CHAR), ''))) as ind_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Организация` AS CHAR), ''))) as org_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Вид экономической деятельности` AS CHAR), ''))) as eat_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Еденица измерения` AS CHAR), ''))) as unit_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Тип данных` AS CHAR), ''))) as data_type_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Тип значения` AS CHAR), ''))) as value_type_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Метрика` AS CHAR), ''))) as metric_id,
  MD5(CONCAT_WS('||', COALESCE(CAST(`Дискретность` AS CHAR), ''))) as discr_id,
  -- Use source metadata fields
  `processed_at` as load_dttm,
  `source_file` as source_nm
FROM {{ source('raw', 'accounting_balance_form_1') }}

