{{
  config(
    materialized='table',
    schema='stage'
  )
}}

-- Staging модель для данных из Template 2
-- Объединяет данные из всех таблиц в:
--   stg_asb, stg_indefinite, stg_mis, stg_asckz, stg_kuirzp, stg_nsi, stg_buinu
-- Исключает таблицы: form_1_assets, form_2_financial

{{ union_all_source_tables() }}
