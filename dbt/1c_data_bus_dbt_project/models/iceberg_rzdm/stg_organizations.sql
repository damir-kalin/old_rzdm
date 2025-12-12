{{ config(
    materialized='ephemeral',
    pre_hook="SET CATALOG iceberg; CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_organizations (
        `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
        src_sys_id VARCHAR(10) DEFAULT 'nsi' COMMENT 'Источник данных',
        load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
    )"
) }}

-- Эта модель только создает структуру таблицы
-- Данные загружаются задачами Flink
SELECT 1 WHERE 1=0
