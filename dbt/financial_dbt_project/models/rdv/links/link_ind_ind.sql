{{
  config(
    materialized='table',
    unique_key='link_ind_ind_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.link_ind_ind"
  )
}}

SELECT
  CAST('N/A' AS CHAR) as link_ind_ind_pk,
  CAST('N/A' AS CHAR) as hub_ind_pk,
  CAST('N/A' AS CHAR) as hub_ind_pk_2,
  CAST(NOW() AS DATETIME) as valid_from,
  CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
  CAST(NOW() AS DATETIME) as load_dttm,
  CAST('1c' AS VARCHAR(100)) as source_nm
WHERE 1=0  -- Пустая таблица, так как нет данных для иерархии показателей

