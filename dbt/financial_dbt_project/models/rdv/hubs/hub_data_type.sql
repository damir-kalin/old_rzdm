{{
  config(
    materialized='table',
    unique_key='hub_data_type_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_data_type"
  )
}}

-- Hub для справочника "Тип данных"

WITH staging AS (
  SELECT DISTINCT
    data_type as hub_data_type_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE data_type IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_data_type_bk']) }} as hub_data_type_pk,
    hub_data_type_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub

