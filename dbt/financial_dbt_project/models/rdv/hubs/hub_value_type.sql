{{
  config(
    materialized='table',
    unique_key='hub_value_type_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_value_type"
  )
}}

-- Hub для справочника "Тип значения"

WITH staging AS (
  SELECT DISTINCT
    value_type as hub_value_type_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE value_type IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_value_type_bk']) }} as hub_value_type_pk,
    hub_value_type_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub

