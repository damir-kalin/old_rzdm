{{
  config(
    materialized='table',
    unique_key='hub_unit_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_unit"
  )
}}

WITH staging AS (
  SELECT DISTINCT
    unit_name as hub_unit_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE unit_name IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_unit_bk']) }} as hub_unit_pk,
    hub_unit_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub



