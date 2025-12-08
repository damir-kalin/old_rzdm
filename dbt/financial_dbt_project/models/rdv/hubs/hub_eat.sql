{{
  config(
    materialized='table',
    unique_key='hub_eat_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_eat"
  )
}}

WITH staging AS (
  SELECT DISTINCT
    eat_name as hub_eat_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE eat_name IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_eat_bk']) }} as hub_eat_pk,
    hub_eat_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub



