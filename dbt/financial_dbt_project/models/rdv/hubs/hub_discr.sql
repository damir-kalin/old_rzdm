{{
  config(
    materialized='table',
    unique_key='hub_discr_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_discr"
  )
}}

-- Hub для дискретности (разбиение периода на месяцы, кварталы, годы)
-- Дискретность определяет уровень детализации временного периода
-- Business Key берется из поля Дискретность из исходных данных

WITH staging AS (
  SELECT DISTINCT
    discreteness as hub_discr_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE discreteness IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_discr_bk']) }} as hub_discr_pk,
    hub_discr_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub

