{{
  config(
    materialized='table',
    unique_key='hub_metric_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_metric"
  )
}}

-- Hub для справочника "Метрика"

WITH staging AS (
  SELECT DISTINCT
    metric as hub_metric_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE metric IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_metric_bk']) }} as hub_metric_pk,
    hub_metric_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub

