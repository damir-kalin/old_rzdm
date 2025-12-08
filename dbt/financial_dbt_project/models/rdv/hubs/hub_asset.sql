{{
  config(
    materialized='table',
    unique_key='hub_asset_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_asset"
  )
}}

WITH staging AS (
  SELECT DISTINCT
    asset_name as hub_asset_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE asset_name IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_asset_bk']) }} as hub_asset_pk,
    hub_asset_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub

