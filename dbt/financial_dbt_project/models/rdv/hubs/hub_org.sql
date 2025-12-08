{{
  config(
    materialized='table',
    unique_key='hub_org_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.hub_org"
  )
}}

WITH staging AS (
  SELECT DISTINCT
    org_name as hub_org_bk,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE org_name IS NOT NULL
),

hub AS (
  SELECT
    {{ generate_hash(['hub_org_bk']) }} as hub_org_pk,
    hub_org_bk,
    load_dttm,
    source_nm
  FROM staging
)

SELECT * FROM hub



