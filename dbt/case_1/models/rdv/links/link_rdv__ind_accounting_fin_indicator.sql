{{ 
  config(
    alias='link_rdv__ind_accounting_fin_indicator',
    materialized='incremental',
    unique_key='link_ind_accounting_fin_indicator_pk',
    table_type='PRIMARY',
    keys=['link_ind_accounting_fin_indicator_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    enabled=true
  )
}}

-- Link между показателем и учетно-финансовым показателем
-- PK формируется как MD5(hub_ind_pk|hub_accounting_fin_indicator_pk)
-- Матчинг через поле code в сателлите sat_rdv__ind
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в таблице

WITH hub_ind_data AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    source_nm,
    load_dttm
  FROM {{ ref('hub_rdv__ind') }}
  {% if is_incremental() %}
    WHERE load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

sat_ind_latest AS (
  SELECT
    hub_ind_pk,
    code,
    load_dttm,
    source_nm,
    ROW_NUMBER() OVER (PARTITION BY hub_ind_pk ORDER BY load_dttm DESC) as rn
  FROM {{ ref('sat_rdv__ind') }}
  WHERE code IS NOT NULL
),

sat_ind_current AS (
  SELECT
    hub_ind_pk,
    code,
    load_dttm,
    source_nm
  FROM sat_ind_latest
  WHERE rn = 1
),

hub_afi_data AS (
  SELECT
    accounting_fin_indicator_hk as hub_accounting_fin_indicator_pk,
    code as hub_accounting_fin_indicator_bk,
    source_system as source_nm
  FROM {{ source('unverified_rdv', 'hub_rdv__accounting_fin_indicator') }}
),

links_with_hubs AS (
  SELECT
    hi.hub_ind_pk,
    hafi.hub_accounting_fin_indicator_pk,
    GREATEST(hi.load_dttm, sat.load_dttm) as load_dttm,
    COALESCE(hi.source_nm, sat.source_nm) as source_nm
  FROM hub_ind_data hi
  INNER JOIN sat_ind_current sat ON
    hi.hub_ind_pk = sat.hub_ind_pk
  INNER JOIN hub_afi_data hafi ON
    TRIM(REPLACE(sat.code, CHAR(160), '')) = TRIM(REPLACE(hafi.hub_accounting_fin_indicator_bk, CHAR(160), ''))
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_accounting_fin_indicator_pk AS CHAR))) as link_ind_accounting_fin_indicator_pk,
    hub_ind_pk,
    hub_accounting_fin_indicator_pk,
    load_dttm,
    source_nm
  FROM links_with_hubs
),

deduplicated_links AS (
  SELECT
    link_ind_accounting_fin_indicator_pk,
    hub_ind_pk,
    hub_accounting_fin_indicator_pk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM link_data
  GROUP BY link_ind_accounting_fin_indicator_pk, hub_ind_pk, hub_accounting_fin_indicator_pk
)

SELECT
  link_ind_accounting_fin_indicator_pk,
  hub_ind_pk,
  hub_accounting_fin_indicator_pk,
  load_dttm,
  source_nm
FROM deduplicated_links

