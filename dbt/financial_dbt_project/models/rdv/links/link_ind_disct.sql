{{
  config(
    materialized='table',
    unique_key='link_ind_disct_pk',
    pre_hook="DROP TABLE IF EXISTS {{ this.schema }}.link_ind_disct"
  )
}}

-- Link между показателем и дискретностью
-- Связывает показатель с уровнем детализации временного периода:
--   - Месяц (M): период разбивается на месяцы
--   - Квартал (Q): период разбивается на кварталы
--   - Год (Y): период разбивается на годы
-- Используется для определения, как должен быть разбит период показателя
-- Определение дискретности на основе периода из исходных данных

WITH hub_ind AS (
  SELECT hub_ind_pk, hub_ind_bk
  FROM {{ ref('hub_ind') }}
),

hub_discr AS (
  SELECT hub_discr_pk, hub_discr_bk
  FROM {{ ref('hub_discr') }}
),

staging AS (
  SELECT DISTINCT
    ind_code,
    period,
    load_dttm,
    source_nm
  FROM {{ ref('stg_source_data') }}
  WHERE ind_code IS NOT NULL AND period IS NOT NULL
),

-- Определяем тип дискретности на основе периода
staging_with_discr AS (
  SELECT
    ind_code,
    period,
    load_dttm,
    source_nm,
    CASE 
      WHEN LOWER(period) LIKE '%месяц%' OR LOWER(period) LIKE '%мес%' THEN 'M'
      WHEN LOWER(period) LIKE '%квартал%' OR LOWER(period) LIKE '%кв%' THEN 'Q'
      WHEN LOWER(period) LIKE '%год%' OR LOWER(period) LIKE '%г.%' THEN 'Y'
      ELSE 'Y'  -- По умолчанию год
    END as discr_code
  FROM staging
)

SELECT
  {{ generate_hash(['hi.hub_ind_pk', 'hd.hub_discr_pk']) }} as link_ind_disct_pk,
  hi.hub_ind_pk,
  hd.hub_discr_pk,
  s.load_dttm as valid_from,
  CAST('9999-12-31 23:59:59' AS DATETIME) as valid_to,
  s.load_dttm,
  s.source_nm
FROM staging_with_discr s
INNER JOIN hub_ind hi ON s.ind_code = hi.hub_ind_bk
INNER JOIN hub_discr hd ON s.discr_code = hd.hub_discr_bk

