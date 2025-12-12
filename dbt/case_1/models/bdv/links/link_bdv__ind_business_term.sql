{{ 
  config(
    alias='link_bdv__ind_business_term',
    materialized='incremental',
    unique_key='link_ind_business_term_pk',
    table_type='PRIMARY',
    keys=['link_ind_business_term_pk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    enabled=true
  )
}}

-- Link между показателем и бизнес-термином через сравнение нормализованных BK
-- BK показателя: hub_ind_bk (парсим и нормализуем) + name из sat_bdv__ind (для получения metric)
-- BK бизнес-термина: fullname (нормализуем тем же правилом)
-- Структура ключа: asset_name|unit_name|data_type|metric|discreteness|value_type

WITH hub_ind_data AS (
  SELECT
    hi.hub_ind_pk,
    hi.hub_ind_bk,
    hi.load_dttm,
    hi.source_nm
  FROM {{ ref('hub_bdv__ind') }} hi
  WHERE hi.hub_ind_bk IS NOT NULL
  {% if is_incremental() %}
    AND hi.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

-- Получаем name из sat_bdv__ind для извлечения metric (позиция 3)
sat_ind_data AS (
  SELECT
    si.hub_ind_pk,
    si.name,
    si.load_dttm,
    ROW_NUMBER() OVER (PARTITION BY si.hub_ind_pk ORDER BY si.load_dttm DESC) as rn
  FROM {{ ref('sat_bdv__ind') }} si
  WHERE si.name IS NOT NULL
  {% if is_incremental() %}
    AND si.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM {{ this }})
  {% endif %}
),

hub_ind_with_sat AS (
  SELECT
    hi.hub_ind_pk,
    hi.hub_ind_bk,
    si.name as sat_name,
    hi.load_dttm,
    hi.source_nm
  FROM hub_ind_data hi
  LEFT JOIN (
    SELECT hub_ind_pk, name, load_dttm
    FROM sat_ind_data
    WHERE rn = 1
  ) si ON hi.hub_ind_pk = si.hub_ind_pk
),

hub_ind_parsed AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    -- Парсим hub_ind_bk (8 полей): unit_name|data_type|discreteness|value_type|org_name|eat_name|asset_name|ind_code
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(hub_ind_bk, '|', 1), '|', -1)) as unit_name_norm,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(hub_ind_bk, '|', 2), '|', -1)) as data_type_norm,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(hub_ind_bk, '|', 3), '|', -1)) as discreteness_norm,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(hub_ind_bk, '|', 4), '|', -1)) as value_type_norm,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(hub_ind_bk, '|', 7), '|', -1)) as asset_name_norm,
    -- Парсим sat_name (9 полей): unit_name|data_type|metric|discreteness|value_type|org_name|eat_name|asset_name|ind_code
    -- Извлекаем metric из позиции 3
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(COALESCE(sat_name, ''), '|', 3), '|', -1)) as metric_norm,
    load_dttm,
    source_nm
  FROM hub_ind_with_sat
),

hub_ind_with_key AS (
  SELECT
    hub_ind_pk,
    REPLACE(CONCAT_WS('|',
      asset_name_norm,
      REPLACE(unit_name_norm, 'Рубль', 'руб.'),
      CASE 
        WHEN LOWER(data_type_norm) = 'утвержденный' THEN 'Утвержденный'
        WHEN LOWER(data_type_norm) = 'расчетный' THEN 'Расчетный'
        WHEN LOWER(data_type_norm) = 'оперативный' THEN 'Оперативный'
        ELSE CONCAT(UPPER(SUBSTRING(data_type_norm, 1, 1)), LOWER(SUBSTRING(data_type_norm, 2)))
      END,
      CASE 
        WHEN LOWER(metric_norm) = 'факт' THEN 'Факт'
        WHEN LOWER(metric_norm) = 'план' THEN 'План'
        WHEN metric_norm = '' OR metric_norm IS NULL THEN ''
        WHEN metric_norm LIKE 'Факт%' THEN metric_norm
        WHEN metric_norm LIKE 'План%' THEN metric_norm
        ELSE CONCAT(UPPER(SUBSTRING(metric_norm, 1, 1)), LOWER(SUBSTRING(metric_norm, 2)))
      END,
      CASE 
        WHEN LOWER(discreteness_norm) = 'квартал' THEN 'Квартал'
        WHEN LOWER(discreteness_norm) = 'месяц' THEN 'Месяц'
        WHEN LOWER(discreteness_norm) = 'год' THEN 'Год'
        WHEN LOWER(discreteness_norm) = 'полугодие' THEN 'Полугодие'
        WHEN LOWER(discreteness_norm) = 'сутки' THEN 'Сутки'
        ELSE CONCAT(UPPER(SUBSTRING(discreteness_norm, 1, 1)), LOWER(SUBSTRING(discreteness_norm, 2)))
      END,
      value_type_norm
    ), 'Рубль', 'руб.') as hub_ind_bk_alt_normalized,
    load_dttm,
    source_nm
  FROM hub_ind_parsed
),

business_term_data AS (
  SELECT
    business_term_hk,
    fullname,
    load_date,
    source_system
  FROM {{ source('unverified_bdv', 'hub_bdv__business_term') }}
  WHERE fullname IS NOT NULL
),

business_term_normalized AS (
  SELECT
    business_term_hk,
    fullname,
    CONCAT_WS('|',
      TRIM(SUBSTRING_INDEX(fullname, '|', 1)),
      REPLACE(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 2), '|', -1)), 'Рубль', 'руб.'),
      CASE 
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 3), '|', -1))) = 'утвержденный' THEN 'Утвержденный'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 3), '|', -1))) = 'расчетный' THEN 'Расчетный'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 3), '|', -1))) = 'оперативный' THEN 'Оперативный'
        ELSE CONCAT(UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 3), '|', -1)), 1, 1)), LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 3), '|', -1)), 2)))
      END,
      CASE 
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1))) = 'факт' THEN 'Факт'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1))) = 'план' THEN 'План'
        WHEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1)) LIKE 'Факт%' THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1))
        WHEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1)) LIKE 'План%' THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1))
        ELSE CONCAT(UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1)), 1, 1)), LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 4), '|', -1)), 2)))
      END,
      CASE 
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1))) = 'квартал' THEN 'Квартал'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1))) = 'месяц' THEN 'Месяц'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1))) = 'год' THEN 'Год'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1))) = 'полугодие' THEN 'Полугодие'
        WHEN LOWER(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1))) = 'сутки' THEN 'Сутки'
        ELSE CONCAT(UPPER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1)), 1, 1)), LOWER(SUBSTRING(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(fullname, '|', 5), '|', -1)), 2)))
      END,
      TRIM(SUBSTRING_INDEX(fullname, '|', -1))
    ) as fullname_normalized,
    load_date,
    source_system
  FROM business_term_data
),

links_with_hubs AS (
  SELECT
    hi.hub_ind_pk,
    bt.business_term_hk,
    hi.load_dttm,
    bt.load_date,
    COALESCE(hi.source_nm, bt.source_system) as source_nm
  FROM hub_ind_with_key hi
  INNER JOIN business_term_normalized bt
    ON hi.hub_ind_bk_alt_normalized = bt.fullname_normalized
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(business_term_hk AS CHAR))) as link_ind_business_term_pk,
    hub_ind_pk,
    business_term_hk,
    GREATEST(load_dttm, load_date) as load_dttm,
    source_nm
  FROM links_with_hubs
),

deduplicated_links AS (
  SELECT
    link_ind_business_term_pk,
    hub_ind_pk,
    business_term_hk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM link_data
  GROUP BY link_ind_business_term_pk, hub_ind_pk, business_term_hk
)

SELECT
  link_ind_business_term_pk,
  hub_ind_pk,
  business_term_hk,
  load_dttm,
  source_nm
FROM deduplicated_links

