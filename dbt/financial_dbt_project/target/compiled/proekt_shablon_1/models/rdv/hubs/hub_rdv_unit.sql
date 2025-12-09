

-- Hub для единиц измерения
-- Полное пересоздание таблицы при каждом запуске
WITH form_1_staging AS (
  SELECT DISTINCT
    unit_name as hub_unit_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE unit_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_1'
  
),

form_1_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_unit_bk_raw, CHAR(160), ''))) as hub_unit_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_1_staging
),

form_1_hub AS (
  SELECT
    MD5(hub_unit_bk) as hub_unit_pk, -- hash из lowercase BK
    hub_unit_bk,
    load_dttm,
    source_nm
  FROM form_1_normalized
),

form_2_staging AS (
  SELECT DISTINCT
    unit_name as hub_unit_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE unit_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_2'
  
),

form_2_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_unit_bk_raw, CHAR(160), ''))) as hub_unit_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_2_staging
),

form_2_hub AS (
  SELECT
    MD5(hub_unit_bk) as hub_unit_pk, -- hash из lowercase BK
    hub_unit_bk,
    load_dttm,
    source_nm
  FROM form_2_normalized
),

all_hubs AS (
  SELECT * FROM form_1_hub
  UNION ALL
  SELECT * FROM form_2_hub
),

deduplicated_hubs AS (
  SELECT
    hub_unit_pk,
    hub_unit_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM all_hubs
  GROUP BY hub_unit_pk, hub_unit_bk
)

SELECT
  hub_unit_pk,
  hub_unit_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs