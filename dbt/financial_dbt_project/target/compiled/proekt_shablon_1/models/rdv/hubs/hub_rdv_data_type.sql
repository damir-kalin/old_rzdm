

-- Hub для справочника "Тип данных"
-- Полное пересоздание таблицы при каждом запуске
WITH form_1_staging AS (
  SELECT DISTINCT
    data_type as hub_data_type_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE data_type IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_1'
  
),

form_1_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_data_type_bk_raw, CHAR(160), ''))) as hub_data_type_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_1_staging
),

form_1_hub AS (
  SELECT
    MD5(hub_data_type_bk) as hub_data_type_pk, -- hash из lowercase BK
    hub_data_type_bk,
    load_dttm,
    source_nm
  FROM form_1_normalized
),

form_2_staging AS (
  SELECT DISTINCT
    data_type as hub_data_type_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE data_type IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_2'
  
),

form_2_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_data_type_bk_raw, CHAR(160), ''))) as hub_data_type_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_2_staging
),

form_2_hub AS (
  SELECT
    MD5(hub_data_type_bk) as hub_data_type_pk, -- hash из lowercase BK
    hub_data_type_bk,
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
    hub_data_type_pk,
    hub_data_type_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM all_hubs
  GROUP BY hub_data_type_pk, hub_data_type_bk
)

SELECT
  hub_data_type_pk,
  hub_data_type_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs