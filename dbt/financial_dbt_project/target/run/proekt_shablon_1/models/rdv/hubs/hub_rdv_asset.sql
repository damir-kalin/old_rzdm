

  create table `unverified`.`hub_rdv__asset`
      PRIMARY KEY (hub_asset_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- Hub для активов
-- Полное пересоздание таблицы при каждом запуске

WITH form_1_staging AS (
  SELECT DISTINCT
    asset_name as hub_asset_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE asset_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_1'
  
),

form_1_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_asset_bk_raw, CHAR(160), ''))) as hub_asset_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_1_staging
),

form_1_hub AS (
  SELECT
    MD5(hub_asset_bk) as hub_asset_pk, -- hash из lowercase BK
    hub_asset_bk,
    load_dttm,
    source_nm
  FROM form_1_normalized
),

form_2_staging AS (
  SELECT DISTINCT
    asset_name as hub_asset_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE asset_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_2'
  
),

form_2_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_asset_bk_raw, CHAR(160), ''))) as hub_asset_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_2_staging
),

form_2_hub AS (
  SELECT
    MD5(hub_asset_bk) as hub_asset_pk, -- hash из lowercase BK
    hub_asset_bk,
    load_dttm,
    source_nm
  FROM form_2_normalized
),

all_new_hubs AS (
  SELECT * FROM form_1_hub
  UNION ALL
  SELECT * FROM form_2_hub
)

-- APPEND-ONLY: Создаем таблицу с данными при первом запуске
SELECT DISTINCT
  hub_asset_pk,
  hub_asset_bk,
  load_dttm,
  source_nm
FROM all_new_hubs