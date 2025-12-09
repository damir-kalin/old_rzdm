

-- Hub для организаций
-- Полное пересоздание таблицы при каждом запуске
WITH form_1_staging AS (
  SELECT DISTINCT
    org_name as hub_organization_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE org_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_1'
  
),

form_1_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_organization_bk_raw, CHAR(160), ''))) as hub_organization_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_1_staging
),

form_1_hub AS (
  SELECT
    MD5(hub_organization_bk) as hub_organization_pk, -- hash из lowercase BK
    hub_organization_bk,
    load_dttm,
    source_nm
  FROM form_1_normalized
),

form_2_staging AS (
  SELECT DISTINCT
    org_name as hub_organization_bk_raw,
    load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new
  WHERE org_name IS NOT NULL
    AND source_nm = 'raw.accounting_balance_form_2'
  
),

form_2_normalized AS (
  SELECT
    LOWER(TRIM(REPLACE(hub_organization_bk_raw, CHAR(160), ''))) as hub_organization_bk, -- BK в lowercase
    load_dttm,
    source_nm
  FROM form_2_staging
),

form_2_hub AS (
  SELECT
    MD5(hub_organization_bk) as hub_organization_pk, -- hash из lowercase BK
    hub_organization_bk,
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
    hub_organization_pk,
    hub_organization_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM all_hubs
  GROUP BY hub_organization_pk, hub_organization_bk
)

SELECT
  hub_organization_pk,
  hub_organization_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs