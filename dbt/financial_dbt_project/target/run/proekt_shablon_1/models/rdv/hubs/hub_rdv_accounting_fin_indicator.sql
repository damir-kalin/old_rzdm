

  create table `unverified`.`hub_rdv__accounting_fin_indicator`
      PRIMARY KEY (hub_accounting_fin_indicator_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- Hub для учетно-финансовых показателей
-- Полное пересоздание таблицы при каждом запуске

WITH source_data AS (
  SELECT
    LOWER(
  CONCAT_WS('|', 
    TRIM(REPLACE(COALESCE(CAST(unit_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(data_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(metric AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(discreteness AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(value_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(org_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(eat_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(asset_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(ind_code AS CHAR), ''), CHAR(160), ''))
  )
) as hub_accounting_fin_indicator_bk, -- BK в lowercase
    MIN(load_dttm) as load_dttm,
    source_nm
  FROM t1_stage.stg_source_data_new s
  WHERE unit_name IS NOT NULL 
    AND data_type IS NOT NULL
    AND metric IS NOT NULL
    AND discreteness IS NOT NULL
    AND value_type IS NOT NULL
    AND org_name IS NOT NULL 
    AND eat_name IS NOT NULL 
    AND asset_name IS NOT NULL
    AND ind_code IS NOT NULL
  
  GROUP BY 
  CONCAT_WS('|', 
    TRIM(REPLACE(COALESCE(CAST(unit_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(data_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(metric AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(discreteness AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(value_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(org_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(eat_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(asset_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST(ind_code AS CHAR), ''), CHAR(160), ''))
  )
, source_nm
),

hub_data AS (
  SELECT
    MD5(hub_accounting_fin_indicator_bk) as hub_accounting_fin_indicator_pk, -- hash из lowercase BK
    hub_accounting_fin_indicator_bk,
    load_dttm,
    source_nm
  FROM source_data
),

deduplicated_hubs AS (
  SELECT
    hub_accounting_fin_indicator_pk,
    hub_accounting_fin_indicator_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM hub_data
  GROUP BY hub_accounting_fin_indicator_pk, hub_accounting_fin_indicator_bk
)

SELECT
  hub_accounting_fin_indicator_pk,
  hub_accounting_fin_indicator_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs
LIMIT 25000