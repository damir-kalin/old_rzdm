

  create table `unverified`.`hub_rdv__ind`
      PRIMARY KEY (hub_ind_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- Hub для показателей
-- Business Key формируется из составного ключа: hub_unit_pk|hub_data_type_pk|hub_date_type_pk|hub_value_type_pk|hub_organization_pk|hub_economic_activity_type_pk|hub_asset_pk|ind_code
-- Полное пересоздание таблицы при каждом запуске

WITH source_data AS (
  SELECT DISTINCT
    LOWER(s.hub_ind_bk) as hub_ind_bk_lower,  -- приводим BK к lowercase для единого ключа
    s.load_dttm,
    s.source_nm
  FROM t1_stage.stg_source_data_new s
  WHERE s.hub_ind_bk IS NOT NULL
  
),

hub_data AS (
  SELECT
    MD5(s.hub_ind_bk_lower) as hub_ind_pk,  -- hash из lowercase BK
    s.hub_ind_bk_lower as hub_ind_bk,       -- сохраняем BK в lowercase
    s.load_dttm,
    s.source_nm
  FROM source_data s
),

deduplicated_hubs AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM hub_data
  GROUP BY hub_ind_pk, hub_ind_bk
)

SELECT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM deduplicated_hubs
LIMIT 25000