

  create table `unverified`.`link_rdv__ind_economic_activity_type`
      PRIMARY KEY (link_ind_economic_activity_type_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- Link между показателем и видом экономической деятельности
-- PK формируется как MD5(hub_ind_pk|hub_economic_activity_type_pk)
-- Полное пересоздание таблицы при каждом запуске

WITH staging_normalized AS (
  SELECT
    TRIM(REPLACE(s.unit_name, CHAR(160), '')) as unit_name_norm,
    TRIM(REPLACE(s.data_type, CHAR(160), '')) as data_type_norm,
    TRIM(REPLACE(s.discreteness, CHAR(160), '')) as discreteness_norm,
    TRIM(REPLACE(s.value_type, CHAR(160), '')) as value_type_norm,
    TRIM(REPLACE(s.org_name, CHAR(160), '')) as org_name_norm,
    TRIM(REPLACE(s.eat_name, CHAR(160), '')) as eat_name_norm,
    TRIM(REPLACE(s.asset_name, CHAR(160), '')) as asset_name_norm,
    TRIM(REPLACE(s.ind_code, CHAR(160), '')) as ind_code_norm,
    MAX(s.load_dttm) as load_dttm,
    ANY_VALUE(s.source_nm) as source_nm
  FROM t1_stage.stg_source_data_new s
  WHERE s.ind_code IS NOT NULL
    AND s.eat_name IS NOT NULL
    AND s.unit_name IS NOT NULL
    AND s.data_type IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.asset_name IS NOT NULL
  
  GROUP BY
    TRIM(REPLACE(s.unit_name, CHAR(160), '')),
    TRIM(REPLACE(s.data_type, CHAR(160), '')),
    TRIM(REPLACE(s.discreteness, CHAR(160), '')),
    TRIM(REPLACE(s.value_type, CHAR(160), '')),
    TRIM(REPLACE(s.org_name, CHAR(160), '')),
    TRIM(REPLACE(s.eat_name, CHAR(160), '')),
    TRIM(REPLACE(s.asset_name, CHAR(160), '')),
    TRIM(REPLACE(s.ind_code, CHAR(160), ''))
),

links_with_hubs AS (
  SELECT
    hi.hub_ind_pk,
    h.hub_economic_activity_type_pk,
    s.load_dttm,
    s.source_nm
  FROM staging_normalized s
  INNER JOIN `unverified`.`hub_rdv__ind` hi ON hi.hub_ind_bk = CONCAT_WS('|', 
    s.unit_name_norm,
    s.data_type_norm,
    s.discreteness_norm,
    s.value_type_norm,
    s.org_name_norm,
    s.eat_name_norm,
    s.asset_name_norm,
    s.ind_code_norm
  )
  INNER JOIN `unverified`.`hub_rdv__economic_activity_type` h ON h.hub_economic_activity_type_bk = s.eat_name_norm
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_economic_activity_type_pk AS CHAR))) as link_ind_economic_activity_type_pk,
    hub_ind_pk,
    hub_economic_activity_type_pk,
    load_dttm,
    source_nm
  FROM links_with_hubs
),

deduplicated_links AS (
  SELECT
    link_ind_economic_activity_type_pk,
    hub_ind_pk,
    hub_economic_activity_type_pk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM link_data
  GROUP BY link_ind_economic_activity_type_pk, hub_ind_pk, hub_economic_activity_type_pk
)

SELECT
  link_ind_economic_activity_type_pk,
  hub_ind_pk,
  hub_economic_activity_type_pk,
  load_dttm,
  source_nm
FROM deduplicated_links