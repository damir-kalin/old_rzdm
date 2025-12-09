

  create table `unverified`.`link_rdv__ind_accounting_fin_indicator`
      PRIMARY KEY (link_ind_accounting_fin_indicator_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- Link между показателем и учетно-финансовым показателем
-- PK формируется как MD5(hub_ind_pk|hub_accounting_fin_indicator_pk)
-- Полное пересоздание таблицы при каждом запуске

WITH source_data AS (
  SELECT DISTINCT
    TRIM(REPLACE(s.unit_name, CHAR(160), '')) as unit_name_norm,
    TRIM(REPLACE(s.data_type, CHAR(160), '')) as data_type_norm,
    TRIM(REPLACE(COALESCE(CAST(s.metric AS CHAR), ''), CHAR(160), '')) as metric_norm,
    TRIM(REPLACE(s.discreteness, CHAR(160), '')) as discreteness_norm,
    TRIM(REPLACE(s.value_type, CHAR(160), '')) as value_type_norm,
    TRIM(REPLACE(s.org_name, CHAR(160), '')) as org_name_norm,
    TRIM(REPLACE(s.eat_name, CHAR(160), '')) as eat_name_norm,
    TRIM(REPLACE(s.asset_name, CHAR(160), '')) as asset_name_norm,
    TRIM(REPLACE(s.ind_code, CHAR(160), '')) as ind_code_norm,
    s.load_dttm,
    s.source_nm
  FROM t1_stage.stg_source_data_new s
  WHERE s.unit_name IS NOT NULL 
    AND s.data_type IS NOT NULL
    AND s.metric IS NOT NULL
    AND s.discreteness IS NOT NULL
    AND s.value_type IS NOT NULL
    AND s.org_name IS NOT NULL 
    AND s.eat_name IS NOT NULL 
    AND s.asset_name IS NOT NULL
    AND s.ind_code IS NOT NULL
  
),

hub_ind_data AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    source_nm
  FROM `unverified`.`hub_rdv__ind`
),

hub_afi_data AS (
  SELECT
    hub_accounting_fin_indicator_pk,
    hub_accounting_fin_indicator_bk,
    source_nm
  FROM `unverified`.`hub_rdv__accounting_fin_indicator`
),

source_with_hubs AS (
  SELECT
    hi.hub_ind_pk,
    hafi.hub_accounting_fin_indicator_pk,
    s.load_dttm,
    s.source_nm
  FROM source_data s
  INNER JOIN hub_ind_data hi ON 
    CONCAT_WS('|',
      s.unit_name_norm,
      s.data_type_norm,
      s.discreteness_norm,
      s.value_type_norm,
      s.org_name_norm,
      s.eat_name_norm,
      s.asset_name_norm,
      s.ind_code_norm
    ) = hi.hub_ind_bk
    AND s.source_nm = hi.source_nm
  INNER JOIN hub_afi_data hafi ON
    CONCAT_WS('|',
      s.unit_name_norm,
      s.data_type_norm,
      s.metric_norm,
      s.discreteness_norm,
      s.value_type_norm,
      s.org_name_norm,
      s.eat_name_norm,
      s.asset_name_norm,
      s.ind_code_norm
    ) = hafi.hub_accounting_fin_indicator_bk
    AND s.source_nm = hafi.source_nm
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_accounting_fin_indicator_pk AS CHAR))) as link_ind_accounting_fin_indicator_pk,
    hub_ind_pk,
    hub_accounting_fin_indicator_pk,
    load_dttm,
    source_nm
  FROM source_with_hubs
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