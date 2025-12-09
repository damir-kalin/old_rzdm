

-- Link между показателем и фактом
-- PK формируется как MD5(hub_ind_pk|hub_fact_pk)
-- Полное пересоздание таблицы при каждом запуске

WITH hub_fact_data AS (
  SELECT
    hub_fact_pk,
    bk_hub_ind_pk as hub_ind_pk,
    load_dttm,
    source_nm
  FROM `unverified`.`hub_rdv__fact`
  
),

link_data AS (
  SELECT
    MD5(CONCAT_WS('|', CAST(hub_ind_pk AS CHAR), CAST(hub_fact_pk AS CHAR))) as link_ind_fact_pk,
    hub_ind_pk,
    hub_fact_pk,
    load_dttm,
    source_nm
  FROM hub_fact_data
),

deduplicated_links AS (
  SELECT
    link_ind_fact_pk,
    hub_ind_pk,
    hub_fact_pk,
    MAX(load_dttm) as load_dttm,
    ANY_VALUE(source_nm) as source_nm
  FROM link_data
  GROUP BY link_ind_fact_pk, hub_ind_pk, hub_fact_pk
)

SELECT
  link_ind_fact_pk,
  hub_ind_pk,
  hub_fact_pk,
  load_dttm,
  source_nm
FROM deduplicated_links