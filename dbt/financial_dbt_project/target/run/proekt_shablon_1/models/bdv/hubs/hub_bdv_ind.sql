

  create table `unverified`.`hub_bdv__ind`
      PRIMARY KEY (hub_ind_pk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- BDV Hub для показателей: RDV + расчётные calc из stg_calc_facts

WITH rdv AS (
  SELECT
    hub_ind_pk,
    hub_ind_bk,
    load_dttm,
    source_nm
  FROM `unverified`.`hub_rdv__ind`
),

calc_values AS (
  SELECT DISTINCT
    CAST(bk_hub_ind_pk AS VARCHAR) AS ind_code,
    load_dttm,
    source_nm
  FROM `stage`.`stg_calc_facts`
),

calc_ind AS (
  SELECT DISTINCT
    MD5(LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    ))) AS hub_ind_pk,
    LOWER(CONCAT_WS('|',
      'calc_unit',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      'calc',
      ind_code
    )) AS hub_ind_bk,
    load_dttm,
    source_nm
  FROM calc_values
)

SELECT DISTINCT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM rdv

UNION ALL

SELECT DISTINCT
  hub_ind_pk,
  hub_ind_bk,
  load_dttm,
  source_nm
FROM calc_ind