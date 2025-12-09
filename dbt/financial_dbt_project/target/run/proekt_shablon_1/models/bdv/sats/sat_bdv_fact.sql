

  create table `unverified`.`sat_bdv__fact`
      DUPLICATE KEY (hub_fact_pk,load_date)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 

-- BDV Satellite для фактов: копия RDV

SELECT
  hub_fact_pk,
  load_date,
  period_dt,
  report_date,
  source_nm
FROM `unverified`.`sat_rdv__fact`