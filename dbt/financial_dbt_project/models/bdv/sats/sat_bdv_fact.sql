{{
  config(
    alias='sat_bdv__fact',
    materialized='table',
    table_type='DUPLICATE',
    keys=['hub_fact_pk', 'load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- BDV Satellite для фактов: копия RDV

SELECT
  hub_fact_pk,
  load_date,
  period_dt,
  report_date,
  source_nm
FROM {{ ref('sat_rdv_fact') }}

