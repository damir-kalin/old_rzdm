{{
  config(
    alias='sat_bdv__fact',
    materialized='incremental',
    unique_key=['hub_fact_pk', 'load_date'],
    table_type='DUPLICATE',
    keys=['hub_fact_pk', 'load_date'],
    buckets=3,
    properties={
      'replication_num': '1'
    }
  )
}}

-- BDV Satellite для фактов: копия RDV
-- Incremental модель: добавляются только новые записи с load_date больше максимального в таблице

SELECT
  hub_fact_pk,
  load_date,
  period_dt,
  report_date,
  source_nm
FROM {{ ref('sat_rdv__fact') }} s
{% if is_incremental() %}
  WHERE s.load_date > (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
{% endif %}

