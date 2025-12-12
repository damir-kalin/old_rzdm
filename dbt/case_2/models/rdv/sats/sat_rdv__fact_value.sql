{{ 
  config(
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

-- Satellite для значений фактов
-- Атрибуты: значение показателя
-- Incremental модель: добавляются только новые записи с load_dttm больше максимального в hub_rdv__fact

WITH source_with_fact AS (
  SELECT
    s.value_amt,
    DATE(s.load_dttm) as load_date,
    s.load_dttm,
    s.source_nm,
    hf.hub_fact_pk
  FROM {{ ref('stg_source_data_2') }} s
  INNER JOIN {{ ref('hub_rdv__ind') }} hi ON 
    LOWER(CASE 
      WHEN s.hub_ind_bk_dynamic IS NOT NULL AND s.hub_ind_bk_dynamic != '' 
      THEN CONCAT_WS('|', s.hub_ind_bk_base, s.hub_ind_bk_dynamic)
      ELSE s.hub_ind_bk_base
    END) = hi.hub_ind_bk
  INNER JOIN {{ ref('hub_rdv__fact') }} hf ON 
    hi.hub_ind_pk = hf.bk_hub_ind_pk
    AND s.period_dt = hf.bk_period_dt
    AND COALESCE(s.report_date, '1900-01-01') = hf.bk_report_date
    AND s.source_nm = hf.source_nm
  WHERE s.value_amt IS NOT NULL
  {% if is_incremental() %}
    AND s.load_dttm > (
      SELECT COALESCE(MAX(load_dttm), '1900-01-01') 
      FROM {{ ref('hub_rdv__fact') }}
      WHERE source_nm = s.source_nm
    )
  {% endif %}
),

aggregated_values AS (
  SELECT
    s.hub_fact_pk,
    s.load_date,
    SUM(s.value_amt) as value_amt,
    s.source_nm,
    MAX(s.load_dttm) as load_dttm
  FROM source_with_fact s
  GROUP BY s.hub_fact_pk, s.load_date, s.source_nm
),

row_counts AS (
  SELECT
    source_nm,
    COUNT(*) as file_row_number
  FROM source_with_fact
  GROUP BY source_nm
)

SELECT
  av.hub_fact_pk,
  av.load_date,
  av.value_amt,
  av.source_nm,
  rc.file_row_number,
  CAST(NULL AS VARCHAR(100)) as file_code,
  av.source_nm as file_name,
  CAST(NULL AS VARCHAR(100)) as ind_source_name,
  CAST(NULL AS VARCHAR(100)) as ind_model_code,
  'rdv' as ind_model_name
FROM aggregated_values av
INNER JOIN row_counts rc ON av.source_nm = rc.source_nm

