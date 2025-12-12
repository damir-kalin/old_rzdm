{{
  config(
    materialized='view')
}}

SELECT 
    forms_hk,
    orgname,
    orginn,
    orgkpp,
    kodform,
    period1,
    period2,
    kodstr,
    kodkol,
    summa,
    src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
from {{ ref('hub_rdv__forms') }}