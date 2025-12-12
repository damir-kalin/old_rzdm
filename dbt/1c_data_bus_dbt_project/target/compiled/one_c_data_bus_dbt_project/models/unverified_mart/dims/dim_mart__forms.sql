

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
from `unverified_rdv`.`hub_rdv__forms`