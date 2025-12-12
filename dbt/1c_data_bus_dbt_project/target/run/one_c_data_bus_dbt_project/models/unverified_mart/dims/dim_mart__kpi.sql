
  create view `unverified_mart`.`dim_mart__kpi__dbt_tmp` as 

SELECT
    kpi_hk,
    orgname,
    orginn,
    orgogrn,
    kodform,
    nameform,
    period1,
    period2,
    src_sys_id,
    nomstroki,
    nom_pokazatelya,
    naimenovanie_chuz,
    naimenovanie_gruppy,
    naimenovanie_pokazatelya,
    kategoriya,
    edinitsa_izmereniya,
    plan,
    fakt,
    protsent_vypolneniya,
    CURRENT_TIMESTAMP as load_dttm
from `unverified_rdv`.`hub_rdv__kpi`;