
  create view `unverified_mart`.`dim_mart__employees__dbt_tmp` as 

SELECT 
	employees_hk,
    organization,
    organization_inn,
    organization_kpp,
    organization_mdm_key,
    division,
    division_mdm_key,
    employee,
    table_number,
    post,
    gender,
    type_of_employment,
    `timestamp`,
    src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
from `unverified_rdv`.`hub_rdv__employees`;