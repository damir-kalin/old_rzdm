

select  
    contractor_debt_hk,
    organization,
    organization_inn,
    organization_kpp,
    organization_mdm_key,
    counterparty,
    counterparty_inn,
    counterparty_kpp,
    counterparty_mdm_key,
    contract,
    amount_owed,
    `timestamp`,
    src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
from `unverified_rdv`.`hub_rdv__contractor_debt`