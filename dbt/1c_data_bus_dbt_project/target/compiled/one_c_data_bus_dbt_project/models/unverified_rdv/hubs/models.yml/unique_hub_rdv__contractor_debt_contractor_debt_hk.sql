
    
    

select
    contractor_debt_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__contractor_debt`
where contractor_debt_hk is not null
group by contractor_debt_hk
having count(*) > 1


