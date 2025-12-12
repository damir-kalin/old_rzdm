
    
    

select
    organizations_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__organizations`
where organizations_hk is not null
group by organizations_hk
having count(*) > 1


