
    
    

select
    forms_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__forms`
where forms_hk is not null
group by forms_hk
having count(*) > 1


