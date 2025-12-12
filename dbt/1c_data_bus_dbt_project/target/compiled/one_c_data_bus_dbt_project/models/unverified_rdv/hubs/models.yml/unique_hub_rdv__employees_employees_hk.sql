
    
    

select
    employees_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__employees`
where employees_hk is not null
group by employees_hk
having count(*) > 1


