
    
    

select
    contractors_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__contractors`
where contractors_hk is not null
group by contractors_hk
having count(*) > 1


