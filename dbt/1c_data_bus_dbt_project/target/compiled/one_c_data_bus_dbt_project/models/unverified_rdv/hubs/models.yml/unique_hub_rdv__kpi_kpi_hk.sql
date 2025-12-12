
    
    

select
    kpi_hk as unique_field,
    count(*) as n_records

from `unverified_rdv`.`hub_rdv__kpi`
where kpi_hk is not null
group by kpi_hk
having count(*) > 1


