
    
    

select
    observed_at as unique_field,
    count(*) as n_records

from "awsdatacatalog"."staging"."stg_weather"
where observed_at is not null
group by observed_at
having count(*) > 1


