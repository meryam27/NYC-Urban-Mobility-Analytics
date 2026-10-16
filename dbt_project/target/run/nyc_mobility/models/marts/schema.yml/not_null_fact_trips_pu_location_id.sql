
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pu_location_id
from "awsdatacatalog"."marts"."fact_trips"
where pu_location_id is null



  
  
      
    ) dbt_internal_test