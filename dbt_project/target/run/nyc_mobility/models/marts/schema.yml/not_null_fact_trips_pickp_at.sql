
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickp_at
from "awsdatacatalog"."marts"."fact_trips"
where pickp_at is null



  
  
      
    ) dbt_internal_test