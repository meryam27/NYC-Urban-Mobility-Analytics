
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "awsdatacatalog"."marts"."fact_trips"

where not(total_amount >= 0)


  
  
      
    ) dbt_internal_test