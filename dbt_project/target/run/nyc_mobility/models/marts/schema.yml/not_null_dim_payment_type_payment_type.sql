
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type
from "awsdatacatalog"."marts"."dim_payment_type"
where payment_type is null



  
  
      
    ) dbt_internal_test