



select
    1
from "awsdatacatalog"."marts"."fact_trips"

where not(total_amount >= 0)

