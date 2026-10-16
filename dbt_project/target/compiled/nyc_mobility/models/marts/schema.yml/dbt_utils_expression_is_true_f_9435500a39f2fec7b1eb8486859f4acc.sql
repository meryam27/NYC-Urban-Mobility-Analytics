



select
    1
from "awsdatacatalog"."marts"."fact_trips"

where not(duration_min >= 0 and duration_min < 1440)

