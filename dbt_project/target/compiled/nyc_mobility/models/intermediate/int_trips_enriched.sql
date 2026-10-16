
select t.*, w.temperature_c, w.precipitation_mm, w.weather_category,
 z.borough as pickup_borough, z.zone as pickup_zone
from "awsdatacatalog"."staging"."stg_trips" t
left join "awsdatacatalog"."staging"."stg_weather" w on date_trunc('hour', t.pickup_at) = w.observed_at
left join "awsdatacatalog"."raw"."zones" z on t.pickup_location_id = z.location_id