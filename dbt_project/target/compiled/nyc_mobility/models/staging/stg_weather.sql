
select observed_at, temperature_c, precipitation_mm, wind_speed_kmh,
 case when weather_code in (71,73,75,77,85,86) then 'neige'
 when weather_code between 51 and 67
 or weather_code between 80 and 82 or weather_code >= 95 then 'pluie'
 else 'sec' end as weather_category
from "awsdatacatalog"."raw"."weather"