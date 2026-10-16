{{ config(materialized='view', schema='staging') }}
select t.*, w.temperature_c, w.precipitation_mm, w.weather_category,
 z.borough as pickup_borough, z.zone as pickup_zone
from {{ ref('stg_trips') }} t
left join {{ ref('stg_weather') }} w on date_trunc('hour', t.pickup_at) = w.observed_at
left join {{ source('raw', 'zones') }} z on t.pickup_location_id = z.location_id
