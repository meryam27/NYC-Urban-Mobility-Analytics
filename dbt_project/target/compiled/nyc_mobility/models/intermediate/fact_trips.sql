
select pickup_at, dropoff_at, duration_min, passenger_count, trip_distance,
 pickup_location_id, dropoff_location_id, payment_type, fare_amount, tip_amount,
 total_amount, temperature_c, precipitation_mm, weather_category,
 pickup_date
from "awsdatacatalog"."staging"."int_trips_enriched"

where pickup_date between date '2025-01-01' and date '2025-01-31'
