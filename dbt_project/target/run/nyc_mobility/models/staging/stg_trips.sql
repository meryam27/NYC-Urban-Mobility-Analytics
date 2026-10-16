create or replace view
    "awsdatacatalog"."staging"."stg_trips"
  as
    
select vendor_id, pickup_at, dropoff_at, date(pickup_at) as pickup_date,
 date_diff('minute', pickup_at, dropoff_at) as duration_min,
 passenger_count, trip_distance, pickup_location_id, dropoff_location_id,
 payment_type, fare_amount, tip_amount, tolls_amount, total_amount
from "awsdatacatalog"."raw"."trips"
where trip_distance > 0 and fare_amount > 0
 and dropoff_at > pickup_at
 and date_diff('hour', pickup_at, dropoff_at) < 24
