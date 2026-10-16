{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['pickup_date'],
    format='parquet',
    schema='marts'
) }}
select pickup_at, dropoff_at, duration_min, passenger_count, trip_distance,
 pickup_location_id, dropoff_location_id, payment_type, fare_amount, tip_amount,
 total_amount, temperature_c, precipitation_mm, weather_category,
 pickup_date
from {{ ref('int_trips_enriched') }}
{% if is_incremental() %}
where pickup_date between date '{{ var("start_date") }}' and date '{{ var("end_date") }}'
{% endif %}