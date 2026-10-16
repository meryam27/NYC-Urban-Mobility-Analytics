{{ config(materialized='table', schema='marts') }}
select location_id, borough, zone, service_zone from {{ source('raw', 'zones') }}

