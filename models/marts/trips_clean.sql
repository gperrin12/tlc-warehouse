{{ config(materialized='table') }}

with raw as (
    select * from {{ source('tlc_raw', 'tlc_2015_latlon') }}
),

cleaned as (
    select *
    from raw
    where
        total_amount >= 0
        and total_amount < 500
        and trip_distance > 0
        and trip_distance < 200
        and pickup_latitude between 40.4 and 40.95
        and pickup_longitude between -74.3 and -73.6
        and dropoff_latitude between 40.4 and 40.95
        and dropoff_longitude between -74.3 and -73.6
)

select * from cleaned