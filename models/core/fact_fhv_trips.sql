{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)


SELECT fhv_tripdata.*,

    pz.borough as pickup_borough, 
    pz.zone as pickup_zone, 
    dz.borough as dropoff_borough, 
    dz.zone as dropoff_zone,  
     from
fhv_tripdata 
join dim_zones as pz on fhv_tripdata.pickup_locationid = pz.locationid
join dim_zones as dz on fhv_tripdata.dropoff_locationid = dz.locationid