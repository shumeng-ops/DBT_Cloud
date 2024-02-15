


{{ config(materialized='view') }}
select 
    dispatching_base_num,
    
    cast(pickup_datetime as timestamp) as pickup_datetime,
    
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- trip info
    SR_flag,
    Affiliated_base_number,

    -- payment info



from {{source('staging_2022','fhv_2019')}}
where EXTRACT(year from cast(pickup_datetime as timestamp)) =2019


-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}