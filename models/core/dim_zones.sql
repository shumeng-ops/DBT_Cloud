{{config(materialized='table')}}

select 
    locationid, 
    borough, 
    zone, 
    --add comments
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('taxi_zone_lookup') }}