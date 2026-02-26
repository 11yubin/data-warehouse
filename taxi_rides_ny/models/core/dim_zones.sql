-- 택시 구역 정보를 정리한 테이블

{{ config(materialized='table') }}

select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}