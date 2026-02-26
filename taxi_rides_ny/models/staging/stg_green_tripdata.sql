-- Green Taxi TripData 모델링

with tripdata as (
  select *
  from {{ source('staging','green_tripdata') }}
  where vendorid is not null 
),

renamed as (
  select
      cast(vendorid as integer)            as vendor_id,
      {{get_vendor_data('vendorid')}} as vendor_name,  -- vendor_id 대신 vendor_name으로 변경
      cast(ratecodeid as integer)          as ratecode_id,
      cast(pulocationid as integer)        as pickup_location_id,
      cast(dolocationid as integer)        as dropoff_location_id,
      cast(lpep_pickup_datetime as timestamp)  as pickup_datetime,
      cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
      store_and_fwd_flag,
      cast(passenger_count as integer)     as passenger_count,
      cast(trip_distance as numeric)       as trip_distance,
      cast(trip_type as integer)           as trip_type,
      cast(fare_amount as numeric)         as fare_amount,
      cast(extra as numeric)               as extra,
      cast(mta_tax as numeric)             as mta_tax,
      cast(tip_amount as numeric)          as tip_amount,
      cast(tolls_amount as numeric)        as tolls_amount,
      cast(ehail_fee as numeric)           as ehail_fee,
      cast(improvement_surcharge as numeric) as improvement_surcharge,
      cast(total_amount as numeric)        as total_amount,
      cast(payment_type as integer)        as payment_type,
      {{get_payment_type_description('payment_type')}} as payment_type_description  -- 추가!
  from tripdata
)
select * from renamed