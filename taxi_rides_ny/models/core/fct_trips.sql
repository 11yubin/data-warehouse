-- 택시 운행 정보 + 구역 정보 조인하여 정리한 테이블
-- models/core/fct_trips.sql

{{ config(
    materialized='table',
    schema='core'
) }}

with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

-- Step 1: surrogate key 생성
with_trip_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_location_id',
            'dropoff_location_id'
        ]) }} as trip_id,
        *
    from trips
),

-- Step 2: 중복 제거
deduplicated as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY pickup_datetime
        ) as rn
    from with_trip_id
),

final as (
    select
        -- Primary Key
        trip_id,

        -- 식별자
        vendor_id,
        ratecode_id,
        pickup_location_id,
        dropoff_location_id,

        -- 시간
        pickup_datetime,
        dropoff_datetime,

        -- 운행 정보
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,

        -- 결제 정보
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        payment_type_description,  -- staging macro에서 이미 생성됨

        -- 서비스 구분
        service_type

    from deduplicated
    where rn = 1  -- 중복 제거 핵심
)

select * from final