-- [1] 파티션 없는 척하는 테이블 (Non-partitioned)
-- 데이터를 무작위로 섞어서 저장하면, 날짜 검색할 때 전체를 다 뒤져야 함
CREATE OR REPLACE TABLE yellow_tripdata_non_partitioned AS
SELECT * FROM yellow_tripdata_2024
ORDER BY UUID_STRING(); -- 랜덤 정렬 (최악의 효율 유도)

-- [2] 파티션(클러스터링) 잘 된 테이블 (Partitioned)
-- 날짜 기준으로 예쁘게 정렬해서 저장
CREATE OR REPLACE TABLE yellow_tripdata_partitioned 
CLUSTER BY ("tpep_dropoff_datetime") AS
SELECT * FROM yellow_tripdata_2024
ORDER BY "tpep_dropoff_datetime";

-- 캐시 끄기
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- [Test A] 섞인 테이블 조회 (느리고 무거움)
SELECT DISTINCT "VendorID"
FROM yellow_tripdata_non_partitioned
WHERE "tpep_dropoff_datetime" BETWEEN '2024-03-01' AND '2024-03-15';

-- [Test B] 정리된 테이블 조회 (빠르고 가벼움)
SELECT DISTINCT "VendorID"
FROM yellow_tripdata_partitioned
WHERE "tpep_dropoff_datetime" BETWEEN '2024-03-01' AND '2024-03-15';