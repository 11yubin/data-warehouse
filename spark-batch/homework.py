from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Homework") \
    .getOrCreate()

# Q1
print(f"Q1. Spark Version: {spark.version}")

# Q2
# 데이터 로드
df_yellow = spark.read.parquet("data/yellow_tripdata_2025-11.parquet")

# 4개 파티션으로 나누어 저장
df_yellow.repartition(4).write.mode("overwrite").parquet("yellow_oct_2025_repartitioned")

# terminal에서 파일 크기 확인
# ls -lh yellow_oct_2025_repartitioned/

# Q3
# 2025-11-15 시작된 운행 필터링
nov_15_count = df_yellow.filter(F.to_date(df_yellow.tpep_pickup_datetime) == "2025-11-15").count()

print(f"Q3. Trips on Nov 15th: {nov_15_count}")

# Q4
# (종료시간 - 시작시간)을 초 단위로 계산 후 시간(Hour)으로 변환
df_with_duration = df_yellow.withColumn(
    "trip_duration_hrs",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)

longest_trip = df_with_duration.select(F.max("trip_duration_hrs")).collect()[0][0]
print(f"Q4. Longest trip: {longest_trip} hours")

# Q6
# 존 데이터 로드
df_zones = spark.read.option("header", "true").csv("data/taxi_zone_lookup.csv")

# 픽업 위치별 카운트 계산
pickup_counts = df_yellow.groupBy("PULocationID").count()

# 존 데이터와 조인하여 구역 이름 가져오기
least_frequent_zone = pickup_counts.join(df_zones, pickup_counts.PULocationID == df_zones.LocationID) \
    .orderBy("count", ascending=True) \
    .select("Zone", "count") \
    .limit(1)

least_frequent_zone.show(truncate=False)
print(f"Q6. Least frequent zone: {least_frequent_zone.collect()[0][0]}")