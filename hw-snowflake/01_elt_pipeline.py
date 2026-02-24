# 01_elt_pipeline.py
# snowflake를 이용해서 ELT 파이프라인을 구축하는 코드
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# 설정 확인
if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT]):
    raise ValueError("❌ .env 파일에 접속 정보가 없거나 읽지 못했습니다!")

DOWNLOAD_DIR = "./data"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse='COMPUTE_WH'
    )

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # 이미 파일 있으면 다운로드 스킵
    if os.path.exists(file_path):
        print(f"⏭️ {month}월 파일 이미 있음. 다운로드 스킵.")
        return file_path
    
    try:
        print(f"📥 Downloading {month}월...")
        urllib.request.urlretrieve(url, file_path)
        return file_path
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

def main():
    conn = None
    try:
        print("🔌 Connecting to Snowflake...")
        conn = get_snowflake_conn()
        cur = conn.cursor()
        
        # 1. 기본 설정 (DB, Schema, Stage)
        cur.execute("CREATE DATABASE IF NOT EXISTS NY_TAXI")
        cur.execute("CREATE SCHEMA IF NOT EXISTS NY_TAXI.TRIPDATA")
        cur.execute("USE SCHEMA NY_TAXI.TRIPDATA")
        cur.execute("CREATE OR REPLACE FILE FORMAT my_parquet_format TYPE = PARQUET")
        cur.execute("CREATE STAGE IF NOT EXISTS my_local_stage FILE_FORMAT = my_parquet_format")
        
        # 2. 파일 다운로드 & 업로드
        print("🚀 Checking & Uploading files...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            files = list(executor.map(download_file, MONTHS))
        
        valid_files = [f for f in files if f]
        for f in valid_files:
            f_path = os.path.abspath(f).replace('\\', '/')
            print(f"   -> Pushing {os.path.basename(f)}...")
            cur.execute(f"PUT 'file://{f_path}' @my_local_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

        # 3. [1단계] 임시 테이블(RAW) 생성
        # 일단 Parquet 있는 그대로(외계어 상태로) 다 때려 넣음
        print("🏗️ Creating RAW table...")
        cur.execute("""
            CREATE OR REPLACE TABLE yellow_tripdata_raw
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA(LOCATION=>'@my_local_stage', FILE_FORMAT=>'my_parquet_format'))
            )
        """)
        
        cur.execute("""
            COPY INTO yellow_tripdata_raw
            FROM @my_local_stage
            FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)

        # 4. [2단계 - Transform] 변환해서 진짜 테이블 생성 (CTAS)
        
        print("✨ Transforming data & Creating Final Table...")
        cur.execute("""
            CREATE OR REPLACE TABLE yellow_tripdata_2024 AS
            SELECT
                -- 1. 날짜 변환 (마이크로초 -> 타임스탬프)
                -- (주의: 컬럼명은 쿼리 호환성을 위해 원본 이름 그대로 유지하는 게 좋음)
                TO_TIMESTAMP_NTZ("tpep_pickup_datetime" / 1000000) AS "tpep_pickup_datetime",
                TO_TIMESTAMP_NTZ("tpep_dropoff_datetime" / 1000000) AS "tpep_dropoff_datetime",
                
                -- 2. 금액 변환 (지수표기법 -> 소수점 2자리)
                CAST("fare_amount" AS DECIMAL(10, 2)) AS "fare_amount",
                CAST("total_amount" AS DECIMAL(10, 2)) AS "total_amount",
                
                -- 3. 나머지 컬럼들은 그대로 가져오기 (EXCLUDE 기능 사용)
                * EXCLUDE ("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "total_amount")
            FROM yellow_tripdata_raw
        """)

        # 5. [변환 검증] 변환이 잘 되었는지 확인
        print("\n✅ 변환 검증 중...")
        
        # 5-1. 총 레코드 수 확인
        cur.execute("SELECT COUNT(*) FROM yellow_tripdata_2024")
        count = cur.fetchone()[0]
        print(f"   📊 총 데이터: {count:,} 건")
        
        # 5-2. 데이터 샘플 확인 (날짜 & 금액이 제대로 변환되었는지)
        cur.execute("""
            SELECT 
                "tpep_pickup_datetime", 
                "tpep_dropoff_datetime",
                "fare_amount",
                "total_amount" 
            FROM yellow_tripdata_2024 LIMIT 3
        """)
        samples = cur.fetchall()
        print(f"   📋 샘플 데이터 (처음 3건):")
        for row in samples:
            print(f"      - Pickup: {row[0]}, Dropoff: {row[1]}, Fare: ${row[2]:.2f}, Total: ${row[3]:.2f}")
        
        # 5-3. 데이터 타입 & 통계 확인
        cur.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT DATE("tpep_pickup_datetime")) as unique_dates,
                MIN("total_amount") as min_amount,
                MAX("total_amount") as max_amount,
                AVG("total_amount") as avg_amount
            FROM yellow_tripdata_2024
        """)
        stats = cur.fetchone()
        print(f"\n   📈 데이터 통계:")
        print(f"      - 총 행 수: {stats[0]:,}")
        print(f"      - 운행 일수: {stats[1]}")
        print(f"      - 최소 금액: ${stats[2]:.2f}")
        print(f"      - 최대 금액: ${stats[3]:.2f}")
        print(f"      - 평균 금액: ${stats[4]:.2f}")
        
        # 5-4. 사용자 확인 - 계속 진행할지 결정
        print("\n" + "=" * 50)
        user_input = input("✋ 변환 데이터가 정상입니다. 저장을 계속 진행하시겠습니까? (yes/no): ").strip().lower()
        print("=" * 50)
        
        if user_input != 'yes':
            print("⏸️  작업이 중단되었습니다. 데이터는 저장되지 않았습니다.")
            cur.execute("DROP TABLE IF EXISTS yellow_tripdata_2024")
            cur.execute("DROP TABLE IF EXISTS yellow_tripdata_raw")
            return
        
        # 6. 임시 테이블 삭제 (청소)
        cur.execute("DROP TABLE IF EXISTS yellow_tripdata_raw")
        print(f"\n✨ 임시 테이블 정리 완료")
        
        # 7. 최종 저장 확인
        print("-" * 50)
        print(f"🎉 변환 및 저장 완료! yellow_tripdata_2024 테이블 저장됨")
        print("-" * 50)
        
    except Exception as e:
        print(f"\n🔥 에러 발생: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()