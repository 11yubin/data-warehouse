import os
import snowflake.connector
from dotenv import load_dotenv

# .env 로드
load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='COMPUTE_WH',
        database='NY_TAXI',
        schema='TRIPDATA'
    )

def ingest_data():
    conn = get_snowflake_conn()
    cur = conn.cursor()
    
    try:
        # 1. 환경 설정
        cur.execute("CREATE DATABASE IF NOT EXISTS NY_TAXI")
        cur.execute("CREATE SCHEMA IF NOT EXISTS NY_TAXI.TRIPDATA")
        cur.execute("USE SCHEMA NY_TAXI.TRIPDATA")
        cur.execute("CREATE OR REPLACE FILE FORMAT my_parquet_format TYPE = PARQUET")
        cur.execute("CREATE STAGE IF NOT EXISTS my_local_stage FILE_FORMAT = my_parquet_format")

        taxi_types = ['green', 'yellow']
        
        for taxi in taxi_types:
            print(f"🚀 Processing {taxi} trip data...")
            
            # 로컬 data 폴더 경로
            local_path = os.path.abspath(f"data/{taxi}/*.parquet").replace('\\', '/')
            
            # 2. Snowflake Stage로 파일 업로드
            print(f"   -> Uploading {taxi} parquet files to stage...")
            cur.execute(f"PUT 'file://{local_path}' @my_local_stage/{taxi}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            # 3. Raw 테이블 생성 (Schema Inference 활용)
            print(f"   -> Creating {taxi}_tripdata_raw...")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {taxi}_tripdata_raw
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(INFER_SCHEMA(LOCATION=>'@my_local_stage/{taxi}/', FILE_FORMAT=>'my_parquet_format'))
                )
            """)
            
            cur.execute(f"""
                COPY INTO {taxi}_tripdata_raw
                FROM @my_local_stage/{taxi}/
                FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """)

            # 4. dbt가 사용할 최종 테이블 생성 (대소문자 문제 해결을 위해 대문자로 강제 변환)
            print(f"   -> Creating final {taxi}_tripdata for dbt (Standardizing Column Names)...")
            
            # 컬럼 정보를 가져와서 대문자로 변환하는 쿼리 생성
            cur.execute(f"SHOW COLUMNS IN TABLE {taxi}_tripdata_raw")
            columns = [row[2] for row in cur.fetchall()] # column_name은 3번째 컬럼(index 2)
            
            # 대문자로 변환된 컬럼 리스트 생성 (dbt 호환용)
            select_list = []
            pickup_col = "lpep_pickup_datetime" if taxi == 'green' else "tpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime" if taxi == 'green' else "tpep_dropoff_datetime"

            for col in columns:
                clean_col = col.strip('"') # 기존 따옴표 제거
                
                # 날짜 컬럼은 타임스탬프 변환 로직 적용
                if clean_col.lower() == pickup_col:
                    select_list.append(f'CASE WHEN TYPEOF("{clean_col}") = \'INTEGER\' THEN TO_TIMESTAMP_NTZ("{clean_col}" / 1000000) ELSE CAST("{clean_col}" AS TIMESTAMP_NTZ) END AS {pickup_col.upper()}')
                elif clean_col.lower() == dropoff_col:
                    select_list.append(f'CASE WHEN TYPEOF("{clean_col}") = \'INTEGER\' THEN TO_TIMESTAMP_NTZ("{clean_col}" / 1000000) ELSE CAST("{clean_col}" AS TIMESTAMP_NTZ) END AS {dropoff_col.upper()}')
                else:
                    # 나머지 모든 컬럼은 이름을 대문자로 바꿔서 (AS 뒤에 따옴표 없이) 선택
                    select_list.append(f'"{clean_col}" AS {clean_col.upper()}')

            sql_final = f"""
                CREATE OR REPLACE TABLE {taxi}_tripdata AS
                SELECT
                    {", ".join(select_list)}
                FROM {taxi}_tripdata_raw
            """
            cur.execute(sql_final)
            
            # 임시 테이블 삭제
            cur.execute(f"DROP TABLE {taxi}_tripdata_raw")
            print(f"✅ {taxi}_tripdata created with standardized (UPPERCASE) columns.")

        print("\n🎉 All data ingested. Now run 'dbt run' in your dbt project!")

    except Exception as e:
        print(f"🔥 Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    ingest_data()