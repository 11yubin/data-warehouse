# 02_time_travel_demo.py
# Snoflake의 Time Travel 기능을 사용해 삭제된 테이블을 복구하는 코드
import os
import time
import snowflake.connector
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse='COMPUTE_WH',
        database='NY_TAXI',   # 01_elt_pipeline.py에서 만든 DB
        schema='TRIPDATA'     # 01_elt_pipeline.py에서 만든 Schema
    )

def main():
    conn = None
    try:
        print("🔌 Snowflake에 연결 중...")
        conn = get_snowflake_conn()
        cur = conn.cursor()

        target_table = "yellow_tripdata_2024"

        # 1. 원본 데이터 확인
        print(f"\n[Step 1] 원본 테이블({target_table}) 상태 확인")
        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        count = cur.fetchone()[0]
        print(f"   📊 현재 데이터 건수: {count:,} 건")

        # 2. 고의로 테이블 삭제 (Disaster Simulation)
        time.sleep(1)
        print("\n[Step 2] 🚨 치명적인 실수 발생: 주니어 엔지니어가 운영 테이블을 DROP 했습니다!")
        cur.execute(f"DROP TABLE {target_table}")
        print(f"   💥 테이블({target_table})이 완전히 삭제되었습니다.")

        # 삭제 확인 (에러가 나야 정상임을 보여줌)
        try:
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"   ❌ 조회 실패 (테이블이 삭제되었기 때문에, 존재하지 않음)")

        time.sleep(2)

        # 3. Time Travel로 1초 만에 복구 (UNDROP)
        print("\n[Step 3] ⏱️ Snowflake Time Travel을 이용한 즉시 복구")
        print("   ✨ 백업 스토리지 복원 없이 UNDROP 명령어 단 한 줄로 복구합니다...")
        cur.execute(f"UNDROP TABLE {target_table}")

        cur.execute(f"SELECT COUNT(*) FROM {target_table}")
        recovered_count = cur.fetchone()[0]
        print(f"   ✅ 복구 완료! 복원된 데이터 건수: {recovered_count:,} 건 (손실률 0%)")

        time.sleep(2)

        # 4. Zero-Copy Clone 기능 시연
        clone_table = f"{target_table}_dev_clone"
        print("\n[Step 4] 👯 Zero-Copy Clone으로 개발용 DB 즉시 복제")
        print(f"   ✨ {clone_table} 생성 중 (스토리지 추가 비용 없음)...")
        
        # 기존 클론이 있으면 삭제 후 다시 생성 (반복 실행을 위해)
        cur.execute(f"DROP TABLE IF EXISTS {clone_table}")
        cur.execute(f"CREATE TABLE {clone_table} CLONE {target_table}")
        
        cur.execute(f"SELECT COUNT(*) FROM {clone_table}")
        clone_count = cur.fetchone()[0]
        print(f"   ✅ 복제 완료! 개발용 테이블 건수: {clone_count:,} 건")
        print("   💡 수백만 건의 데이터를 물리적 I/O 없이 메타데이터만으로 1초 만에 복제했습니다.")

        print("\n🎉 [Demo 종료] Snowflake의 핵심 아키텍처(Time Travel & Zero-Copy Clone) 시연이 성공적으로 끝났습니다.")

    except Exception as e:
        print(f"\n🔥 에러 발생: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()