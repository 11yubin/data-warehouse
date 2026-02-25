# NY Taxi Data ETL with Snowflake

2024년 뉴욕 택시 데이터를 Snowflake 데이터 웨어하우스에 로드, 변환, 검증하는 자동화 ETL 파이프라인입니다.
2026 Data Engineering Zoomcamp 3주차 HW 과정에서, 수업 내용에서 사용한 BigQuery가 아닌 Snowflake를 자체적으로 활용한 개인 학습용 프로젝트 입니다.

## 🎯 프로젝트 개요

이 프로젝트는 다음을 수행합니다:

1. **데이터 다운로드**: NYC 택시 데이터(2024년 1월~6월) Parquet 파일 병렬 다운로드
2. **Snowflake 업로드**: 스테이징 영역(Stage)에 파일 배포
3. **임시 테이블 생성**: RAW 형식으로 데이터 로드
4. **데이터 변환**: 날짜/금액 형식 표준화
5. **검증 및 승인**: 데이터 샘플 & 통계 확인 후 수동 승인
6. **최종 저장**: 정규화된 테이블에 저장

## 아키텍처

### 파일 구조
- snowflake_python: snowflake에 파일을 적재/변환/조회하는 python 코드 - 로컬에서 실행
- snowflake_sql: 주차별 homework를 해결하기 위해 사용한 sql 코드 - snowflake 클라우드에서 실행

### 🏗️ Snowflake 아키텍처

<img width="1650" height="526" alt="diagram-export-2026 -2 -24 -오후-3_17_21" src="https://github.com/user-attachments/assets/99fe2faa-5f13-4bbd-8cc2-558b09a0489b" />

### 데이터베이스 구조

```
SNOWFLAKE
└── NY_TAXI (DATABASE)
    └── TRIPDATA (SCHEMA)
        ├── my_local_stage (STAGE)
        │   └── [Parquet 파일들]
        ├── yellow_tripdata_raw (임시 테이블)
        └── yellow_tripdata_2024 (최종 테이블)
```

### 주요 객체

| 객체 | 용도 |
|------|------|
| `my_parquet_format` | Parquet 파일 형식 정의 |
| `my_local_stage` | 로컬 Parquet 파일 스테이징 |
| `yellow_tripdata_raw` | RAW 데이터 임시 저장소 |
| `yellow_tripdata_2024` | 변환된 최종 데이터 |


## 🔧 설치 및 설정

### 환경 변수 설정

`.env` 파일을 프로젝트 루트에 생성하고 Snowflake 자격증명을 입력하세요:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_id
```

### 필수 패키지

```bash
pip install snowflake-connector-python python-dotenv
```

또는 UV 패키지 매니저 사용:

```bash
uv pip install snowflake-connector-python python-dotenv
```

## 🚀 사용 방법

### 기본 실행

```bash
python upload_data.py
```

또는 UV 사용:

```bash
uv run python upload_data.py
```

### 실행 흐름

1. **Snowflake 연결 확인** 🔌
   ```
   🔌 Connecting to Snowflake...
   ```

2. **파일 다운로드 & 업로드** 📥
   - 6개월 데이터(1월~6월)를 병렬로 다운로드 (max_workers=4)
   - 이미 있는 파일은 스킵

3. **데이터 변환 검증** ✅
   - 샘플 데이터 출력 (처음 3건)
   - 통계 정보 확인
     - 총 행 수
     - 운행 일수
     - 최소/최대/평균 금액

4. **수동 승인** ✋
   ```
   ✋ 변환 데이터가 정상입니다. 저장을 계속 진행하시겠습니까? (yes/no):
   ```
   - `yes` 입력 → 저장 진행
   - 그 외 입력 → 작업 중단 (테이블 정리)

5. **최종 저장** 🎉
   ```
   🎉 변환 및 저장 완료! yellow_tripdata_2024 테이블 저장됨
   ```

## 🔄 데이터 변환 프로세스

### 1단계: RAW 테이블 생성

```sql
CREATE OR REPLACE TABLE yellow_tripdata_raw
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(LOCATION=>'@my_local_stage', FILE_FORMAT=>'my_parquet_format'))
)
```

- Snowflake가 파일 스키마 자동 감지
- 모든 컬럼을 그대로 로드

### 2단계: 데이터 변환

```sql
CREATE OR REPLACE TABLE yellow_tripdata_2024 AS
SELECT
    -- 날짜 변환: 마이크로초 → TIMESTAMP
    TO_TIMESTAMP_NTZ("tpep_pickup_datetime" / 1000000) AS "tpep_pickup_datetime",
    TO_TIMESTAMP_NTZ("tpep_dropoff_datetime" / 1000000) AS "tpep_dropoff_datetime",
    
    -- 금액 변환: 지수표기법 → 소수점 2자리
    CAST("fare_amount" AS DECIMAL(10, 2)) AS "fare_amount",
    CAST("total_amount" AS DECIMAL(10, 2)) AS "total_amount",
    
    -- 나머지 컬럼들 그대로 가져오기
    * EXCLUDE ("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "total_amount")
FROM yellow_tripdata_raw
```

**변환 항목:**
- **날짜**: 마이크로초 단위 숫자 → 표준 TIMESTAMP 형식
- **금액**: 과학 표기법 → DECIMAL(10, 2) (소수점 이하 2자리)

### 3단계: 검증 쿼리

```sql
-- 통계 정보
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT DATE("tpep_pickup_datetime")) as unique_dates,
    MIN("total_amount") as min_amount,
    MAX("total_amount") as max_amount,
    AVG("total_amount") as avg_amount
FROM yellow_tripdata_2024
```

## 📊 예상 결과

성공적인 실행 시 다음과 같은 출력을 확인할 수 있습니다:

```
🔌 Connecting to Snowflake...
🚀 Checking & Uploading files...
   -> Pushing yellow_tripdata_2024-01.parquet...
   -> Pushing yellow_tripdata_2024-02.parquet...
   [... 계속 ...]

🏗️ Creating RAW table...
✨ Transforming data & Creating Final Table...

✅ 변환 검증 중...
   📊 총 데이터: 12,345,678 건
   📋 샘플 데이터 (처음 3건):
      - Pickup: 2024-01-01 08:30:00.000, Dropoff: 2024-01-01 08:45:00.000, Fare: $12.50, Total: $15.75
      ...

   📈 데이터 통계:
      - 총 행 수: 12,345,678
      - 운행 일수: 183
      - 최소 금액: $2.50
      - 최대 금액: $250.00
      - 평균 금액: $18.75

✋ 변환 데이터가 정상입니다. 저장을 계속 진행하시겠습니까? (yes/no): yes

✨ 임시 테이블 정리 완료
==================================================
🎉 변환 및 저장 완료! yellow_tripdata_2024 테이블 저장됨
==================================================
```

## ⚙️ 주요 설정값

| 설정 | 값 | 설명 |
|------|-----|------|
| `WAREHOUSE` | COMPUTE_WH | Snowflake 컴퓨팅 리소스 |
| `DATABASE` | NY_TAXI | 데이터베이스명 |
| `SCHEMA` | TRIPDATA | 스키마명 |
| `MONTHS` | 01~06 | 다운로드할 월 범위 |
| `max_workers` | 4 | 병렬 다운로드 스레드 수 |

## 🔍 문제 해결

### 1. Snowflake 연결 실패
```
❌ .env 파일에 접속 정보가 없거나 읽지 못했습니다!
```
→ `.env` 파일과 자격증명을 확인하세요.

### 2. 파일 다운로드 실패
```
❌ Download failed: [error message]
```
→ 인터넷 연결과 URL을 확인하세요.

### 3. 작업 중단
```
⏸️ 작업이 중단되었습니다. 데이터는 저장되지 않았습니다.
```
→ RAW와 변환 테이블이 자동 정리됩니다.

## 📝 라이선스

- 2026 Data Engineering Zoomcamp 3주차 HW 과정에서, 수업 내용에서 사용한 BigQuery가 아닌 Snowflake를 자체적으로 활용한 개인 학습용 프로젝트 입니다.
