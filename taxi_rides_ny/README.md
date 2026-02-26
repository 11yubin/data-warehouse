# dbt 프로젝트 최종 파일 구조 및 경로

모든 파일은 taxi_rides_ny/ 프로젝트 루트 디렉토리를 기준으로 생성

## 1. 설정 및 패키지 (Root)
- 파일 경로: packages.yml
    내용: dbt_utils 패키지 설치 설정.
    실행 명령어: 작성 후 dbt deps 실행 필수.

## 2. 매크로 (Macros)
- 파일 경로: macros/get_payment_type_description.sql
    내용: 결제 코드(1, 2...)를 문자열(Cash, Credit...)로 변환하는 로직.

## 3. 스테이징 레이어 (Staging)
- 파일 경로: models/staging/stg_yellow_tripdata.sql
    내용: Yellow 택시 원본 데이터 타입 변환 및 매크로 적용.

- 파일 경로: models/staging/stg_green_tripdata.sql
    내용: Green 택시 원본 데이터 타입 변환 및 매크로 적용.

- 파일 경로: models/staging/int_trips_unioned.sql
    내용: Yellow와 Green 데이터를 하나로 합치는(Union) 중간 단계.

- 파일 경로: models/staging/schema.yml
    내용: 스테이징 모델들에 대한 설명(Description)과 테스트(unique, not_null) 설정.

## 4. 코어 레이어 (Core)
- 파일 경로: models/core/dim_zones.sql
    내용: taxi_zone_lookup 시드 데이터를 가공한 구역 정보 차원 테이블.

- 파일 경로: models/core/fct_trips.sql
    내용: 합쳐진 택시 데이터와 구역 정보를 조인한 최종 분석용 팩트 테이블.

- 파일 경로: models/core/schema.yml
    내용: 코어 모델들에 대한 문서화 및 무결성 테스트 설정.

## 5. 시드 데이터 (Seeds)
- 파일 경로: seeds/taxi_zone_lookup.csv
    내용: 외부에서 가져온 택시 구역 CSV 데이터.

### 실행 명령어: dbt seed

## 실행 순서
- dbt deps (패키지 설치)
- dbt seed (CSV 데이터 로드)
- dbt run (모든 모델 빌드)
- dbt test (데이터 검증)
- dbt docs generate -> dbt docs serve (문서 확인)

## 기타 명령어
- dbt clean: 청소
- dbt compile: 컴파일

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
