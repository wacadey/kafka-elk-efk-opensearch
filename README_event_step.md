# 데이터 목표
- 메달리온 아키텍처 기본
- 브론즈 : 원본 데이터
```
{
  "timestamp": "2026-08-31T13:25:14+09:00",
  "sensor_id": "SENSOR-003",
  "temperature": 87.5,
  "humidity": 42.1,
  "status": "WARN"
}
```
- 실버 : 정제된 데이터 -> 분석 가능한 깨끗한 데이터, 필요시 파생변수 추가 가능
```
{
  # 원데이터
  "timestamp": "2026-08-31T13:25:14+09:00",
  "sensor_id": "SENSOR-003",
  "temperature": 87.5,
  "humidity": 42.1,
  "status": "WARN",

  # 파생 변수 -> 온도, 습도등 이상 상황에 대한 파생변수 추가
  "is_abnormal_temperature": true,
  "is_abnormal_humidity": false,
  "is_abnormal": true,

  # 파티션 데이터
  "year": 2026,
  "month": 8,
  "day": 31,
  "hour": 13
}
```
```
silver/
└── year=2026/
    └── month=08/
        └── day=31/
            └── hour=13/
                └── part-xxxx.parquet
```
- 골드 - 집계된 데이터(비즈니스에 부합한 데이터 형태)
```
# 시간당 집계한다면 -> 해당 센서에 대해서 인사이트 도출, 설명, 이해 등을 할 수 있게 제공
# 데이터 요구자 DS라면 요청에 맞게 구성...
{
  "sensor_id": "SENSOR-003",

  "year": 2026,
  "month": 8,
  "day": 31,
  "hour": 13,

  "event_count": 3600,
  "avg_temperature": 72.4,
  "max_temperature": 91.2,
  "avg_humidity": 43.8,
  "max_humidity": 75.6,
  "high_temperature_count": 150,
  "high_humidity_count": 30
}

```
- 위의 silver, gold 데이터 형태에 맞춰서 스키마 구성 필요 -> Glue Data catalog -> db/테이블 구성 -> parquet 저장 -> 열기반  검색/쿼리 -> 속도 이득


# 인프라 목표
- 기존 로그젠 => fluent-bit => kafka => vector => firehose 동일
    - kafka를 굳이 사용하는 이유?
        - 데이터 트래픽이 급상승하고 처리량이 많아질 때
            - 안정성 제공 (손실 x)
            - 극저 지연 전송
            - 토픽을 구독하는 여러 컨슈머에게 동시 전송 가능함
                - 원소스 멀티유즈 사용 가능
            - ...
- firehose -> s3 bronze 적재
                -> EventBridge 서비스
                    => 이벤트 버스 + (스케줄)
                        - `이벤트` 트리거 활용 메달리온 아키텍처 기반 전처리 수행
                        - Airflow에서는 센서를 이용하여 이벤트 감지 -> task 작동
                    => [v]스케줄
                        - 특정 시간이 되면 (매시간 10분에 `스케줄링`) 메달리온 아키텍처 기반 전처리 수행
                        - Airflow에서 스케줄 적용 task 작동
                -> 트리거 작동 -> 실제 작업(task) 진행 : Step Functions
                    => 메달리온 아키텍처 기반 전처리 수행 실제 세부 task를 구성 => ... => s3 하위에 silver, gold, 기타 등등 데이터들을 처리하여 세팅 해둠 => 대시보드, 각종 서비스에서 s3를 조회하여(athena)를 활용
                -> glue등 에서 db/table등, job(전처리-pandas, polars, pyspark)을 구성 진행 -> task에 관여, 저장형태(컬럼)

- 요약
    - 배치 프로세싱 기준, 저비용, 고효율 작업 구성
        - EventBridge : 트리거 역할/본 프로젝트는 스케줄만 적용
        - Step Functions : 실제 절차적 task 담당. ETL 수행, 메달리온 아키텍처 처리 task 정의
        - glue : 데이터 구조에 따른 스키마, ETL JOB을 제공(스파크, pandas 활용)
        - 기타 : 권한, 변수, 등등 조정, 추가
        - firehose에서 기존 opensearch(제거)로 가는 방향성, s3로 변경

# 데이터 파이프라인
```
================================================================================
데이터 파이프라인
================================================================================

[기존 구간]

Python log_gen.py
   |
   | sensor_json.log / sensor_text.log
   v
Fluent Bit
   |-------------------------------|
   | JSON                          | TEXT (선택 경로 / 기존 설정에서는 주석)
   v                               v
factory-json-topic              Logstash
                                   |
                                   v
                            factory-text-topic
   |                               |
   +---------------+---------------+
                   v
                Kafka -> 데이터 폭주시 안정적공급(누락x), 대량 처리 안정적, 공급/소비 분린(브로커)
                   |
                   | Consume
                   v
                Vector
                   |
                   | PutRecord / PutRecordBatch
                   v
            Amazon Data Firehose

--------------------------------------------------------------------------------
여기까지 이전 Kafka/Vector/Firehose 수업과 같은 방향성
--------------------------------------------------------------------------------

[변경되는 지점]

기존 : Firehose -> OpenSearch
변경 : Firehose -> S3 Bronze

                   |
                   v
             S3 Data Lake
             ├─ bronze/   JSONL.GZ
             ├─ silver/   Parquet
             ├─ reject/   JSON
             └─ gold/     Parquet
                   ^
                   |
            EventBridge (매시간 10분) : 스케줄링 처리, 매시간 10분에 Step Functions 지시
                   |
                   v
            Step Functions : 7개의 job을 순차 작동하여 데이터 처리 -> 배치 오케스트레이션(EventBridge 포함)
                   |
       +-----------+-----------------------------+
       |                                         |
       v                                         |
1. Lambda CheckBronze                            |
       |                                         |
       v                                         |
2. Choice : Bronze 있음?                         |
       | YES                                     |
       v                                         |
3. Glue Bronze -> Silver ----------------------> S3 Silver
       |                                         |
       v                                         |
4. Athena MSCK REPAIR                            |
       |                                         |
       v                                         |
5. Lambda Cleanup Gold                           |
       |                                         |
       v                                         |
6. Athena Silver -> Gold ----------------------> S3 Gold
       |                                         |
       v                                         |
7. Lambda Quality Check                          |
       |                                         |
       +---- SUCCESS -> SNS                      |
       +---- FAILURE -> SNS                      |
                                                 |
            Glue Data Catalog <------------------+
                   |
                   v
                Athena : 최종 조회 -> 결과셋 -> 대시보드/기타 서비스 전개

================================================================================
핵심
================================================================================

기존 수업
  Collect -> Broker -> Consume -> Deliver -> Search
  Fluent Bit -> Kafka -> Vector -> Firehose -> OpenSearch

이번 업그레이드
  Collect -> Broker -> Consume -> Deliver -> Store -> Orchestrate -> Transform -> Analyze
  Fluent Bit -> Kafka -> Vector -> Firehose -> S3 -> EventBridge -> Step Functions -> Glue/Athena

파이썬 작성 (task 작업)
    - lambda 처리 -> s3 저장
    - glue job (ETL) 처리 -> s3 저장
```

# Step Functions
```
S3 Bronze
   
EventBridge
   ↓
Step Functions
   ├─ Lambda : Bronze 존재 확인
   ├─ Glue   : Bronze → Silver Parquet
   ├─ Athena : Silver Partition 등록
   ├─ Lambda : 기존 Gold 삭제
   ├─ Athena : Silver → Gold 집계
   ├─ Lambda : Gold 품질검사
   └─ SNS    : 성공/실패
                ↓
        S3 Silver / Gold
```

# 인프라 구성 1 (브론즈 저장까지 수정)
- 0. opensearch 제거
       - opensearch.tf 삭제
       - variables.tf에서 opensearch 관련 변수 삭제
       - firehose.tf에서 opensearch 구성삭제
       - outputs.tf에서 opensearch 출력 삭제

- 1. 변수명 조정 => 금요일 인프라 구분
       - 프로젝트명, firehose 크기/시간 

- 2. firehose에서 s3로 적재되게 조정 -> Data Lake 구성 완료

- 3. 인프라 구성
```
       terraform -chdir=infra fmt
       terraform -chdir=infra validate
       terraform -chdir=infra plan
       terraform -chdir=infra apply
```
- 4. .env에 FIREHOSE_STREAM_NAME 수정
```
       # 이름 조회
       terraform -chdir=infra output

       # .env 수정
       FIREHOSE_STREAM_NAME=de-ai-12-eb-step-pipeline-firehose

       # 도커 컴포즈 재구성 => vector 컨테이너의 환경변수로 설정되게 수정
       docker compose down
       docker compose up -d
```
- 5. 로그 발생 -> ... -> s3://버킷/bronze/*.gzip 확인
```
       python log_gen.py
```

# 인프라 구성 2
```
eventbridge.tf
stepfunctions.tf
glue.tf
catalog.tf
athena.tf
lambda.tf
iam-stepfunctions.tf
iam-glue.tf
iam-lambda.tf
sns.tf

...
```

# Step Functions state 목록

| 순서 | State                    | Type    | 실제 업무                                                                                 |
| -: | ------------------------ | ------- | ------------------------------------------------------------------------------------- |
|  1 | `CheckBronze`            | Task    | Lambda를 실행하여 **현재 처리할 Bronze 데이터가 있는지 확인**하고, 처리 대상 날짜/시간 및 S3 경로·Athena 쿼리 등의 정보를 생성 |
|  2 | `BronzeExists`           | Choice  | `data_exists` 값을 확인하여 **Bronze 데이터가 있으면 처리 계속**, 없으면 종료                               |
|  3 | `NoBronzeData`           | Succeed | 처리할 Bronze 데이터가 없을 경우 **정상 종료**                                                       |
|  4 | `BronzeToSilver`         | Task    | Glue Job을 실행하여 **Bronze 원본 데이터를 정제·변환하고 Silver에 저장**                                  |
|  5 | `RepairSilverPartitions` | Task    | Athena에서 `MSCK REPAIR TABLE`을 실행하여 **새로 생성된 Silver S3 파티션을 Glue Catalog 테이블에 등록**     |
|  6 | `CleanupExistingGold`    | Task    | Lambda를 실행하여 **해당 처리 대상의 기존 Gold S3 데이터를 삭제**. 재실행 시 데이터 중복 방지                        |
|  7 | `RegisterGoldPartition`  | Task    | Athena에서 전달받은 `gold_partition_query`를 실행하여 **처리 대상 Gold 파티션을 테이블에 등록**                |
|  8 | `SilverToGold`           | Task    | Athena에서 `gold_insert_query`를 실행하여 **Silver 데이터를 집계·가공해서 Gold 파티션에 저장**               |
|  9 | `QualityCheck`           | Task    | Lambda를 실행하여 **생성된 Gold 결과가 비어 있거나 비정상인지 검사**                                         |
| 10 | `QualityPassed`          | Choice  | 품질검사 결과 `ok=true`이면 성공 처리, 아니면 품질 실패 처리                                               |
| 11 | `NotifySuccess`          | Task    | SNS를 통해 **파이프라인 성공 알림**을 전송하고 정상 종료                                                   |
| 12 | `NotifyQualityFailure`   | Task    | Gold 품질검사 실패 시 **SNS 품질 실패 알림**을 전송                                                   |
| 13 | `PipelineQualityFailed`  | Fail    | 품질검사 실패를 `GoldQualityCheckFailed` 오류로 **최종 실패 처리**                                    |
| 14 | `NotifyFailure`          | Task    | Lambda/Glue/Athena 등의 작업 자체가 실패했을 때 **SNS 실패 알림** 전송                                  |
| 15 | `PipelineFailed`         | Fail    | 작업 오류를 `PipelineTaskFailed`로 **최종 실패 처리**                                             |

- 작업 시퀀스
```
EventBridge
    │
    ▼
① CheckBronze
   Bronze 데이터 존재 여부 및
   처리에 필요한 각종 값 생성
    │
    ▼
② BronzeExists
    │
    ├── 데이터 없음
    │       │
    │       ▼
    │   ③ NoBronzeData
    │       정상 종료
    │
    └── 데이터 있음
            │
            ▼
④ BronzeToSilver
   Glue
   Bronze → Silver
            │
            ▼
⑤ RepairSilverPartitions
   Athena
   Silver 파티션 인식/등록
            │
            ▼
⑥ CleanupExistingGold
   Lambda
   기존 Gold 데이터 삭제
            │
            ▼
⑦ RegisterGoldPartition
   Athena
   Gold 파티션 등록
            │
            ▼
⑧ SilverToGold
   Athena
   Silver → Gold 집계/저장
            │
            ▼
⑨ QualityCheck
   Lambda
   Gold 데이터 품질 검사
            │
            ▼
⑩ QualityPassed
       │
       ├── ok = true
       │       ↓
       │  ⑪ NotifySuccess
       │     SNS 성공 알림
       │       ↓
       │      종료
       │
       └── ok = false
               ↓
          ⑫ NotifyQualityFailure
             SNS 품질실패 알림
               ↓
          ⑬ PipelineQualityFailed
             최종 실패
```


# 파이썬 파일
- etl glue job
       - ~/glue/bronze_to_silver.py
- lambda
       - ~/lambda/check_bronze.py
       - ~/lambda/cleanup_gold.py
       - ~/lambda/quality_check.py


# Glue worker 유형
- G.1X 스펙 ( https://docs.aws.amazon.com/glue/latest/dg/add-job.html?utm_source=chatgpt.com )

| 항목     |        G.1X |
| ------ | ----------: |
| DPU    |   **1 DPU** |
| vCPU   |  **4 vCPU** |
| Memory |   **16 GB** |
| Disk   |   **94 GB** |
| 용도     | 일반적인 ETL 작업 |
