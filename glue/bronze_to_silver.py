# Python 실행 인자(sys.argv)를 읽기 위해 sys 모듈을 가져온다.
import sys

# Glue Job 실행 시 전달받은 --KEY VALUE 형식의 파라미터를 읽기 위한 함수이다.
from awsglue.utils import getResolvedOptions

# SparkSession을 생성하기 위해 SparkSession 클래스를 가져온다.
from pyspark.sql import SparkSession

# PySpark의 컬럼 연산, 형변환, 조건식, 집계 등에 사용하는 functions를 F라는 이름으로 가져온다.
from pyspark.sql import functions as F


# Step Functions → Glue Job 실행 시 전달되는 실행 파라미터를 읽는다.
ARGS = getResolvedOptions(
    # 현재 Python 프로그램에 전달된 전체 실행 인자를 사용한다.
    sys.argv,

    # Glue Job에서 반드시 전달받아야 할 파라미터 이름 목록이다.
    [
        # 현재 실행 중인 Glue Job 이름이다.
        "JOB_NAME",

        # Bronze 데이터를 읽을 S3 경로이다.
        "SOURCE_PATH",

        # Silver 데이터를 저장할 S3 기본 경로이다.
        "SILVER_BASE_PATH",

        # Reject 데이터를 저장할 S3 기본 경로이다.
        "REJECT_BASE_PATH",

        # 처리 대상 데이터의 연도이다.
        "TARGET_YEAR",

        # 처리 대상 데이터의 월이다.
        "TARGET_MONTH",

        # 처리 대상 데이터의 일이다.
        "TARGET_DAY",

        # 처리 대상 데이터의 시간이다.
        "TARGET_HOUR",

        # Silver 결과 파일을 몇 개의 파티션으로 출력할지 결정한다.
        "OUTPUT_PARTITIONS",
    ],
)


# Glue Job 이름을 Spark Application 이름으로 사용하여 SparkSession을 생성한다.
spark = SparkSession.builder.appName(ARGS["JOB_NAME"]).getOrCreate()


# Bronze 원본 데이터를 읽을 S3 경로를 변수에 저장한다.
source_path = ARGS["SOURCE_PATH"]

# 처리 대상 연도를 가져온다.
year = ARGS["TARGET_YEAR"]

# 처리 대상 월을 가져온다.
month = ARGS["TARGET_MONTH"]

# 처리 대상 일을 가져온다.
day = ARGS["TARGET_DAY"]

# 처리 대상 시간을 가져온다.
hour = ARGS["TARGET_HOUR"]

# 출력 Parquet 파일 개수를 정수로 변환하며 최소 1개 이상이 되도록 설정한다.
output_partitions = max(int(ARGS["OUTPUT_PARTITIONS"]), 1)


# 현재 처리 시간에 해당하는 Silver 저장 경로를 만든다.
# 예: s3://bucket/silver/year=2026/month=08/day=31/hour=04/
silver_path = (
    f'{ARGS["SILVER_BASE_PATH"]}'
    f'year={year}/month={month}/day={day}/hour={hour}/'
)

# 현재 처리 시간에 해당하는 Reject 저장 경로를 만든다.
# 예: s3://bucket/reject/year=2026/month=08/day=31/hour=04/
reject_path = (
    f'{ARGS["REJECT_BASE_PATH"]}'
    f'year={year}/month={month}/day={day}/hour={hour}/'
)


# ---------------------------------------------------------
# 1. Bronze 데이터 읽기
# ---------------------------------------------------------

# Firehose가 S3에 저장한 JSONL GZIP 파일을 읽는다.
# Spark는 .gz 압축을 자동으로 해제하고 JSON 레코드를 DataFrame으로 변환한다.
raw = spark.read.json(source_path)


# ---------------------------------------------------------
# 2. 입력 스키마 보완
# ---------------------------------------------------------

# 기존 Kafka 프로젝트에서는 JSON Topic과 TEXT Topic의 필드명이 서로 다를 수 있다.
#
# 예:
#
# JSON Topic
#   timestamp
#   sensor_id
#   temperature
#   humidity
#
# TEXT Topic
#   log_time
#   device_id
#   temp
#   humi
#
# 특정 시간대에 한 Topic의 데이터만 존재하더라도
# 뒤의 정규화 코드가 실패하지 않도록 필요한 컬럼 목록을 정의한다.
optional_columns = [
    "timestamp",
    "log_time",
    "sensor_id",
    "device_id",
    "temperature",
    "temp",
    "humidity",
    "humi",
    "status",
    "topic",
    "partition",
    "offset",
    "vector_ingest_at",
]


# 필요한 컬럼을 하나씩 확인한다.
for column_name in optional_columns:

    # 현재 Bronze DataFrame에 해당 컬럼이 존재하지 않는 경우 처리한다.
    if column_name not in raw.columns:

        # 없는 컬럼을 NULL 값으로 새로 추가한다.
        # 이를 통해 이후 F.col() 호출에서 컬럼 없음 오류가 발생하지 않도록 한다.
        raw = raw.withColumn(
            column_name,
            F.lit(None),
        )


# ---------------------------------------------------------
# 3. Bronze → 공통 스키마 정규화
# ---------------------------------------------------------

# 서로 다른 두 Kafka Topic의 컬럼 구조를 하나의 공통 구조로 맞춘다.
normalized = (
    raw

    # timestamp가 있으면 사용하고, 없으면 log_time을 사용한다.
    # 최종적으로 occurred_at_raw이라는 공통 컬럼을 만든다.
    .withColumn(
        "occurred_at_raw",
        F.coalesce(
            F.col("timestamp"),
            F.col("log_time"),
        ),
    )

    # sensor_id가 있으면 사용하고, 없으면 device_id를 사용한다.
    .withColumn(
        "sensor_id_raw",
        F.coalesce(
            F.col("sensor_id"),
            F.col("device_id"),
        ),
    )

    # temperature가 있으면 사용하고, 없으면 temp를 사용한다.
    .withColumn(
        "temperature_raw",
        F.coalesce(
            F.col("temperature"),
            F.col("temp"),
        ),
    )

    # humidity가 있으면 사용하고, 없으면 humi를 사용한다.
    .withColumn(
        "humidity_raw",
        F.coalesce(
            F.col("humidity"),
            F.col("humi"),
        ),
    )

    # 문자열 형태의 이벤트 발생 시간을 Spark Timestamp 타입으로 변환한다.
    .withColumn(
        "occurred_at_ts",
        F.to_timestamp("occurred_at_raw"),
    )

    # Vector에서 추가한 수집 시간을 Timestamp 타입으로 변환한다.
    .withColumn(
        "vector_ingest_at_ts",
        F.to_timestamp("vector_ingest_at"),
    )

    # 온도 값을 숫자 계산이 가능한 double 타입으로 변환한다.
    .withColumn(
        "temperature_num",
        F.col("temperature_raw").cast("double"),
    )

    # 습도 값을 double 타입으로 변환한다.
    .withColumn(
        "humidity_num",
        F.col("humidity_raw").cast("double"),
    )

    # Kafka Partition 값을 정수형으로 변환한다.
    .withColumn(
        "kafka_partition_num",
        F.col("partition").cast("int"),
    )

    # Kafka Offset 값을 long 타입으로 변환한다.
    .withColumn(
        "kafka_offset_num",
        F.col("offset").cast("long"),
    )

    # Kafka Topic + Partition + Offset을 이용하여
    # 각 Kafka 레코드를 식별할 event_id를 생성한다.
    .withColumn(
        "event_id",

        # 조합된 문자열을 SHA-256 Hash 값으로 변환한다.
        F.sha2(

            # 여러 값을 | 문자로 연결한다.
            F.concat_ws(
                "|",

                # Topic이 NULL이면 unknown-topic을 대신 사용한다.
                F.coalesce(
                    F.col("topic").cast("string"),
                    F.lit("unknown-topic"),
                ),

                # Partition이 NULL이면 -1을 사용한다.
                F.coalesce(
                    F.col("partition").cast("string"),
                    F.lit("-1"),
                ),

                # Offset이 NULL이면 -1을 사용한다.
                F.coalesce(
                    F.col("offset").cast("string"),
                    F.lit("-1"),
                ),
            ),

            # SHA-256 알고리즘을 사용한다.
            256,
        ),
    )
)


# ---------------------------------------------------------
# 4. 정상 / Reject 데이터 판단 조건
# ---------------------------------------------------------

# 센서 값 자체가 높거나 낮은 것은 분석 대상 데이터로 인정한다.
#
# 예:
# temperature = 120
# humidity = 90
#
# 이런 값은 Reject하지 않고 Silver에 저장한다.
#
# 대신 구조적으로 사용할 수 없는 레코드만 Reject 대상으로 판단한다.
valid_condition = (

    # 이벤트 시간이 정상적인 Timestamp로 변환되었는지 확인한다.
    F.col("occurred_at_ts").isNotNull()

    # 센서 ID가 존재하는지 확인한다.
    & F.col("sensor_id_raw").isNotNull()

    # 온도를 숫자로 변환할 수 있었는지 확인한다.
    & F.col("temperature_num").isNotNull()

    # 습도를 숫자로 변환할 수 있었는지 확인한다.
    & F.col("humidity_num").isNotNull()

    # 센서 상태 값이 존재하는지 확인한다.
    & F.col("status").isNotNull()

    # Kafka Topic 정보가 존재하는지 확인한다.
    & F.col("topic").isNotNull()

    # Kafka Partition 정보가 존재하는지 확인한다.
    & F.col("kafka_partition_num").isNotNull()

    # Kafka Offset 정보가 존재하는지 확인한다.
    & F.col("kafka_offset_num").isNotNull()
)


# valid_condition의 결과가 NULL이 되는 경우도 False로 처리한다.
# 최종 결과는 True 또는 False만 가지도록 만든다.
is_valid = F.coalesce(
    valid_condition,
    F.lit(False),
)


# ---------------------------------------------------------
# 5. 정상 데이터 → Silver
# ---------------------------------------------------------

# 정상으로 판단된 데이터만 Silver 형태로 정제한다.
valid = (

    # 앞에서 정규화한 DataFrame을 사용한다.
    normalized

    # is_valid=True인 레코드만 선택한다.
    .filter(is_valid)

    # 분석에 필요한 컬럼만 선택하고 이름과 타입을 정리한다.
    .select(

        # Kafka Topic/Partition/Offset으로 만든 고유 이벤트 ID이다.
        F.col("event_id")
        .cast("string")
        .alias("event_id"),

        # 원 로그가 실제 발생한 시간이다.
        F.col("occurred_at_ts")
        .alias("occurred_at"),

        # JSON/TEXT Topic의 센서 ID 필드를 하나의 sensor_id로 통합한다.
        F.col("sensor_id_raw")
        .cast("string")
        .alias("sensor_id"),

        # 숫자형으로 정규화된 온도 값이다.
        F.col("temperature_num")
        .alias("temperature"),

        # 숫자형으로 정규화된 습도 값이다.
        F.col("humidity_num")
        .alias("humidity"),

        # 원본 로그의 상태 값을 문자열로 저장한다.
        F.col("status")
        .cast("string")
        .alias("status"),

        # 어떤 Kafka Topic에서 들어온 데이터인지 기록한다.
        F.col("topic")
        .cast("string")
        .alias("source_topic"),

        # 해당 메시지가 속한 Kafka Partition 번호를 저장한다.
        F.col("kafka_partition_num")
        .alias("kafka_partition"),

        # Kafka 메시지의 Offset 값을 저장한다.
        F.col("kafka_offset_num")
        .alias("kafka_offset"),

        # Vector가 해당 로그를 소비한 시간을 저장한다.
        F.col("vector_ingest_at_ts")
        .alias("vector_ingest_at"),

        # 온도가 100 이상이면 True, 그렇지 않으면 False로 판단한다.
        # 원 로그에는 존재하지 않고 Silver 단계에서 새로 만드는 파생 컬럼이다.
        (
            F.col("temperature_num") >= F.lit(100.0)
        ).alias("temperature_alert"),

        # 습도가 70 이상이면 True, 그렇지 않으면 False로 판단한다.
        # 역시 Silver 단계에서 새로 만드는 파생 컬럼이다.
        (
            F.col("humidity_num") >= F.lit(70.0)
        ).alias("humidity_alert"),

        # 현재 Glue에서 데이터를 처리한 시간을 추가한다.
        F.current_timestamp()
        .alias("processed_at"),
    )

    # 같은 event_id를 가진 중복 Kafka 메시지가 존재하면 하나만 남긴다.
    .dropDuplicates(["event_id"])
)


# ---------------------------------------------------------
# 6. 비정상 데이터 → Reject
# ---------------------------------------------------------

# 정상 조건을 만족하지 못한 데이터만 추출한다.
reject = (

    # 정규화된 데이터를 기준으로 처리한다.
    normalized

    # is_valid=False인 레코드만 선택한다.
    .filter(~is_valid)

    # 어떤 이유로 Reject 되었는지 reject_reason 컬럼을 추가한다.
    .withColumn(
        "reject_reason",

        # 여러 Reject 이유가 발생하면 쉼표(,)로 연결한다.
        F.concat_ws(
            ",",

            # Timestamp 변환이 실패했다면 invalid_timestamp를 기록한다.
            F.when(
                F.col("occurred_at_ts").isNull(),
                F.lit("invalid_timestamp"),
            ),

            # 센서 ID가 없으면 missing_sensor_id를 기록한다.
            F.when(
                F.col("sensor_id_raw").isNull(),
                F.lit("missing_sensor_id"),
            ),

            # 온도를 숫자로 변환할 수 없으면 invalid_temperature를 기록한다.
            F.when(
                F.col("temperature_num").isNull(),
                F.lit("invalid_temperature"),
            ),

            # 습도를 숫자로 변환할 수 없으면 invalid_humidity를 기록한다.
            F.when(
                F.col("humidity_num").isNull(),
                F.lit("invalid_humidity"),
            ),

            # status가 없으면 missing_status를 기록한다.
            F.when(
                F.col("status").isNull(),
                F.lit("missing_status"),
            ),

            # Kafka Topic이 없으면 missing_topic을 기록한다.
            F.when(
                F.col("topic").isNull(),
                F.lit("missing_topic"),
            ),

            # Kafka Partition 정보가 없으면 missing_partition을 기록한다.
            F.when(
                F.col("kafka_partition_num").isNull(),
                F.lit("missing_partition"),
            ),

            # Kafka Offset 정보가 없으면 missing_offset을 기록한다.
            F.when(
                F.col("kafka_offset_num").isNull(),
                F.lit("missing_offset"),
            ),
        ),
    )

    # Reject된 시간을 추가한다.
    .withColumn(
        "rejected_at",
        F.current_timestamp(),
    )
)


# ---------------------------------------------------------
# 7. Silver 데이터 저장
# ---------------------------------------------------------

# Silver DataFrame의 파티션 수를 지정된 수만큼 줄인다.
# 작은 파일이 너무 많이 만들어지는 Small File 문제를 줄이기 위한 설정이다.
valid.coalesce(output_partitions) \
    .write \
    .mode("overwrite") \
    .parquet(silver_path)


# ---------------------------------------------------------
# 8. Reject 데이터 저장
# ---------------------------------------------------------

# Reject 데이터는 파일 수를 1개로 줄여 JSON 형식으로 저장한다.
reject.coalesce(1) \
    .write \
    .mode("overwrite") \
    .json(reject_path)


# ---------------------------------------------------------
# 9. Glue Job 실행 결과 로그 출력
# ---------------------------------------------------------

# 이번 Glue Job이 읽은 Bronze S3 경로를 CloudWatch 로그에 출력한다.
print(f"SOURCE_PATH={source_path}")

# Silver 데이터가 저장된 S3 경로를 출력한다.
print(f"SILVER_PATH={silver_path}")

# Reject 데이터가 저장된 S3 경로를 출력한다.
print(f"REJECT_PATH={reject_path}")

# 정상 처리되어 Silver로 들어간 데이터 개수를 출력한다.
print(f"VALID_COUNT={valid.count()}")

# Reject 영역으로 분리된 데이터 개수를 출력한다.
print(f"REJECT_COUNT={reject.count()}")


# SparkSession을 종료하고 Glue Job의 Spark 리소스를 정리한다.
spark.stop()