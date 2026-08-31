# 운영체제 환경변수를 읽기 위해 os 모듈을 가져온다.
import os

# UTC 기준 시간 계산과 직전 시간대 산출을 위해 datetime 관련 클래스를 가져온다.
from datetime import datetime, timedelta, timezone

# AWS S3 서비스에 접근하기 위해 boto3 SDK를 가져온다.
import boto3

# Lambda 실행 환경에서 재사용할 S3 클라이언트를 생성한다.
s3 = boto3.client("s3")

# Terraform이 Lambda 환경변수로 전달한 Data Lake S3 버킷 이름을 읽는다.
BUCKET_NAME = os.environ["BUCKET_NAME"]
# Athena/Glue Data Catalog에 등록된 Silver 테이블 이름을 읽는다.
SILVER_TABLE = os.environ["SILVER_TABLE"]
# Athena/Glue Data Catalog에 등록된 Gold 테이블 이름을 읽는다.
GOLD_TABLE = os.environ["GOLD_TABLE"]

# 처리할 기준 시간대를 결정하는 내부 함수다.
def _target_hour(event: dict) -> datetime:
    # 수동 실행 시 event에 target_datetime이 있으면 해당 시간을 처리 대상으로 사용한다.
    value = event.get("target_datetime")
    # target_datetime 값이 전달된 경우 수동 실행용 시간 계산을 수행한다.
    if value:
        # 문자열 끝의 Z를 Python이 해석할 수 있는 UTC 오프셋(+00:00) 형태로 바꾼다.
        value = value.replace("Z", "+00:00")
        # ISO 8601 문자열을 datetime 객체로 변환한다.
        dt = datetime.fromisoformat(value)
        # 전달된 시간에 timezone 정보가 없으면 UTC 시간으로 간주한다.
        if dt.tzinfo is None:
            # timezone 정보가 없는 datetime에 UTC timezone을 설정한다.
            dt = dt.replace(tzinfo=timezone.utc)
        # UTC 기준으로 변환한 뒤 분/초/마이크로초를 0으로 만들어 해당 시간의 정각으로 맞춘다.
        return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # EventBridge 자동 실행 시 현재 UTC 시간을 구한다.
    now = datetime.now(timezone.utc)
    # Firehose Flush 시간을 고려해 현재 시간이 아닌 직전 1시간을 처리 대상으로 사용한다.
    return now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

# Step Functions가 호출하는 Lambda의 시작 함수다.
def lambda_handler(event, context):
    # 입력 event를 기준으로 실제 처리할 시간대를 계산한다.
    target = _target_hour(event or {})
    # 처리 대상 시간에서 연도 값을 YYYY 형식으로 추출한다.
    year = target.strftime("%Y")
    # 처리 대상 시간에서 월 값을 MM 형식으로 추출한다.
    month = target.strftime("%m")
    # 처리 대상 시간에서 일 값을 DD 형식으로 추출한다.
    day = target.strftime("%d")
    # 처리 대상 시간에서 시 값을 HH 형식으로 추출한다.
    hour = target.strftime("%H")

    # 해당 시간대의 Bronze 데이터가 저장되는 S3 Prefix를 만든다.
    bronze_prefix = f"bronze/year={year}/month={month}/day={day}/hour={hour}/"
    # 해당 시간대의 Silver 데이터가 저장되는 S3 Prefix를 만든다.
    silver_prefix = f"silver/year={year}/month={month}/day={day}/hour={hour}/"
    # 정제 과정에서 Reject된 데이터가 저장되는 S3 Prefix를 만든다.
    reject_prefix = f"reject/year={year}/month={month}/day={day}/hour={hour}/"
    # 해당 시간대의 Gold 집계 데이터가 저장되는 S3 Prefix를 만든다.
    gold_prefix = f"gold/sensor_hourly/year={year}/month={month}/day={day}/hour={hour}/"

    # Bronze Prefix 아래에 객체가 한 개라도 있는지 확인하기 위해 최대 1개만 조회한다.
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=bronze_prefix, MaxKeys=1)
    # KeyCount가 1 이상이면 해당 시간대에 처리할 Bronze 데이터가 있다고 판단한다.
    data_exists = response.get("KeyCount", 0) > 0

    # Gold 파티션이 실제 저장될 전체 S3 URI를 만든다.
    gold_location = f"s3://{BUCKET_NAME}/{gold_prefix}"

    # Athena Gold 테이블에 해당 시간 파티션을 추가하기 위한 SQL을 생성한다.
    gold_partition_query = (
        # 기존 파티션이 이미 있더라도 오류가 나지 않도록 IF NOT EXISTS를 사용한다.
        f"ALTER TABLE {GOLD_TABLE} ADD IF NOT EXISTS "
        # year/month/day/hour 값을 Gold 테이블의 파티션 값으로 지정한다.
        f"PARTITION (year='{year}', month='{month}', day='{day}', hour='{hour}') "
        # 위 파티션이 실제로 바라볼 S3 경로를 지정한다.
        f"LOCATION '{gold_location}'"
    )

    # Silver 데이터를 센서별 시간 단위 Gold 통계로 집계하는 Athena INSERT SQL을 생성한다.
    gold_insert_query = f"""
INSERT INTO {GOLD_TABLE}
SELECT
    sensor_id,
    COUNT(*) AS event_count,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    MAX(temperature) AS max_temperature,
    MAX(humidity) AS max_humidity,
    SUM(CASE WHEN temperature_alert THEN 1 ELSE 0 END) AS high_temperature_count,
    SUM(CASE WHEN humidity_alert THEN 1 ELSE 0 END) AS high_humidity_count,
    '{year}' AS year,
    '{month}' AS month,
    '{day}' AS day,
    '{hour}' AS hour
FROM {SILVER_TABLE}
WHERE year='{year}' AND month='{month}' AND day='{day}' AND hour='{hour}'
GROUP BY sensor_id
""".strip()

    # Step Functions의 다음 단계들이 사용할 처리 정보와 SQL을 반환한다.
    return {
        # Bronze 데이터 존재 여부를 반환해 Choice 단계에서 작업 계속 여부를 판단하게 한다.
        "data_exists": data_exists,
        # 실제 처리한 기준 시간을 UTC ISO 8601 문자열로 반환한다.
        "target_datetime": target.isoformat().replace("+00:00", "Z"),
        # 처리 대상 연도를 반환한다.
        "year": year,
        # 처리 대상 월을 반환한다.
        "month": month,
        # 처리 대상 일을 반환한다.
        "day": day,
        # 처리 대상 시간을 반환한다.
        "hour": hour,
        # Glue가 읽을 Bronze Prefix를 반환한다.
        "bronze_prefix": bronze_prefix,
        # Glue 입력 데이터의 전체 S3 경로를 반환한다.
        "source_path": f"s3://{BUCKET_NAME}/{bronze_prefix}",
        # Glue가 Silver를 저장할 기본 S3 경로를 반환한다.
        "silver_base_path": f"s3://{BUCKET_NAME}/silver/",
        # 현재 시간대 Silver 데이터의 S3 경로를 반환한다.
        "silver_path": f"s3://{BUCKET_NAME}/{silver_prefix}",
        # Reject 데이터가 저장될 기본 S3 경로를 반환한다.
        "reject_base_path": f"s3://{BUCKET_NAME}/reject/",
        # 현재 시간대 Reject 데이터의 S3 경로를 반환한다.
        "reject_path": f"s3://{BUCKET_NAME}/{reject_prefix}",
        # Gold 데이터 삭제 및 검증 단계에서 사용할 Gold Prefix를 반환한다.
        "gold_prefix": gold_prefix,
        # Athena Gold 파티션이 바라볼 S3 경로를 반환한다.
        "gold_location": gold_location,
        # Step Functions가 Athena에서 실행할 Gold 파티션 등록 SQL을 반환한다.
        "gold_partition_query": gold_partition_query,
        # Step Functions가 Athena에서 실행할 Silver → Gold 집계 SQL을 반환한다.
        "gold_insert_query": gold_insert_query,
    }