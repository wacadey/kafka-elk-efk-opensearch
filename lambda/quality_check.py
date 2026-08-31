# Lambda 환경변수에서 S3 버킷 이름을 읽기 위해 os 모듈을 가져온다.
import os

# AWS S3 API를 사용하기 위해 boto3 SDK를 가져온다.
import boto3

# Lambda 실행 환경에서 재사용할 S3 클라이언트를 생성한다.
s3 = boto3.client("s3")
# Terraform이 Lambda 환경변수로 전달한 Data Lake S3 버킷 이름을 읽는다.
BUCKET_NAME = os.environ["BUCKET_NAME"]

# Step Functions가 Gold 생성 후 호출하는 품질검사 Lambda 시작 함수다.
def lambda_handler(event, context):
    # CheckBronze 단계에서 생성한 현재 시간대 Gold Prefix를 입력값에서 읽는다.
    prefix = event["gold_prefix"]
    # Gold Prefix 아래 모든 객체를 조회하기 위해 S3 paginator를 생성한다.
    paginator = s3.get_paginator("list_objects_v2")

    # Gold Prefix 아래에 생성된 객체 수를 세기 위한 변수를 0으로 초기화한다.
    object_count = 0
    # Gold 객체들의 전체 파일 크기를 누적하기 위한 변수를 0으로 초기화한다.
    total_bytes = 0

    # 해당 Gold Prefix 아래의 모든 S3 객체 페이지를 순회한다.
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        # 현재 페이지의 모든 객체를 하나씩 순회한다.
        for item in page.get("Contents", []):
            # Gold 객체 하나를 발견할 때마다 객체 개수를 1 증가시킨다.
            object_count += 1
            # 현재 객체의 Size 값을 전체 바이트 크기에 누적한다.
            total_bytes += item.get("Size", 0)

    # 객체가 1개 이상이고 전체 크기가 0보다 크면 Gold 생성이 정상이라고 판단한다.
    ok = object_count > 0 and total_bytes > 0

    # Step Functions의 Choice 또는 알림 단계에서 사용할 품질검사 결과를 반환한다.
    return {
        # Gold 품질검사 성공 여부를 True/False로 반환한다.
        "ok": ok,
        # 검증한 Gold Prefix를 반환한다.
        "gold_prefix": prefix,
        # Gold Prefix 아래 생성된 객체 개수를 반환한다.
        "object_count": object_count,
        # 생성된 Gold 객체들의 전체 크기를 바이트 단위로 반환한다.
        "total_bytes": total_bytes,
    }