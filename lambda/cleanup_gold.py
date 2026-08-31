# Lambda 환경변수에서 S3 버킷 이름을 읽기 위해 os 모듈을 가져온다.
import os

# AWS S3 API를 사용하기 위해 boto3 SDK를 가져온다.
import boto3

# Lambda 실행 환경에서 재사용할 S3 클라이언트를 생성한다.
s3 = boto3.client("s3")
# Terraform이 Lambda 환경변수로 전달한 Data Lake S3 버킷 이름을 읽는다.
BUCKET_NAME = os.environ["BUCKET_NAME"]

# Step Functions가 Gold 재집계 전에 호출하는 Lambda 시작 함수다.
def lambda_handler(event, context):
    # CheckBronze 단계에서 전달된 현재 시간대 Gold Prefix를 읽는다.
    prefix = event["gold_prefix"]
    # S3 객체가 1,000개를 초과해도 전체를 조회할 수 있도록 paginator를 생성한다.
    paginator = s3.get_paginator("list_objects_v2")
    # 삭제된 S3 객체 개수를 누적하기 위한 변수를 0으로 초기화한다.
    deleted = 0

    # 지정한 Gold Prefix 아래의 모든 S3 객체 페이지를 순회한다.
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        # 현재 페이지의 객체 Key만 추출해 delete_objects 형식의 목록으로 만든다.
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        # S3 delete_objects API의 최대 1,000개 제한에 맞춰 객체 목록을 1,000개씩 나눈다.
        for start in range(0, len(objects), 1000):
            # 현재 삭제할 최대 1,000개의 객체 묶음을 만든다.
            chunk = objects[start : start + 1000]
            # 삭제할 객체가 실제로 존재할 때만 S3 삭제 API를 호출한다.
            if chunk:
                # 현재 Gold 시간대의 기존 객체들을 한 번에 삭제한다.
                s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": chunk})
                # 삭제한 객체 수를 누적한다.
                deleted += len(chunk)

    # Step Functions의 다음 단계에서 확인할 삭제 결과를 반환한다.
    return {
        # 실제로 삭제된 Gold 객체 개수를 반환한다.
        "deleted_objects": deleted,
        # 어떤 Gold Prefix를 정리했는지 함께 반환한다.
        "gold_prefix": prefix,
    }