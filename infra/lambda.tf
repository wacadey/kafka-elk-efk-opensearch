# *.py 파일 -> 압축(zip) -> 람다 함수 등록시 업로드 -> 관련 정보 조회
# py 원소스 위치, 차후 압축되면 생성되는 zip 위치 지정 -> 조회통해서 가져올수 있게 구성
data "archive_file" "check_bronze" {
  type        = "zip"
  source_file = "${path.module}/../lambda/check_bronze.py"
  output_path = "${path.module}/.check_bronze.zip"
}
data "archive_file" "cleanup_gold" {
  type        = "zip"
  source_file = "${path.module}/../lambda/cleanup_gold.py"
  output_path = "${path.module}/.cleanup_gold.zip"
}

data "archive_file" "quality_check" {
  type        = "zip"
  source_file = "${path.module}/../lambda/quality_check.py"
  output_path = "${path.module}/.quality_check.zip"
}

# 람다 함수 3개 리소스 생성
resource "aws_lambda_function" "check_bronze" {
  # 이름
  function_name = "${var.project_name}-check-bronze"
  # 업무 => 권한 => role
  role = aws_iam_role.lambda.arn
  # 파이썬 작동 => 런타임 환경
  runtime = "python3.12"
  # 엔트리포인트 (시작점 지정)
  handler = "check_bronze.lambda_handler" # 모듈명.함수
  # 소스
  filename = data.archive_file.check_bronze.output_path # zip 파일
  # 소스 업데이트
  source_code_hash = data.archive_file.check_bronze.output_base64sha256 # 해시값이 바뀌면 업데이트로 간주
  # 작업 최대시간 - 설정
  timeout = 30
  # 람다 사용할 최대 메모리 - 설정
  memory_size = 128
  # 환경변수 - s3 버킷경로, parquet 저장시 사용할 실버/골드 스키마
  environment {
    variables = {
      BUCKET_NAME  = aws_s3_bucket.data_lake.id
      SILVER_TABLE = aws_glue_catalog_table.silver.name
      GOLD_TABLE   = aws_glue_catalog_table.gold.name
    }
  }
}
resource "aws_lambda_function" "cleanup_gold" {
  # 이름
  function_name = "${var.project_name}-cleanup-gold"
  # 업무 => 권한 => role
  role = aws_iam_role.lambda.arn
  # 파이썬 작동 => 런타임 환경
  runtime = "python3.12"
  # 엔트리포인트 (시작점 지정)
  handler = "cleanup_gold.lambda_handler" # 모듈명.함수
  # 소스
  filename = data.archive_file.cleanup_gold.output_path # zip 파일
  # 소스 업데이트
  source_code_hash = data.archive_file.cleanup_gold.output_base64sha256 # 해시값이 바뀌면 업데이트로 간주
  # 작업 최대시간 - 설정
  timeout = 60
  # 람다 사용할 최대 메모리 - 설정
  memory_size = 128
  # 환경변수 - s3 버킷경로, parquet 저장시 사용할 실버/골드 스키마
  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.data_lake.id
    }
  }
}
resource "aws_lambda_function" "quality_check" {
  # 이름
  function_name = "${var.project_name}-quality-check"
  # 업무 => 권한 => role
  role = aws_iam_role.lambda.arn
  # 파이썬 작동 => 런타임 환경
  runtime = "python3.12"
  # 엔트리포인트 (시작점 지정)
  handler = "quality_check.lambda_handler" # 모듈명.함수
  # 소스
  filename = data.archive_file.quality_check.output_path # zip 파일
  # 소스 업데이트
  source_code_hash = data.archive_file.quality_check.output_base64sha256 # 해시값이 바뀌면 업데이트로 간주
  # 작업 최대시간 - 설정
  timeout = 30
  # 람다 사용할 최대 메모리 - 설정
  memory_size = 128
  # 환경변수 - s3 버킷경로, parquet 저장시 사용할 실버/골드 스키마
  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.data_lake.id
    }
  }
}
