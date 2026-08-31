# ETL JOB -> UI단, 노트북업로드, [v]*.py 업로드(python shell 엔진 or [v]pyspark 엔진)
# 대용량 데이터 처리 => pyspark 엔진 => 소/중/대 모두 OK => 스파크 구동의 모든 환경은 Glue 자동 제공 (인프라 신경 쓸 필요 없다)
# step funcions 에서 Bronze -> Silver로 데이터를 정체/전처리하여 저장 구조 -> ETL JOB
# E:브론즈 추출, T:스파크 처리, L:실버 적재
# 1. 소스는 어디에? -> S3
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  # 소스 -> key
  key = "scripts/glue/bronze_to_silver.py"
  # 원소스 위치
  source = "${path.module}/../glue/bronze_to_silver.py"
  # 업로드된 파일 변경 여부 판단 -> 암호화, 해시 등등 활용
  etag = filemd5("${path.module}/../glue/bronze_to_silver.py")
}

# 2. JOB 구성
resource "aws_glue_job" "bronze_to_silver" {
  name     = local.glue_job_name
  role_arn = aws_iam_role.glue.arn

  # 구동 환경 구성
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30
  execution_class   = "STANDARD"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.id}/glue-temp/"
  }
}
