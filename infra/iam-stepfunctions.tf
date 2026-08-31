# states -> sfn 정책 조회
data "aws_iam_policy_document" "stepfunctions_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}
# 위의 정책의 기반으로 iam role 생성
resource "aws_iam_role" "stepfunctions" {
  name               = "${var.project_name}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.stepfunctions_assume.json
}

# 추가 정책 필요
data "aws_iam_policy_document" "stepfunctions" {
  # 람다 함수 호출 -> 해당 함수 리소스 -> 3개 지정
  statement {
    sid     = "InvokeLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.check_bronze.arn,
      aws_lambda_function.cleanup_gold.arn,
      aws_lambda_function.quality_check.arn,
    ]
  }
  # Glue 실행 권한
  statement {
    sid = "RunGlue"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }
  # 아테나 실행
  statement {
    sid = "RunAthena"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:BatchGetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
    ]
    resources = ["*"]
  }
  # 스키마 관련 권한 => parquet 관련
  statement {
    sid = "GlueCatalogForAthena"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
    ]
    resources = ["*"]
  }
  # s3 저장/삭제/...관련 권한
  statement {
    sid = "AthenaS3"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*",
    ]
  }
  # 작업의 완료 통보하는 권한 -> sns
  statement {
    sid       = "Notify"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline.arn]
  }
  # 로깅 -> 각 작업에 대한 로그 관련 처리
  statement {
    sid = "StepFunctionsLogging"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}
# role에 추가 정책 반영
resource "aws_iam_role_policy" "stepfunctions" {
  name   = "${var.project_name}-sfn-policy"
  role   = aws_iam_role.stepfunctions.id
  policy = data.aws_iam_policy_document.stepfunctions.json
}