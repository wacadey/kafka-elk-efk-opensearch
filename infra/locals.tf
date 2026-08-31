locals {

  bucket_name         = "${var.project_name}-eb-sf-${data.aws_caller_identity.current.account_id}"
  firehose_log_group  = "/aws/kinesisfirehose/${var.project_name}"
  firehose_log_stream = "S3Delivery"

  # 추가 및 보정
  firehose_name = "${var.project_name}-firehose"

  # sfn의 리소스 이름
  sfn_name = "${var.project_name}-sfn"
  # 디비명
  glue_database_name = replace("${var.project_name}_db", "-", "_")
  # 테이블명
  silver_table_name = "silver_tbl"
  gold_table_name   = "gold_tbl"

  # glue
  glue_job_name = "${var.project_name}-bronze-to-silver"
  # athena 리소스명
  workgroup_name = "${var.project_name}-athena-wg"
}
