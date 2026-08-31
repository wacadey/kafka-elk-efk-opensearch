variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "프로젝트명, 이벤트브릿지와 스텝 함수를 이용한 배치 오케스트레이션"
  type        = string
  # 프로젝트명 수정
  default = "de-ai-12-eb-step-pipeline"
}

# Firehose 버퍼 설정
variable "firehose_buffer_size" {
  description = "S3로 전송할 때 최대 버퍼 크기(MB)"
  type        = number
  # 데이터를 모아서 s3로 전송 (크기 확대)
  default = 64
}
variable "firehose_buffer_interval" {
  description = "S3로 전송할 때 최대 버퍼 시간(초)"
  type        = number
  # 데이터 전송에 대해 시간 기준 확대
  default = 300
}

# Vector -> Firehose: IAM 사용자 이름
variable "vector_iam_user_name" {
  description = "선택값. 로컬 Vector가 사용하는 기존 IAM User에 Firehose Put 권한을 Terraform으로 붙일 때 지정한다. 비워두면 정책만 생성한다."
  type        = string
  # 개인 관리 번호로 교체 25 => 12
  default = ""
}

variable "tags" {
  description = "공통 태그"
  type        = map(string)
  default = {
    Project   = "kafka-eventbridge-stepfunctions"
    ManagedBy = "Terraform"
    Purpose   = "data-engineering-lab"
  }
}

# 추가되는 리소스에 맞춰 변수 추가
variable "eb_sch_expression" {
  description = "이벤트브릿지 스케줄(UTC)"
  type        = string
  default     = "cron(10 * * * ? *)"
}

variable "glue_worker_type" {
  description = "Glue Worker의 유형"
  type        = string
  default     = "G.1X"
}
variable "glue_number_of_workers" {
  description = "가동할 Glue Worker 수"
  type        = number
  default     = 2
}

# 성공/실패 결과를 이메일로 받을 수 있도록 선택 설정
variable "notification_email" {
  description = "선택, SNS 이메일 구독 주소"
  type        = string
  default     = null
  nullable    = true
}

# var.glue_output_partitions
# 브론즈에 파일이 1000개(gzip) => 실버에서는 1개의 parquet로 구성하겠다는 설정
variable "glue_output_partitions" {
  description = "시간 파티션당 silver parquet 파일 수"
  type        = number
  default     = 1
}
