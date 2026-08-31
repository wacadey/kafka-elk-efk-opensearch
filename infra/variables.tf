variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "프로젝트명, 이벤트브릿지, 스탭함수을 이용한 배치 오케스트레이션"
  type        = string
  # 프로젝트명 수정
  default = "de-ai-12-eb-step-pipeline"
}

# firehose 이름, firhose->opensearch : iam role name
variable "firehose_buffer_size" {
  description = "오픈 서치로 전송할때 최대 버퍼 사이즈(MB)"
  type        = number
  # 데이터를 모아서 s3로 전송 (크기 확대)
  default = 64
}
variable "firehose_buffer_interval" {
  description = "오픈 서치로 전송할때 최대 버퍼 시간(s)"
  type        = number
  # 데이터 전송에 대해 시간 기준 확대
  default = 300
}

# vector -> firhose : iam role name
variable "vector_iam_user_name" {
  description = "선택값. 로컬 Vector가 사용하는 기존 IAM User에 Firehose Put 권한을 Terraform으로 붙일 때 지정한다. 비워두면 정책만 생성한다."
  type        = string
  # 개인 관리 번호로 교체 25 => xx
  default = ""
}

variable "tags" {
  description = "공통 태그"
  type        = map(string)
  default = {
    Project   = "kafka-local-opensearch"
    ManagedBy = "Terraform"
    Purpose   = "data-engineering-lab"
  }
}

# 추가되는 리소스에 맞춰 변수 추가