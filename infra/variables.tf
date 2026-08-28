variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "프로젝트명"
  type        = string
  default     = "de-ai-12-kakfa-efk"
}
# opensearch 서비스(<-엘라스틱서치)/ opensearch 대시보드(<-키바나) 접속 가능한 IP 입력
variable "allowed_cidr" {
  description = "opensearch 대시보드/API에 접속한 공인 IP x.x.x.x/32"
  type        = string
  # 접속 위치가 바뀌면 접근 x => ip를 변경하여 인프라 반영시켜야 함
  default     = "222.108.125.33/32"
  validation {
    condition     = can(cidrhost(var.allowed_cidr, 0))
    error_message = "가능한 주소는 CIDR 형식이여야 합니다."
  }
}

# opensearch, spec(버전, 인스턴유형, 볼륨단위, 인덱스등 설정)

# firhose 이름, firhose->opensearch : iam role name

# vector -> firhose : iam role name