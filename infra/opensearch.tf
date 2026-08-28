# 정책 조회
data "aws_iam_policy_document" "opensearch_access" {
  # firehose가 opensearch에게 데이터 배달하는 허가
  # 특정 도메인에 대해 아래 액션 3개를 허가
  statement {
    sid    = "AllowFirehoseDelivery"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.firehose.arn]
    }
    actions = [
      "es:ESHttpGet",
      "es:ESHttpPost",
      "es:ESHttpPut"
    ]
    resources = ["${local.opensearch_domain_arn}/*"]
  }
  # opensearch의 대시보드(엘라스틱서치의 키바나)
  statement {
    sid    = "AllowDashboardFromMyIp"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    actions   = ["es:ESHttp*"]
    resources = ["${local.opensearch_domain_arn}/*"]
    # 특정 IP로만 대시보드에 접근할수 있다 = 조건부여 (내부용)
    condition {
      test     = "IpAddress"
      variable = "aws:SourceIp"
      values   = [var.allowed_cidr]
    }
  }
}
# opensearch_domain 리소스 생성
resource "aws_opensearch_domain" "factory" {
  # 도메인 이름
  domain_name = local.opensearch_domain_name
  # 보수적 버전 지정
  engine_version = var.opensearch_engine_version

  # 클러스터 설정
  cluster_config {
    # t3.small.search, 확장시 t3.small.search로 스케일링 처리
    instance_type = var.opensearch_instance_type
    # 노드 개수
    instance_count = 1
    # 단일 AZ
    zone_awareness_enabled = false
  }

  # 저장용(데이터 저장->저장공간->단위설정)
  # 저장 공간을 별도 관리 -> 유지되면 새로운 도메인에 붙여서 사용할수 있음
  ebs_options {
    # ebs 사용 설정 
    ebs_enabled = true
    # 볼륨 유형
    volume_type = "gp3"
    # 10 GB 볼륨 할당
    volume_size = var.opensearch_ebs_volume_size
  }
  # 저장 데이터 암호화
  encrypt_at_rest {
    enabled = true
  }
  # 노드간 통신시 암호화 설정
  # 향후 데이터가 많아지면 => 10GB 초과 => 노드 증설 => 인덱스 해당되는 데이터가 분산됨
  # 검색시 노드들 간에 통신 진행되야함
  node_to_node_encryption {
    enabled = true
  }
  # 도메인 접속시 허가된 프로토콜
  domain_endpoint_options {
    # https만 됨
    enforce_https = true
    # TLS 1-2 버전
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }
  # 엑세스 정책 처음에 조회한 내용으로 설정
  access_policies = data.aws_iam_policy_document.opensearch_access.json

  # 들어오는 데이터의 타입에 대한 매필 정보 제공
  # 데이터 구조가 변경되면 => 교체해야함
  # 이런 정보가 없으면 opensearch가 데이터를 보고 => 타입 추정 => 타입 매칭 (오류 많이 발생함)
  # timestamp 에서 오류 발생이 많음
  provisioner "local-exec" {
    command = <<-EOT
      curl -X PUT "https://${self.endpoint}/factory-sensor" \
        -H "Content-Type: application/json" \
        -d '{"mappings":{"properties":{"@timestamp":{"type":"date_nanos"},"timestamp":{"type":"date"},"vector_ingest_at":{"type":"date_nanos"},"sensor_id":{"type":"keyword"},"temperature":{"type":"float"},"humidity":{"type":"float"},"status":{"type":"keyword"}}}}'
    EOT
  }
}