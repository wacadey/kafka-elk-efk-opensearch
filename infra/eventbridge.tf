# 권한 + 리소스
# 정책
data "aws_iam_policy_document" "eventbridge_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}
# Role 생성 -> 기본 events에 대한 정책 반영
resource "aws_iam_role" "eventbridge" {
  name               = "${var.project_name}-eventbridge-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume.json
}
# 이벤트브릿지 => step function 작동을 위한 정책 조회
data "aws_iam_policy_document" "eventbridge" {
  statement {
    actions = ["states:StartExecution"]
    # step functions 리소스
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}
# role 바로 위 정책을 반영
resource "aws_iam_role_policy" "eventbridge" {
  name   = "${var.project_name}-eventbridge-policy"
  role   = aws_iam_role.eventbridge.id
  policy = data.aws_iam_policy_document.eventbridge.json
}

# 스케줄 관련 본 업무
# 특정 주기 단위로 이벤트브릿지 규칙 생성
resource "aws_cloudwatch_event_rule" "hourly" {
  name        = "${var.project_name}-hourly"
  description = "Run the data pipeline at minute 10 of every hour"
  # 스케줄 주기 표기 (매시간 10분)
  schedule_expression = var.eb_sch_expression
}
# 이벤트브릿지가 스케줄이 되면 누구를 연결.실행등 처리할것인지
resource "aws_cloudwatch_event_target" "stepfunctions" {
  # rule, 내부 규칙
  rule = aws_cloudwatch_event_rule.hourly.name
  # 상호간 식별자
  target_id = "${var.project_name}-sfn-pipeline"
  # 타겟 리소스
  arn = aws_sfn_state_machine.pipeline.arn
  # role, 해당 서비스를 이용한 role 지정
  role_arn = aws_iam_role.eventbridge.arn

  # sfn 호출/작업 지시할대 전달할 파라미터, 데이터 정의
  # 간단한 신호만 부여
  input = jsonencode({
    source = "eventbridge-schedule"
  })

}
