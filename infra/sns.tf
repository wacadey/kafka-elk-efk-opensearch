# 리소스
resource "aws_sns_topic" "pipeline" {
  name = "${var.project_name}-notifications"
}

# 해당 리소스에 대한 구독
resource "aws_sns_topic_subscription" "email" {
  count = var.notification_email == null ? 0 : 1

  topic_arn = aws_sns_topic.pipeline.arn
  protocol  = "email"
  endpoint  = var.notification_email
}
