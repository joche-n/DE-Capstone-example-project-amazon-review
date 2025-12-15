# infra/sns/main.tf
#
# Creates an SNS topic for job success/failure notifications.
# Subscribes optional email endpoints (confirm via email).

resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts-${var.env}"
  tags = {
    Project = var.project
    Env     = var.env
  }
}

# Create email subscriptions (each must be confirmed by user)
resource "aws_sns_topic_subscription" "email_subs" {
  for_each = toset(var.alert_emails)

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# Output the topic name and ARN for other modules
output "sns_topic_name" {
  value = aws_sns_topic.alerts.name
}

output "sns_topic_arn" {
  value = aws_sns_topic.alerts.arn
}