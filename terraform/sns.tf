
# SNS Topic

resource "aws_sns_topic" "retail_alerts" {

  name = "retail-alerts"
  tags = var.common_tags
}

# Email Subscription

resource "aws_sns_topic_subscription" "email_notification" {

  topic_arn = aws_sns_topic.retail_alerts.arn
  protocol = "email"
  endpoint = "zp@company.com"
}

# CloudWatch Alarm Topic Policy

resource "aws_sns_topic_policy" "default_policy" {

  arn = aws_sns_topic.retail_alerts.arn

  policy = jsonencode({

    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCloudWatchPublish"
        Effect = "Allow"

        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }

        Action = "SNS:Publish"
        Resource = aws_sns_topic.retail_alerts.arn
      }
    ]
  })
}
