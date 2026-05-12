
# Glue Failure Alarm

resource "aws_cloudwatch_metric_alarm" "glue_job_failure_alarm" {

  alarm_name = "retail-glue-job-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = 1
  metric_name = "glue.driver.aggregate.numFailedTasks"
  namespace = "Glue"
  period = 300
  statistic = "Sum"
  threshold = 1
  alarm_description = "Glue Job Failure Alert"

  alarm_actions = [
    aws_sns_topic.retail_alerts.arn
  ]

  dimensions = {
    JobName = "retail-silver-layer-etl"
  }

}

# MWAA Task Failure Alarm

resource "aws_cloudwatch_metric_alarm" "mwaa_task_failure_alarm" {

  alarm_name = "retail-mwaa-task-failure"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = 1
  metric_name = "FailedTasks"
  namespace = "AWS/MWAA"
  period = 300
  statistic = "Sum"
  threshold = 1
  alarm_description = "MWAA Task Failure Alert"

  alarm_actions = [
    aws_sns_topic.retail_alerts.arn
  ]

  dimensions = {
    Environment = var.mwaa_environment_name
  }

}