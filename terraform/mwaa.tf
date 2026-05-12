
resource "aws_s3_bucket" "mwaa_bucket" {

  bucket = "retail-mwaa-dags-dev"
}

# MWAA Environment

resource "aws_mwaa_environment" "retail_mwaa" {

  name = "retail-mwaa-environment"
  airflow_version = "3.0.0"
  execution_role_arn = "iam/role/path"
  source_bucket_arn = aws_s3_bucket.mwaa_bucket.arn
  dag_s3_path = "dags"
  requirements_s3_path = "requirements/requirements.txt"
  environment_class = "mw1.small"
  max_workers = 5
  min_workers = 1
  schedulers = 2
  webserver_access_mode = "PUBLIC_ONLY"

  logging_configuration {

    dag_processing_logs {

      enabled   = true
      log_level = "INFO"
    }

    scheduler_logs {

      enabled   = true
      log_level = "INFO"
    }

    task_logs {

      enabled   = true
      log_level = "INFO"
    }

    webserver_logs {

      enabled   = true
      log_level = "INFO"
    }

    worker_logs {

      enabled   = true
      log_level = "INFO"
    }
  }

  weekly_maintenance_window_start = "SUN:03:00"

  depends_on = [
    iam/role/with/policy/AmazonMWAAFullConsoleAccess
  ]
}