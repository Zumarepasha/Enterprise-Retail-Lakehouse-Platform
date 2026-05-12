

# Glue Catalog Databases

resource "aws_glue_catalog_database" "bronze_db" {

  name = "retail_bronze_db"
}

resource "aws_glue_catalog_database" "silver_db" {

  name = "retail_silver_db"
}

resource "aws_glue_catalog_database" "gold_db" {

  name = "retail_gold_db"
}

# Glue Crawlers

resource "aws_glue_crawler" "bronze_crawler" {

  name          = "retail-bronze-crawler"
  role          = "iam/role/path"

  database_name = aws_glue_catalog_database.bronze_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.bronze_bucket.bucket}/"
  }

  schema_change_policy {

    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "silver_crawler" {

  name          = "retail-silver-crawler"
  role          = "iam/role/path"

  database_name = aws_glue_catalog_database.silver_db.name

  delta_target {
    delta_tables = [
      "s3://${aws_s3_bucket.silver_bucket.bucket}/"
    ]
  }

  schema_change_policy {

    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

# Glue Jobs

resource "aws_glue_job" "silver_etl_job" {

  name     = "retail-silver-layer-etl"
  role_arn = "iam/role/path"

  glue_version = "4.0"
  worker_type = "G.1X"
  number_of_workers = 5

  command {

    script_location = "s3://retail-glue-scripts/silver_layer_etl.py"
    python_version = "3"
    name = "glueetl"
  }

  default_arguments = {

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--TempDir" = "s3://${aws_s3_bucket.glue_temp_bucket.bucket}/"
  }

  execution_property {

    max_concurrent_runs = 1
  }

  max_retries = 2
  timeout = 60
}

resource "aws_glue_job" "gold_aggregation_job" {

  name     = "retail-gold-layer-aggregation"
  role_arn = "iam/role/path"

  glue_version = "4.0"
  worker_type = "G.1X"
  number_of_workers = 5

  command {

    script_location = "s3://retail-glue-scripts/gold_layer_aggregation.py"
    python_version = "3"
    name = "glueetl"
  }

  default_arguments = {

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--TempDir" = "s3://${aws_s3_bucket.glue_temp_bucket.bucket}/"
  }

  execution_property {

    max_concurrent_runs = 1
  }

  max_retries = 2
  timeout = 60
}