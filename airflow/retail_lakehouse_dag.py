from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

# Default DAG Arguments

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# DAG Definition
@dag(
    dag_id="retail_lakehouse_pipeline",
    default_args=default_args,
    description="Enterprise Retail Lakehouse ETL Pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["aws", "glue", "delta", "lakehouse", "retail"]
)

def retail_lakehouse_pipeline():

    # Start Task
    @task(task_id="start_pipeline")
    def start_pipeline():
        print("Starting Retail Lakehouse Pipeline")
        return "Pipeline Started"

    # Bronze S3 Sensor
    bronze_data_sensor = S3KeySensor(

        task_id="bronze_data_sensor",
        bucket_name="retail-lakehouse",
        bucket_key="bronze/orders/*",
        wildcard_match=True,
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
        aws_conn_id="aws_default"
    )

    # Bronze Layer Crawler
    bronze_crawler = GlueCrawlerOperator(
        task_id="bronze_layer_crawler",

        config={
            "Name": "bronze-retail-crawler",

            "Role": "AWSGlueServiceRole",

            "DatabaseName": "retail_bronze_db",
            "Targets": {
                "S3Targets": [
                    {
                        "Path": "s3://retail-lakehouse/bronze/"
                    }
                ]
            },

            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            }
        },

        wait_for_completion=True,

        aws_conn_id="aws_default"
    )

    # Silver Layer Glue ETL Job

    silver_etl_job = GlueJobOperator(
        task_id="silver_layer_etl_job",

        job_name="silver-layer-etl",

        script_args={
            "--JOB_NAME": "silver-layer-etl",
            "--SOURCE_DATABASE": "retail_bronze_db",
            "--SOURCE_TABLE": "orders_bronze",
            "--TARGET_S3_PATH":
                "s3://retail-lakehouse/silver/orders/",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:retail-alerts"
        },

        wait_for_completion=True,
        verbose=True,
        aws_conn_id="aws_default"
    )

    # Silver Delta Table Crawler
    silver_crawler = GlueCrawlerOperator(
        task_id="silver_layer_crawler",

        config={
            "Name": "silver-delta-crawler",
            "Role": "AWSGlueServiceRole",
            "DatabaseName": "retail_silver_db",

            "Targets": {
                "DeltaTargets": [
                    {
                        "DeltaTables": [
                            "s3://retail-lakehouse/silver/orders/"
                        ]
                    }
                ]
            },

            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            }
        },

        wait_for_completion=True,
        aws_conn_id="aws_default"
    )

    # Gold Layer Aggregation Glue Job
    gold_aggregation_job = GlueJobOperator(
        task_id="gold_layer_aggregation_job",
        job_name="gold-layer-aggregation",

        script_args={
            "--JOB_NAME": "gold-layer-aggregation",
            "--SOURCE_DATABASE": "retail_silver_db",
            "--SOURCE_TABLE": "orders_silver",
            "--TARGET_S3_PATH":
                "s3://retail-lakehouse/gold/daily_sales_summary/",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:retail-alerts"
        },

        wait_for_completion=True,
        verbose=True,
        aws_conn_id="aws_default"
    )

    # Success Notification
    success_notification = SnsPublishOperator(
        task_id="pipeline_success_notification",
        target_arn=
        "arn:aws:sns:ap-south-1:123456789012:retail-alerts",

        subject="Retail Lakehouse Pipeline Success",

        message="""
                    Retail Lakehouse Pipeline Completed Successfully

                    Pipeline:
                    Retail Lakehouse ETL Workflow

                    Layers Processed:
                    Bronze → Silver → Gold

                    Services:
                    Glue Crawlers
                    Glue ETL Jobs
                    Delta Lake
                    Redshift

                    Status:
                    SUCCESS
                """,

        aws_conn_id="aws_default"
    )

    # End Task

    @task(task_id="end_pipeline")
    def end_pipeline():

        print("Retail Lakehouse Pipeline Completed Successfully")
        return "Pipeline Completed"


    # Task Dependencies
    start = start_pipeline()
    end = end_pipeline()
    (
        start
        >> bronze_data_sensor
        >> bronze_crawler
        >> silver_etl_job
        >> silver_crawler
        >> gold_aggregation_job
        >> success_notification
        >> end
    )


# DAG Object
retail_lakehouse_pipeline()