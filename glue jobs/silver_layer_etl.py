import sys
import boto3
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable


class SilverLayerETL:

    def __init__(self):
        """Resolve Glue Job Arguments and Initialize Spark & Glue Context"""

        self.args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "SOURCE_DATABASE",
                "SOURCE_TABLE",
                "TARGET_S3_PATH",
                "SNS_TOPIC_ARN"
            ]
        )

        self.job_name = self.args["JOB_NAME"]
        self.source_database = self.args["SOURCE_DATABASE"]
        self.source_table = self.args["SOURCE_TABLE"]
        self.target_s3_path = self.args["TARGET_S3_PATH"]
        self.sns_topic_arn = self.args["SNS_TOPIC_ARN"]

        # Initialize Spark & Glue Context

        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.job_name, self.args)
        self.logger = self.glue_context.get_logger()

        # Initialize AWS Clients
        self.sns_client = boto3.client("sns")

        # Spark Configurations
        self.spark.conf.set(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        self.spark.conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        self.spark.conf.set(
            "spark.sql.adaptive.enabled",
            "true"
        )
        self.spark.conf.set(
            "spark.sql.shuffle.partitions",
            "200"
        )
        self.spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled",
            "true"
        )
        self.spark.conf.set(
            "spark.databricks.delta.autoCompact.enabled",
            "true"
        )
        self.spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true"
        )

    # Logging Utility
    def log_info(self, message):
        """Enable cloudwatch logging"""

        self.logger.info(message)
        print(message)

    # SNS Alert Utility
    def send_sns_alert(self, subject, message):
        """Sending SNS Notifications with Subject and Message"""

        self.sns_client.publish(
            TopicArn= self.sns_topic_arn,
            Subject= subject,
            Message= message
        )

    # Read Source Data
    def read_source_data(self):
        """Reading source data from Glue Catalog"""

        self.log_info("Reading source data from Glue Catalog")

        dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.source_database,
            table_name=self.source_table
        )
        df = dynamic_frame.toDF()
        self.log_info(f"Source Record Count : {df.count()}")

        return df
    
    # Flatten Nested JSON Structure
    def flatten_json(self, df):
        """Flattening nested JSON structure"""

        self.log_info("Flattening nested JSON structure")

        flattened_df = df.select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("amount"),
            F.col("event_time"),

            F.col("customer.customer_id").alias("customer_id"),
            F.col("customer.tier").alias("customer_tier"),

            F.col("payment.method").alias("payment_method"),
            F.col("payment.status").alias("payment_status"),

            F.current_timestamp().alias("etl_processed_time")
        )

        return flattened_df

    # Data Quality Validation
    def validate_data(self, df):
        """Applying data quality validations"""

        self.log_info("Applying data quality validations")

        validated_df = df.filter(
            (
                F.col("event_id").isNotNull()
            ) &
            (
                F.col("customer_id").isNotNull()
            ) &
            (
                F.col("amount").isNotNull()
            ) &
            (
                F.col("event_time").isNotNull()
            )
        )

        return validated_df

    # Deduplication Logic
    def deduplicate_data(self, df):
        """Deduplication Logic using row_number"""

        self.log_info("Removing duplicate records")

        window_spec = Window.partitionBy(
            "event_id"
        ).orderBy(
            F.col("event_time").desc()
        )

        dedup_df = df.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).drop("row_num")

        return dedup_df

    # Add Partition Columns
    def add_partition_columns(self, df):
        """Adding partition columns"""

        self.log_info("Adding partition columns")

        partition_df = df.withColumn(
            "year",
            F.year("event_time")
        ).withColumn(
            "month",
            F.month("event_time")
        ).withColumn(
            "day",
            F.dayofmonth("event_time")
        )

        return partition_df

    # Write Delta Table
    def write_delta_table(self, df):
        """Writing data to Delta Lake Silver Layer"""

        self.log_info("Starting Delta MERGE operation")

        # Check If Delta Table Already Exists
        delta_table_exists = DeltaTable.isDeltaTable(
            self.spark,
            self.target_s3_path
        )

        # Initial Load
        if not delta_table_exists:

            self.log_info("Delta table does not exist")
            self.log_info("Performing initial load")

            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("year", "month", "day") \
                .option("overwriteSchema", "true") \
                .save(self.target_s3_path)

            self.log_info("Initial Delta load completed")

        # Incremental MERGE Load
        else:

            self.log_info("Delta table exists")
            self.log_info("Performing incremental MERGE")

            delta_table = DeltaTable.forPath(
                self.spark,
                self.target_s3_path
            )

            delta_table.alias("target")\
            .merge(
                df.alias("source"),
                """
                target.event_id = source.event_id
                """
            )\
            .whenMatchedUpdate(
                set={
                    "event_type": "source.event_type",
                    "amount": "source.amount",
                    "event_time": "source.event_time",
                    "customer_id": "source.customer_id",
                    "customer_tier": "source.customer_tier",
                    "payment_method": "source.payment_method",
                    "payment_status": "source.payment_status",
                    "etl_processed_time": "source.etl_processed_time",
                    "year": "source.year",
                    "month": "source.month",
                    "day": "source.day"
                }
            )\
            .whenNotMatchedInsert(
                values={
                    "event_id": "source.event_id",
                    "event_type": "source.event_type",
                    "amount": "source.amount",
                    "event_time": "source.event_time",
                    "customer_id": "source.customer_id",
                    "customer_tier": "source.customer_tier",
                    "payment_method": "source.payment_method",
                    "payment_status": "source.payment_status",
                    "etl_processed_time": "source.etl_processed_time",
                    "year": "source.year",
                    "month": "source.month",
                    "day": "source.day"
                }
            )\
            .execute()

            self.log_info("Delta MERGE completed successfully")

    # Delta Maintenance
    def optimize_delta_table(self):
        """Running VACUUM on Delta table"""

        self.log_info("Running VACUUM on Delta table")

        self.spark.sql(f"""
            VACUUM delta.`{self.target_s3_path}`
            RETAIN 168 HOURS
        """)

        self.log_info("VACUUM completed successfully")

    # Process Pipeline
    def process(self):

        """Silver Layer ETL Pipeline"""

        try:

            self.log_info("=======================================")
            self.log_info("Starting Silver Layer ETL Pipeline")
            self.log_info("=======================================")

            # Read Source Data
            source_df = self.read_source_data()
            src_df_cnt = source_df.count()

            if src_df_cnt > 0:

                # Flatten JSON
                flattened_df = self.flatten_json(source_df)

                # Validate Data
                validated_df = self.validate_data(flattened_df)

                # Deduplicate Data
                dedup_df = self.deduplicate_data(validated_df)

                # Add Partition Columns
                final_df = self.add_partition_columns(dedup_df)

                # Write Delta Table
                self.write_delta_table(final_df)

                # Delta Maintenance
                self.optimize_delta_table()

                # Metrics
                total_records = final_df.count()

                self.log_info(
                    f"Total Processed Records : {total_records}"
                )

                # Success Notification
                self.send_sns_alert(
                    subject="Glue Silver ETL Job Success",
                    message=f"""
                                Glue Silver ETL Job Completed Successfully

                                Job Name: {self.job_name}

                                Source: {self.source_database}.{self.source_table}

                                Target: {self.target_s3_path}

                                Processed Records:{total_records}

                                Execution Time: {datetime.now()}
                            """
                )

                self.log_info("SNS success alert sent")

                # Commit Job
                self.job.commit()

                self.log_info("Glue Job Completed Successfully")
            else:

                # Send Notification
                self.send_sns_alert(
                    subject="No data available in source",
                    message=f"""
                                No data available in source

                                Job Name: {self.job_name}

                                Source: {self.source_database}.{self.source_table}

                                Execution Time: {datetime.now()}
                            """
                )

                self.log_info("No Source data available and sent the SNS alert")

                # Commit Job
                self.job.commit()

                self.log_info("Glue Job Completed Successfully")

        except Exception as e:

            error_message = str(e)
            self.logger.error(error_message)

            # Failure Notification
            self.send_sns_alert(
                subject="Glue Silver ETL Job Failed",
                message=f"""
                            Glue Silver ETL Job Failed

                            Job Name: {self.job_name}

                            Error Details: {error_message}

                            Failure Time: {datetime.now()}
                        """
            )

            self.logger.error("Failure SNS alert sent")
            raise e

# Main Driver
if __name__ == "__main__":

    etl = SilverLayerETL()

    etl.process()