import sys
import boto3
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

class GoldLayerAggregationETL:

    def __init__(self):
        """Resolve Glue Job Arguments"""

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

        # AWS Clients
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
        
    # Logging Utility
    def log_info(self, message):
        """Enable cloudwatch logging"""

        self.logger.info(message)
        print(message)

    # SNS Alert Utility
    def send_sns_alert(self, subject, message):
        """Sending SNS Notifications with Subject and Message"""

        self.sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )

    # Read Silver Delta Table
    def read_silver_data(self):
        """Reading source data from Glue Catalog"""

        self.log_info("Reading Silver Layer data")

        dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.source_database,
            table_name=self.source_table
        )
        df = dynamic_frame.toDF()
        self.log_info(f"Silver Layer Record Count : {df.count()}")

        return df
    
    # Data Cleansing
    def clean_data(self, df):
        """Applying business data filters"""

        self.log_info("Applying business data filters")

        cleaned_df = df.filter(
            (
                F.col("payment_status") == "SUCCESS"
            ) &
            (
                F.col("amount") > 0
            )
        )

        return cleaned_df

    # Aggregation Logic
    def aggregate_metrics(self, df):

        self.log_info("Performing Gold Layer aggregations")

        aggregated_df = df.groupBy(
            F.to_date("event_time").alias("order_date"),
            F.col("customer_tier"),
            F.col("payment_method")
        ).agg(
            F.countDistinct("customer_id").alias(
                "unique_customers"
            ),
            F.count("event_id").alias(
                "total_orders"
            ),
            F.sum("amount").alias(
                "total_sales"
            ),
            F.avg("amount").alias(
                "average_order_value"
            ),
            F.max("amount").alias(
                "highest_order_value"
            )
        )

        return aggregated_df
    
    # Add Audit Columns
    def add_audit_columns(self, df):
        """Adding audit columns"""

        self.log_info("Adding audit columns")

        audit_df = df.withColumn(
            "etl_processed_time",
            F.current_timestamp()
        ).withColumn(
            "year",
            F.year("order_date")
        ).withColumn(
            "month",
            F.month("order_date")
        ).withColumn(
            "day",
            F.dayofmonth("order_date")
        )

        return audit_df

    # Write Gold Delta Table
    def write_gold_table(self, df):
        """Writing aggregated data to Amazon Redshift"""

        self.log_info("Writing aggregated data to Amazon Redshift")
        
        redshift_tmp_dir = "s3://temp-dir/redshift-temp/"
        redshift_url = (
            "jdbc:redshift://redshift-etl-cluster:5439/retaildb"
        )

        # Wrong way to initialize secrets we need to use AWS Secret Manager
        redshift_properties = {
            "user": "admin",
            "password": "password",
            "driver": "com.amazon.redshift.jdbc.Driver"
        }

        # Write To Redshift Staging Table
        df.write \
            .format("jdbc") \
            .option("url", redshift_url) \
            .option("dbtable", "st.daily_sales_summary") \
            .option("user", redshift_properties["user"]) \
            .option("password", redshift_properties["password"]) \
            .option("driver", redshift_properties["driver"]) \
            .mode("overwrite") \
            .save()

        self.log_info("Data loaded into Redshift staging table")

        # Merge / Upsert Into Final Table
        # Wrong way to use SQL query, use sql file instead reading from S3 path
        merge_query = """
        BEGIN;

        UPDATE db.daily_sales_summary AS target
        SET
            unique_customers    = stg.unique_customers,
            total_orders        = stg.total_orders,
            total_sales         = stg.total_sales,
            average_order_value = stg.average_order_value,
            highest_order_value = stg.highest_order_value,
            etl_processed_time  = stg.etl_processed_time
        FROM st.daily_sales_summary AS stg
        WHERE target.order_date = stg.order_date
        AND target.customer_tier = stg.customer_tier
        AND target.payment_method = stg.payment_method;

        DELETE FROM st.daily_sales_summary
        USING db.daily_sales_summary tgt
        WHERE st.daily_sales_summary.order_date = tgt.order_date
        AND st.daily_sales_summary.customer_tier = tgt.customer_tier
        AND st.daily_sales_summary.payment_method = tgt.payment_method;

        INSERT INTO db.daily_sales_summary
        SELECT *
        FROM st.daily_sales_summary;

        END;
        """

        self.spark._sc._jvm.java.lang.Class.forName(
            "com.amazon.redshift.jdbc.Driver"
        )
        connection = self.spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            redshift_url,
            redshift_properties["user"],
            redshift_properties["password"]
        )

        statement = connection.createStatement()
        statement.execute(merge_query)

        statement.close()
        connection.close()

        self.log_info("Redshift MERGE / UPSERT completed successfully")

    # Optimize Gold Delta Table
    def optimize_gold_table(self):
        """Running VACUUM on Delta table"""

        self.log_info("Running VACUUM on Gold Layer")

        self.spark.sql(f"""
            VACUUM delta.`{self.target_s3_path}`
            RETAIN 168 HOURS
        """)

        self.log_info("VACUUM completed successfully")

    # Main Process
    def process(self):

        try:

            self.log_info("=======================================")
            self.log_info("Starting Gold Layer Aggregation Job")
            self.log_info("=======================================")

            # Read Silver Layer
            silver_df = self.read_silver_data()
            silver_df_cnt = silver_df.count()

            if silver_df_cnt > 0:
            
                # Clean Data
                cleaned_df = self.clean_data(silver_df)

                # Aggregate Metrics
                aggregated_df = self.aggregate_metrics(cleaned_df)

                # Add Audit Columns
                final_df = self.add_audit_columns(aggregated_df)

                # Write Gold Table
                self.write_gold_table(final_df)

                # Optimize Delta Table
                self.optimize_gold_table()

                # Success Alert
                self.send_sns_alert(
                    subject="Gold Layer Aggregation Job Success",
                    message=f"""
                                Gold Layer Aggregation Completed Successfully

                                Job Name: {self.job_name}

                                Source Table: {self.source_database}.{self.source_table}

                                Target Path: {self.target_s3_path}
            
                                Execution Time: {datetime.now()}
                            """
                )

                self.log_info("SNS success notification sent")

                # Commit Glue Job
                self.job.commit()

                self.log_info("Gold Layer Job Completed Successfully")
            
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

            # Failure Alert
            self.send_sns_alert(
                subject="Gold Layer Aggregation Job Failed",
                message=f"""
                            Gold Layer Aggregation Job Failed

                            Job Name: {self.job_name}

                            Error Details: {error_message}

                            Failure Time: {datetime.now()}
                        """
            )

            self.logger.error("SNS failure notification sent")
            raise e

# Main Driver
if __name__ == "__main__":

    gold_job = GoldLayerAggregationETL()

    gold_job.process()