# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze CDC Structured Streaming to Delta table
# MAGIC
# MAGIC Ingests Debezium-style CDC JSON files from a landing folder into a Delta "bronze" table using Auto Loader (cloudFiles) for incremental streaming.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp

class BronzeCDCIngestionETL:
    def __init__(self, spark_session=None):
        # Use the existing Databricks Spark session
        self.spark = spark_session or spark
        #print("Using existing Spark session for Bronze CDC Stream")

    """
    Reads CDC JSON files from the landing area, normalizes them, 
    and writes them incrementally to a Delta table.
    """
    def run_stream(self, landing, bronze):
        print(f"Starting Auto Loader stream from: {landing}")
        # Schema matching CDC JSON structure
        cdc_schema = """
            op STRING,
            before MAP<STRING,STRING>,
            after MAP<STRING,STRING>
        """
        # Step 1️: Read CDC events using Auto Loader
        raw_stream_df = (
            self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.inferColumnTypes", "false")
                .schema(cdc_schema)
                .load(landing)
        )

        # Step 2️: Normalize CDC structure
        normalized_df = (
            raw_stream_df
            .withColumn("sale_id", expr("coalesce(after['sale_id'], before['sale_id'])"))
            .withColumn("customer_id", expr("coalesce(after['customer_id'], before['customer_id'])"))
            .withColumn("product_id", expr("coalesce(after['product_id'], before['product_id'])"))
            .withColumn("event_ts", expr("coalesce(after['event_ts'], before['event_ts'])"))
            .withColumn("quantity", expr("cast(coalesce(after['quantity'], before['quantity']) as int)"))
            .withColumn("price", expr("cast(coalesce(after['price'], before['price']) as double)"))
            .withColumn("cdc_op", col("op"))
            .withColumn("_ingested_at", current_timestamp())
            .drop("op", "before", "after")
        )

        # Step 3️: Write stream to Bronze Delta table
        query = (
            normalized_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", PATHS["bronze"] + "_chk")
            .trigger(processingTime="10 seconds") 
            .start(bronze)
        )

        query.awaitTermination()
        print("Bronze CDC ingestion completed successfully.")

bronze_etl = BronzeCDCIngestionETL(spark)
bronze_etl.run_stream(PATHS["landing"], PATHS["bronze"])

# COMMAND ----------

display(spark.read.format("delta").load(PATHS["bronze"]))

