# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze CDC Structured Streaming to Delta table
# MAGIC

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp
from datetime import datetime
from time import sleep

class BronzeCDCIngestionETL:
    def __init__(self, spark_session=None):
        # Use the existing Databricks Spark session
        self.spark = spark_session or spark
        #print("Using existing Spark session for Bronze CDC Stream")

    def run_stream(self, landing, bronze):
        schema = """
            op STRING,
            before MAP<STRING,STRING>,
            after MAP<STRING,STRING>
        """

        #stream_df = self.spark.readStream.schema(schema).json(landing)
        stream_df = (
            self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.inferColumnTypes", "false")
                .schema(schema)
                .load(landing)
        )


        normalized = (
            stream_df
            .withColumn("sale_id", expr("coalesce(after['sale_id'], before['sale_id'])"))
            .withColumn("customer_id", expr("coalesce(after['customer_id'], before['customer_id'])"))
            .withColumn("product_id", expr("coalesce(after['product_id'], before['product_id'])"))
            .withColumn("event_ts", expr("coalesce(after['event_ts'], before['event_ts'])"))
            .withColumn("quantity", expr("cast(coalesce(after['quantity'], before['quantity']) as int)"))
            .withColumn("price", expr("cast(coalesce(after['price'], before['price']) as double)"))
            .withColumn("cdc_op", expr("op"))
            .withColumn("_ingested_at", current_timestamp())
        )
        
        # ✅ Use your helper here
        chk_path = workspace_to_local_path(PATHS["bronze"] + "_chk")
        print(f"🗂️ Checkpoint path: {chk_path}")
        query = (
            normalized.writeStream
            .trigger(availableNow=True)  # <--- IMPORTANT: CE-safe
            .format("delta")
            .option("path", PATHS["bronze"])
            .option("checkpointLocation", chk_path)
            .outputMode("append")
            .start()
        )

        print(f"Streaming from {landing} To {bronze}")
        query.awaitTermination()


bronze_etl = BronzeCDCIngestionETL(spark)

# Continuous “streaming” simulation
batch = 1
while True:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] Starting Bronze ingestion batch {batch}")
    bronze_etl.run_stream(PATHS["landing"], PATHS["bronze"])
    print(f"[{now}] Batch {batch} completed, waiting for next CDC drop...\n")
    batch += 1
    sleep(10)
