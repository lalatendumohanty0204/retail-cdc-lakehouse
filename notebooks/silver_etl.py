# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver ETL — Deduplicate and Clean Bronze CDC Data
# MAGIC Applies CDC (insert/update/delete) from Bronze TO Silver Delta table with deduplication, audit columns, and periodic optimization.
# MAGIC

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

#Ensure Silver table exists
silver_path = PATHS["silver"]
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS delta.`{silver_path}`
          (
                sale_id STRING,
                customer_id STRING,
                product_id STRING,
                event_ts STRING,
                quantity INT,
                price DOUBLE,
                cdc_op STRING,
                _ingested_at TIMESTAMP,
                _processed_at TIMESTAMP,
                _source STRING
            )
            USING DELTA
            LOCATION '{silver_path}'
        """)

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

# Silver ETL Class
class SilverCDCTransformationETL:
    def __init__(self, spark_session=None):
        # Use the existing Databricks Spark session
        self.spark = spark_session or spark
        #print("Using existing Spark session for Silver CDC Stream")

    #Applies CDC operations (insert/update/delete) from Bronze to Silver Delta table using MERGE INTO.
    def apply_cdc(self, bronze_path, silver_path):
        print(f"Starting Silver CDC merge stream: {silver_path}")

        # Step 1 - Read stream from Bronze
        bronze_df = (
            self.spark.readStream
            .format("delta")
            .load(bronze_path)
        )

        # Step 2️ - Add processing metadata
        bronze_df = (
            bronze_df
            .dropDuplicates(["sale_id", "cdc_op"])
            .withColumn("_processed_at", current_timestamp())
            .withColumn("_source", lit("bronze"))
        )

        # Step 3 - Define merge logic
        def upsert_to_silver(microBatchDF, batch_id):
            print(f"Processing microbatch #{batch_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            if microBatchDF.count() == 0:
                print("Empty microbatch.")
                return

            delta_silver = DeltaTable.forPath(spark, silver_path)
            # Perform MERGE INTO
            (
                delta_silver.alias("silver")
                .merge(
                    microBatchDF.alias("bronze"),
                    "silver.sale_id = bronze.sale_id"
                )
                .whenMatchedUpdateAll(condition="bronze.cdc_op IN ('u', 'c')")
                .whenMatchedDelete(condition="bronze.cdc_op = 'd'")
                .whenNotMatchedInsertAll(condition="bronze.cdc_op IN ('c', 'u')")
                .execute()
            )

            print(f"Batch #{batch_id} applied to Silver table ({microBatchDF.count()} records).")

        # Step 4 - Stream with foreachBatch 
        query = (
            bronze_df.writeStream
            .foreachBatch(upsert_to_silver)
            .outputMode("update")
            .option("checkpointLocation", PATHS["silver"] + "_chk")
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination()
        print("Silver CDC streaming job running continuously.")


silver_etl = SilverCDCTransformationETL(spark)
silver_etl.apply_cdc(PATHS["bronze"], PATHS["silver"])


# COMMAND ----------

#display(spark.read.format("delta").load(PATHS["silver"]))
