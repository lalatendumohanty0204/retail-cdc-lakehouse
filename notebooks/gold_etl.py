# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold ETL Layer
# MAGIC Consumes Silver Delta table and produces aggregated Gold tables.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, round, current_timestamp, approx_count_distinct
from delta.tables import DeltaTable
from datetime import datetime

class GoldCDCTransformationETL:
    def __init__(self, spark_session=None):
        # Use the existing Databricks Spark session
        self.spark = spark_session or spark
        #print("Using existing Spark session for Gold analytics layer")

    # Build key business metrics from Silver CDC data.
    def build_aggregations(self, df):
        return (
            df.groupBy("customer_id")
              .agg(
                  approx_count_distinct("sale_id").alias("total_orders"),
                  _sum(col("quantity") * col("price")).alias("total_revenue"),
              )
              .withColumn("avg_order_value", round(col("total_revenue") / col("total_orders"), 2))
              .withColumn("_aggregated_at", current_timestamp())
        )

    # Stream from Silver table to build & merge Gold metrics incrementally.
    def apply_gold_transformations(self, silver_path, gold_path):

        # Read from Silver table as streaming source
        silver_df = (
            self.spark.readStream
                .format("delta")
                .load(silver_path)
                .filter(col("cdc_op").isin("c", "u")) 
        )

        # Aggregation
        agg_stream = self.build_aggregations(silver_df)

        # Ensure Gold table exists
        if not DeltaTable.isDeltaTable(self.spark, gold_path):
            print(" Creating new Gold Delta table...")
            empty_df = self.build_aggregations(self.spark.read.format("delta").load(silver_path).limit(0))
            empty_df.write.format("delta").mode("overwrite").save(gold_path)

        def upsert_to_gold(microBatchDF, batch_id):
            print(f"Processing Gold microbatch #{batch_id} — {datetime.now().strftime('%H:%M:%S')}")
            if microBatchDF.count() == 0:
                print("Empty microbatch — skipping.")
                return

            delta_gold = DeltaTable.forPath(self.spark, gold_path)
            (
                delta_gold.alias("gold")
                .merge(
                    microBatchDF.alias("agg"),
                    "gold.customer_id = agg.customer_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(f"Gold table merged successfully for batch #{batch_id}.")

            # periodic maintenance
            if batch_id % 10 == 0:
                print("Optimizing Gold table for faster queries...")
                self.spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY customer_id")
                print("Optimization Done.")

        # Write as streaming upsert
        query = (
            agg_stream.writeStream
                .foreachBatch(upsert_to_gold)
                .outputMode("update")
                .option("checkpointLocation", PATHS["gold"] + "_chk")
                .trigger(processingTime="30 seconds")
                .start()
        )

        query.awaitTermination()
        print("Gold streaming aggregation running continuously.")

gold_etl = GoldCDCTransformationETL(spark)
gold_etl.apply_gold_transformations(PATHS["silver"], PATHS["gold"])

