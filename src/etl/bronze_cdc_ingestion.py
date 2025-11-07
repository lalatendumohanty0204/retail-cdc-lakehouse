"""
BronzeCDCIngestionETL
---------------------
Structured Streaming ingestion that tails a landing folder of Debezium-like CDC JSON files
and appends a normalized schema to a Delta Bronze table.

Why file-based CDC instead of Kafka?
- Zero infrastructure overhead while preserving CDC semantics (c/u/d operations).
- Easy for interview demos, reproducible on any laptop.

Output: Delta (append-only) with columns:
- sale_id, customer_id, product_id, event_ts, quantity, price, cdc_op, _ingested_at
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp
from src.utils.config_manager import load_path_config
from src.utils.logging_utils import create_pipeline_logger

class BronzeCDCIngestionETL:
    def __init__(self):
        self.log = create_pipeline_logger("BronzeCDCIngestionETL")
        self.paths = load_path_config()

    def _build_spark(self) -> SparkSession:
        """Use the active Spark session in Databricks."""
        return SparkSession.getActiveSession()
    def run_cdc_ingestion_stream(self):
        """Start the streaming query that keeps appending to Bronze Delta."""
        spark = self._build_spark()
        landing = self.paths["landing"]
        bronze = self.paths["bronze"]

        schema = """
          op STRING,
          before MAP<STRING,STRING>,
          after  MAP<STRING,STRING>
        """
        stream_df = (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.inferColumnTypes", "false")
                .schema(schema)
                .load(landing)
        )

        normalized = (
            stream_df
            .withColumn("sale_id",      expr("coalesce(after['sale_id'], before['sale_id'])"))
            .withColumn("customer_id",  expr("coalesce(after['customer_id'], before['customer_id'])"))
            .withColumn("product_id",   expr("coalesce(after['product_id'], before['product_id'])"))
            .withColumn("event_ts",     expr("coalesce(after['event_ts'], before['event_ts'])"))
            .withColumn("quantity",     expr("cast(coalesce(after['quantity'], before['quantity']) as int)"))
            .withColumn("price",        expr("cast(coalesce(after['price'], before['price']) as double)"))
            .withColumn("cdc_op",       expr("op"))
            .withColumn("_ingested_at", current_timestamp())
            .select("sale_id","customer_id","product_id","event_ts","quantity","price","cdc_op","_ingested_at")
        )

        query = (
            normalized.writeStream
            .format("delta")
            .option("path", bronze)
            .option("checkpointLocation", bronze + "_chk")
            .outputMode("append")
            .start()
        )

        self.log.info(f"Bronze stream started. Watching {landing} → {bronze}")
        query.awaitTermination()
