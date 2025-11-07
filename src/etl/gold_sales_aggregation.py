"""
GoldSalesAggregationETL
-----------------------
Batch job that reads validated Silver data and produces reporting-friendly facts.

Current metric:
- Daily revenue & order counts grouped by transaction date.

Strategy:
- Full overwrite (idempotent) — easy to reason about in demos.
- Can be changed to incremental merges if needed later.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, sum as _sum, count as _count, col
from delta import configure_spark_with_delta_pip
from src.utils.config_manager import load_path_config
from src.utils.logging_utils import create_pipeline_logger

class GoldSalesAggregationETL:
    def __init__(self):
        self.log = create_pipeline_logger("GoldSalesAggregationETL")
        self.paths = load_path_config()

    def _build_spark(self) -> SparkSession:
        builder = SparkSession.builder.appName("GoldSalesAggregationETL")
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def run_sales_aggregation(self) -> None:
        spark = self._build_spark()
        silver = self.paths["silver"]
        gold = self.paths["gold"]

        df = spark.read.format("delta").load(silver)

        gold_df = (
            df.withColumn("date", to_date("event_ts"))
              .groupBy("date")
              .agg(
                  _sum(col("quantity") * col("price")).alias("daily_revenue"),
                  _count("*").alias("orders")
              )
        )

        gold_df.write.format("delta").mode("overwrite").save(gold)
        self.log.info(f"Gold facts rebuilt at {gold}")
