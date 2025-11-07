"""
SilverSalesRefinementETL
------------------------
Batch job that turns the raw, append-only Bronze CDC log into the latest clean snapshot.

Responsibilities
- Deduplicate records at the business key (sale_id) using latest ingested event
- Drop hard deletes (cdc_op='d') at the snapshot layer
- Enforce basic type casting and null checks
- Run Great Expectations validations (programmatic)
- Overwrite the Silver Delta table (idempotent build)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta import configure_spark_with_delta_pip
from src.utils.config_manager import load_path_config
from src.utils.logging_utils import create_pipeline_logger
from src.quality.expectations_validator import run_data_quality_validations

class SilverSalesRefinementETL:
    def __init__(self):
        self.log = create_pipeline_logger("SilverSalesRefinementETL")
        self.paths = load_path_config()

    def _build_spark(self) -> SparkSession:
        builder = SparkSession.builder.appName("SilverSalesRefinementETL")
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def run_sales_refinement(self) -> None:
        spark = self._build_spark()
        bronze = self.paths["bronze"]
        silver = self.paths["silver"]

        bronze_df = spark.read.format("delta").load(bronze)

        # Keep the latest event per sale_id (simple deterministic approach)
        latest_by_key = (
            bronze_df
            .orderBy(col("_ingested_at").desc())
            .dropDuplicates(["sale_id"])
        )

        # Remove deletes, cast columns, filter invalids
        refined = (
            latest_by_key
            .filter(col("cdc_op") != "d")
            .filter(col("sale_id").isNotNull())
            .filter(col("quantity").isNotNull() & (col("quantity") > 0))
            .filter(col("price").isNotNull() & (col("price") > 0))
            .withColumn("event_ts", to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
            .select("sale_id","customer_id","product_id","event_ts","quantity","price","_ingested_at")
        )

        # Data quality gate — fail fast if expectations are not met
        run_data_quality_validations(refined)

        refined.write.format("delta").mode("overwrite").save(silver)
        self.log.info(f"Silver snapshot rebuilt at {silver}")
