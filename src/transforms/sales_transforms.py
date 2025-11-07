"""
Transformation helpers for sales domain.
Keep business logic small, testable, and reusable across ETL stages.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def calculate_line_total(df: DataFrame) -> DataFrame:
    """Compute line_total = quantity * price with safe casting."""
    return df.withColumn("line_total", col("quantity").cast("double") * col("price").cast("double"))
