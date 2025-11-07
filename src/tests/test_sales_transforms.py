"""
Unit test for a core transform. Fast feedback without spinning Spark streams.
"""
from pyspark.sql import SparkSession
from src.transforms.sales_transforms import calculate_line_total

def test_calculate_line_total():
    spark = SparkSession.builder.master("local[2]").appName("unit-test").getOrCreate()
    try:
        df = spark.createDataFrame([(1,2,10.0),(2,3,5.5)], ["id","quantity","price"])
        out = calculate_line_total(df).orderBy("id").collect()
        assert out[0]["line_total"] == 20.0
        assert out[1]["line_total"] == 16.5
    finally:
        spark.stop()
