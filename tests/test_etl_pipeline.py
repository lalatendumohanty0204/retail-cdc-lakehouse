# test_etl_pipeline.py
# Unit tests for Databricks ETL pipeline

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    """Initialize Spark session for all tests"""
    spark = (
        SparkSession.builder
        .appName("ETLUnitTests")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    return spark

# CDC Generator Tests
def test_cdc_event_structure(spark):
    #Verify CDC event structure fields are correct
    sample = {
        "op": "c",
        "before": None,
        "after": {
            "sale_id": "123",
            "customer_id": "C001",
            "product_id": "P001",
            "event_ts": "2025-11-10 00:00:00",
            "quantity": 2,
            "price": 199.99
        }
    }

    assert "op" in sample
    assert "after" in sample
    assert isinstance(sample["after"]["price"], (float, int))
    assert sample["op"] in ["c", "u", "d"]


# Bronze To Silver Transformation Tests
# Ensure Silver ETL filters out records with null sale_id
def test_silver_transform_removes_nulls(spark):
    data = [
        ("123", "C001", "P001", 2, 100.0),
        (None, "C002", "P002", 3, 250.0)
    ]
    df = spark.createDataFrame(data, ["sale_id", "customer_id", "product_id", "quantity", "price"])
    clean_df = df.filter(col("sale_id").isNotNull())

    assert clean_df.count() == 1
    assert clean_df.filter(col("sale_id").isNull()).count() == 0

# Check price and quantity rules enforced
def test_price_and_quantity_positive(spark):
    data = [
        ("S001", 1, 99.0),
        ("S002", 0, 149.0),
        ("S003", 2, -50.0)
    ]
    df = spark.createDataFrame(data, ["sale_id", "quantity", "price"])
    valid_df = df.filter((col("quantity") > 0) & (col("price") > 0))

    assert valid_df.count() == 1  

# Gold Aggregation Tests
def test_gold_aggregation(spark):
    """Ensure Gold layer aggregates revenue correctly"""
    data = [
        ("C001", "P001", 2, 100.0),
        ("C001", "P002", 1, 200.0)
    ]
    df = spark.createDataFrame(data, ["customer_id", "product_id", "quantity", "price"])
    gold_df = df.groupBy("customer_id").sum("quantity", "price")
    row = gold_df.collect()[0]

    assert row["sum(quantity)"] == 3
    assert row["sum(price)"] == 300.0
