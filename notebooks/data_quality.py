# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Quality Using Great Expectations Suites

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# Ensure latest GE version
%pip install great_expectations==0.15.41
dbutils.library.restartPython()


# COMMAND ----------

import great_expectations as gx
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from datetime import datetime
import json, os

# Load Bronze Delta Table
bronze_path = PATHS["bronze"]
bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df)

print(f"Loaded Bronze table for validation: {bronze_path}")
print(f"Total records: {bronze_df.count()}")

# Wrap with Great Expectations SparkDFDataset
ge_df = SparkDFDataset(bronze_df)


# 🔍 Define Bronze Data Quality Expectations
# Validate CDC operation field
ge_df.expect_column_values_to_not_be_null("cdc_op")
ge_df.expect_column_values_to_be_in_set("cdc_op", ["c", "u", "d"])

# Ensure CDC structure fields exist
ge_df.expect_column_to_exist("sale_id")
ge_df.expect_column_to_exist("customer_id")
ge_df.expect_column_to_exist("product_id")
ge_df.expect_column_to_exist("event_ts")

# Check column nullability and types
ge_df.expect_column_values_to_not_be_null("sale_id")
ge_df.expect_column_values_to_match_regex("customer_id", r"^C\d+$")
ge_df.expect_column_values_to_match_regex("product_id", r"^P\d+$")

# Ensure numeric fields are positive
ge_df.expect_column_values_to_be_between("quantity", min_value=1)
ge_df.expect_column_values_to_be_between("price", min_value=1)

# Event timestamp not null or empty
ge_df.expect_column_values_to_not_be_null("event_ts")

# Validate and Generate Summary
results = ge_df.validate()

for res in results["results"]:
    exp = res["expectation_config"]["expectation_type"]
    success = "Passed" if res["success"] else "Failed"
    print(f" - {exp}: {success}")


# Fail-fast for pipeline integration
if not results["success"]:
    raise Exception("Bronze Data Quality Validation FAILED — Aborting pipeline.")
else:
    print("\n Bronze Data Quality Checks PASSED. Proceeding to Silver layer.")

