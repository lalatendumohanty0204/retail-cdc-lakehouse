# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Quality Using Great Expectations Suites

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# Ensure latest GE version
%pip install -q great-expectations>=1.4.0
dbutils.library.restartPython()


# COMMAND ----------

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Load your Silver Delta table
silver_df = spark.read.format("delta").load(PATHS["silver"])
display(silver_df)
print(f"✅ Loaded Silver layer for GE validation: {PATHS['silver']}")

# Create an in-memory GE context (Ephemeral)
context = gx.get_context()

# --- Step 1: Define a simple suite name ---
suite_name = "silver_suite"

# --- Step 2: Create the Expectation Suite ---
suite = gx.ExpectationSuite(name=suite_name)
context.save_expectation_suite(expectation_suite=suite)

# --- Step 3: Wrap Spark DataFrame in RuntimeBatchRequest ---
batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="silver_sales",
    runtime_parameters={"batch_data": silver_df},
    batch_identifiers={"default_identifier_name": "silver_validation"},
)

# --- Step 4: Create the Validator ---
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

# --- Step 5: Define Expectations ---
validator.expect_column_values_to_not_be_null("sale_id")
validator.expect_column_values_to_be_between("quantity", min_value=1)
validator.expect_column_values_to_be_between("price", min_value=1)
validator.expect_column_values_to_match_regex("customer_id", r"^C\d+$")
validator.expect_column_values_to_match_regex("product_id", r"^P\d+$")

# --- Step 6: Run Validation ---
results = validator.validate()

# --- Step 7: Print a summary ---
print("\n📊 Great Expectations Validation Summary:")
for res in results["results"]:
    exp = res["expectation_config"]["expectation_type"]
    success = "✅ Passed" if res["success"] else "❌ Failed"
    print(f" - {exp}: {success}")

# --- Step 8: Fail-fast check ---
if not results["success"]:
    raise Exception("❌ Data Quality Check Failed: Aborting pipeline.")
else:
    print("\n✅ Data Quality PASSED. Proceeding with next step.")

