# Databricks notebook source
# MAGIC %md
# MAGIC ### Config — DBFS Path
# MAGIC Setup DBFS directories for landing, bronze, silver, and gold.

# COMMAND ----------

# Base path in your workspace
USER_EMAIL = spark.sql("SELECT current_user()").collect()[0][0]
BASE_PATH = f"dbfs:/Workspace/Users/{USER_EMAIL}/Files/retail_cdc_demo"

PATHS = {
    "landing": f"{BASE_PATH}/data/landing",
    "bronze":  f"{BASE_PATH}/data/bronze/sales_delta",
    "silver":  f"{BASE_PATH}/data/silver/sales_clean",
    "gold":    f"{BASE_PATH}/data/gold/sales_daily",
}

display(PATHS)

# Ensure directories exist
for p in PATHS.values():
    try:
        dbutils.fs.mkdirs(p)
    except Exception as e:
        print(f"Error: Could not create {p}: {e}")

