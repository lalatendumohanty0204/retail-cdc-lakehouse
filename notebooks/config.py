# Databricks notebook source

# MAGIC %md
# MAGIC # Config — DBFS Paths

# COMMAND ----------

# Base path for demo data
BASE_PATH = "dbfs:/tmp/retail_cdc_demo"

PATHS = {
    "landing": f"{BASE_PATH}/data/landing",
    "bronze":  f"{BASE_PATH}/data/bronze/sales_delta",
    "silver":  f"{BASE_PATH}/data/silver/sales_clean",
    "gold":    f"{BASE_PATH}/data/gold/sales_daily",
}

display(PATHS)

# Ensure directories exist (idempotent)
try:
    dbutils.fs.mkdirs(PATHS["landing"])
    dbutils.fs.mkdirs(PATHS["bronze"])
    dbutils.fs.mkdirs(PATHS["silver"])
    dbutils.fs.mkdirs(PATHS["gold"])
except Exception:
    # Silent fallback when dbutils is unavailable (e.g., local execution)
    pass

