# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality — Great Expectations

# COMMAND ----------

import os, sys
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from notebooks.config import PATHS
from src.quality.expectations_validator import ExpectationsValidator

validator = ExpectationsValidator(silver_path=PATHS["silver"])
results = validator.run_basic_sales_checks()
display(results)
