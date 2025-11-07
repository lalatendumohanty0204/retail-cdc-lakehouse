# Databricks notebook source

# MAGIC %md
# MAGIC # Silver ETL — CDC MERGE (upsert + delete)

# COMMAND ----------

import os, sys
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from notebooks.config import PATHS
from notebooks.pipeline_classes import Silver

silver = Silver(bronze_path=PATHS["bronze"], silver_path=PATHS["silver"])
silver.run_merge()
print("Silver MERGE complete.")
