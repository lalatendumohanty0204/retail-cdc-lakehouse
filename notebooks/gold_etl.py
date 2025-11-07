# Databricks notebook source

# MAGIC %md
# MAGIC # Gold ETL — Daily Sales Aggregates

# COMMAND ----------

import os, sys
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from notebooks.config import PATHS
from notebooks.pipeline_classes import Gold

gold = Gold(silver_path=PATHS["silver"], gold_path=PATHS["gold"])
gold.build_aggregates()
print("Gold aggregates built.")
