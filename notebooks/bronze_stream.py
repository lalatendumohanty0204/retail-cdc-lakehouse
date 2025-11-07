# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Stream — Ingest JSON → Delta
# MAGIC Auto Loader/structured streaming into Bronze Delta table.

# COMMAND ----------

import os, sys
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from notebooks.config import PATHS
from notebooks.pipeline_classes import Bronze

bronze = Bronze(landing_path=PATHS["landing"], bronze_path=PATHS["bronze"])
bronze.run_cdc_ingestion_stream()  # starts a stream; idempotent if implemented that way
print("Bronze stream started. Check `spark.streams.active`.")
