# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline Classes — Bronze, Silver, Gold
# MAGIC Thin wrappers that import your mirrored OOP classes from `src/` to keep logic unchanged.

# COMMAND ----------

import sys, os
# Add project root to path so `src` imports work in a notebook
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from src.etl.bronze_cdc_ingestion import BronzeCDCIngestionETL as Bronze
from src.etl.silver_sales_refinement import SilverSalesRefinementETL as Silver
from src.etl.gold_sales_aggregation import GoldSalesAggregationETL as Gold

