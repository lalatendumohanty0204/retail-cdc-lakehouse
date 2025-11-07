# Databricks notebook source

# MAGIC %md
# MAGIC # Generate CDC — Debezium-like JSON
# MAGIC Writes Debezium-style events into the **Landing** folder.

# COMMAND ----------

import sys, os, json, time
root = os.path.dirname(os.path.dirname(os.getcwd()))
if root not in sys.path:
    sys.path.insert(0, root)

from notebooks.config import PATHS

from src.simulator.cdc_event_simulator import CDCEventSimulator

sim = CDCEventSimulator(output_path=PATHS["landing"])
sim.generate_initial_load(rows=100)
sim.generate_updates(num_batches=5, rows_per_batch=20, sleep_seconds=0)

print("CDC generated at:", PATHS["landing"])
