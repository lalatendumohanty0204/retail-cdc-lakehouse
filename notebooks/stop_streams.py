# Databricks notebook source
# MAGIC %md
# MAGIC ### Stop Streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping:", s.name, s.id)
    s.stop()
print("All active streams stopped.")
