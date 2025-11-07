# Databricks notebook source

# MAGIC %md
# MAGIC # Setup — Install Dependencies
# MAGIC Installs Great Expectations and restarts Python to make the package available in this cluster session.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

# In Databricks, restarting Python ensures newly installed packages are picked up.
try:
    dbutils.library.restartPython()
except Exception as e:
    print("Restart not available in this environment:", e)
