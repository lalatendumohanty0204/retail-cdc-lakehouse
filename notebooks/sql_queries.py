# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Views / Queries — Examples

# COMMAND ----------

# Create temp views on the Delta locations and run a couple of example queries.
spark.read.format("delta").load(PATHS["silver"]).createOrReplaceTempView("sales_silver")
spark.read.format("delta").load(PATHS["gold"]).createOrReplaceTempView("sales_gold")

display(spark.sql("""
SELECT product_id, SUM(quantity) AS qty, SUM(amount) AS revenue
FROM sales_silver
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 20
"""))

display(spark.sql("""
SELECT date, total_revenue, total_orders
FROM sales_gold
ORDER BY date DESC
LIMIT 30
"""))
