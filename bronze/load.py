# Databricks notebook source
# MAGIC %md
# MAGIC ###### Create new Delta location

# COMMAND ----------

df.write \
  .format("delta") \
  .save(raw_file_path)

# COMMAND ----------

# MAGIC %md ###### Create new Table using Delta Location

# COMMAND ----------

spark.sql("CREATE TABLE " + raw_table + " USING DELTA LOCATION '" + raw_file_path + "'")
