# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract raw data

# COMMAND ----------

df = spark.read.option("header",True).csv("abfss://" + file_system_name + "@" + storage_account_name + ".dfs.core.windows.net/raw").cache()

# COMMAND ----------

# MAGIC %run ./transform
