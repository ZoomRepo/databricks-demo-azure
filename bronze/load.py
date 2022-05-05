# Databricks notebook source
# MAGIC %md
# MAGIC ###### Create new Delta location

# COMMAND ----------

newDf.write \
  .format("delta") \
  .save(bronze_file_path)

# COMMAND ----------

# MAGIC %md ###### Create new Table using Delta Location

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE " + bronze_table + "(Region string,\
  Country string,\
  Item_Type string,\
  Sales_Channel string,\
  Order_Priority string,\
  Order_Date date,\
  Order_ID integer,\
  Ship_Date date,\
  Units_Sold integer,\
  Unit_Price float,\
  Unit_Cost float,\
  Total_Revenue double,\
  Total_Cost double,\
  Total_Profit double\
  ) USING DELTA LOCATION '" + bronze_file_path + "'")
