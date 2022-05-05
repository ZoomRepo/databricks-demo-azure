# Databricks notebook source
# MAGIC %md ## Initial running of DB pipeline Raw ingest

# COMMAND ----------

# MAGIC %md ###### variables should be set using databricks-cli (refer to notes)

# COMMAND ----------

endpoint = "App Endpoint"
secret = "App Secret"
add_id = "App Id"
storage_account_name = "oliverdaslsa"
file_system_name = "sandbox"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type." + storage_account_name + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account_name + ".dfs.core.windows.net", "" + add_id + "")
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account_name + ".dfs.core.windows.net", "" + secret + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account_name + ".dfs.core.windows.net", endpoint)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + file_system_name  + "@" + storage_account_name + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract raw data

# COMMAND ----------

df = spark.read.option("header",True).csv("abfss://" + file_system_name + "@" + storage_account_name + ".dfs.core.windows.net/raw").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update
# MAGIC ##### Rename columns (removed spaces) prior to overwriting data in the raw table, seeing as we would delete the csv and store raw data when the csv is uploaded with new data we would probably append or use delta here

# COMMAND ----------

df = df.withColumnRenamed("Item Type", "Item_Type")\
       .withColumnRenamed("Sales Channel", "Sales_Channel")\
       .withColumnRenamed("Order Priority", "Order_Priority")\
       .withColumnRenamed("Order Date", "Order_Date")\
       .withColumnRenamed("Order ID", "Order_ID")\
       .withColumnRenamed("Ship Date", "Ship_Date")\
       .withColumnRenamed("Units Sold", "Units_Sold")\
       .withColumnRenamed("Unit Price", "Unit_Price")\
       .withColumnRenamed("Unit Cost", "Unit_Cost")\
       .withColumnRenamed("Total Revenue", "Total_Revenue")\
       .withColumnRenamed("Total Cost", "Total_Cost")\
       .withColumnRenamed("Total Profit", "Total_Profit")

# COMMAND ----------

# MAGIC %md ###### Raw data delta configuration

# COMMAND ----------

raw_file_path = '/tmp/delta/raw_invoice_data'
raw_table = 'default.raw_invoice_data'

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ###### Create schema to apply data types to dataframe

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

schema = StructType([
    StructField("Region",StringType(),True),
    StructField("Country",StringType(),True), 
    StructField("Item_Type",StringType(),True), 
    StructField("Sales_Channel", StringType(), True), 
    StructField("Order_Priority", StringType(), False), 
    StructField("Order_Date", DateType(), False),
    StructField("Order_ID", IntegerType(), False),                    
    StructField("Ship_Date", DateType(), True), 
    StructField("Units_Sold", IntegerType(), True), 
    StructField("Unit_Price", FloatType(), False), 
    StructField("Unit_Cost", FloatType(), False), 
    StructField("Total_Revenue", DoubleType(), True), 
    StructField("Total_Cost", DoubleType(), False), 
    StructField("Total_Profit", DoubleType(), True) 
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Attempt application of schema on raw data set

# COMMAND ----------

newDf = spark.createDataFrame(spark.sql("SELECT * FROM default.raw_invoice_data").rdd, schema)

# COMMAND ----------

# MAGIC %md ###### Bronze delta configuration

# COMMAND ----------

bronze_file_path = '/tmp/delta/bronze_invoice_data'
bronze_table = 'default.bronze_invoice_data'

# COMMAND ----------

# MAGIC %md ###### Create Delta Location

# COMMAND ----------

display(newDf.take(5))

# COMMAND ----------

dbutils.fs.rm('tmp/delta/bronze_invoice_data',recurse=True)


# COMMAND ----------

dbutils.fs.ls("/tmp/delta/")

# COMMAND ----------

delta = spark \
  .read \
  .format("delta") \
  .load(bronze_file_path)


# COMMAND ----------

# MAGIC %md ##### First account of 'Dirty' data, we have a string in our date field.... This is where we would start to clense our data and our code could get messy so a refactor is in place.

# COMMAND ----------

display(newDf.take(5))

# COMMAND ----------

newDf.write \
  .format("delta") \
  .save(bronze_file_path)

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
