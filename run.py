# Databricks notebook source
# MAGIC %md ## Initial running of DB pipeline Raw ingest

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

# MAGIC %md ###### Create Delta Location

# COMMAND ----------

display(newDf.take(5))

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
