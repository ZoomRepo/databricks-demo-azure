# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform data

# COMMAND ----------

# MAGIC %md ##### Rename columns (removed spaces) prior to overwriting data in the raw table, seeing as we would delete the csv and store raw data when the csv is uploaded with new data we would probably append or use delta here

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

# MAGIC %md ##### Create schema to apply data types to dataframe

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType, DoubleType

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
# MAGIC ##### Attempt application of schema on raw data set

# COMMAND ----------

newDf = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

# MAGIC %md ##### First account of 'Dirty' data, we have a string in our date field.... This is where we would start to clense our data and our code could get messy so a refactor is in place.

# COMMAND ----------

display(newDf.take(5))
