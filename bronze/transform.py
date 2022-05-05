# Databricks notebook source
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
