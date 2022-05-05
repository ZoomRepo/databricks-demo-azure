# Databricks notebook source
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

# MAGIC %md ###### Raw data delta configuration

# COMMAND ----------

raw_file_path = '/tmp/delta/raw_invoice_data'
raw_table = 'default.raw_invoice_data'

# COMMAND ----------

# MAGIC %md ###### Bronze delta configuration

# COMMAND ----------

bronze_file_path = '/tmp/delta/bronze_invoice_data'
bronze_table = 'default.bronze_invoice_data'
