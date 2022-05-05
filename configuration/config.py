# Databricks notebook source
# Variables set using the following commands in the databricks-cli:
# databricks secrets create-scope --scope users --initial-manage-principle "users"
# databricks secrets put --scope users --key keyName

# COMMAND ----------

import sys

endpoint = dbutils.secrets.get(scope="users", key="CLIENT_ID")
secret = dbutils.secrets.get(scope="users", key="TOKEN")
add_id = dbutils.secrets.get(scope="users", key="APP_ID")
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
