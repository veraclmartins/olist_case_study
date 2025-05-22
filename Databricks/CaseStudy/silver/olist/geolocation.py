# Databricks notebook source
# MAGIC %md
# MAGIC # Load functions

# COMMAND ----------

# MAGIC %run ../../generic/functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Initiate variables

# COMMAND ----------
storage_account_name = 'sadevdp00001'
storage_account_access_key = <access_key>
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
source_container = 'bronze'
source_path = "wasbs://" + source_container + "@" + storage_account_name + ".blob.core.windows.net/bronze/olist/"
source_name = 'geolocation'
sink_container = 'silver'
sink_path = "wasbs://" + sink_container + "@" + storage_account_name + ".blob.core.windows.net/silver/"
sink_table = 'DIM1_' + source_name

# COMMAND ----------

# MAGIC %md
# MAGIC # Initiate widgets

# COMMAND ----------

# Create a widget for execution date
dbutils.widgets.text("currentDate", "")

# Get the value from the widget
currentDate = dbutils.widgets.get("currentDate")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get last execution data

# COMMAND ----------

lastFolder = dbutils.fs.ls(source_path)[-1]

print('Last folder: ' + lastFolder.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data, prepare to load and load to silver

# COMMAND ----------

df = spark.read.parquet(lastFolder.path + source_name)\
          .withColumnRenamed("geolocation_zip_code_prefix", "KEY_geolocation_zip_code")\
          .withColumnRenamed("geolocation_state", "KEY_geolocation_state")\
          .withColumn("KEY_geolocation_city", RemoveAccentsUDF(col("geolocation_city")))\
          .withColumn("LoadDate", lit(currentDate))\
          .select("KEY_geolocation_zip_code", "KEY_geolocation_city", "KEY_geolocation_state", "LoadDate").distinct()

silver_df = PrepareToSilver(df)

SaveToSilver(silver_df, sink_path, sink_table)