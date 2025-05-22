# Databricks notebook source
# MAGIC %md
# MAGIC # Load functions

# COMMAND ----------

# MAGIC %run ../generic/functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Initiate widgets

# COMMAND ----------

# Create a widget for execution date, table name
dbutils.widgets.text("currentDate", "")
dbutils.widgets.text("tableName", "")

# Get the value from the widget
currentDate = dbutils.widgets.get("currentDate")
table_name = dbutils.widgets.get("tableName")

# COMMAND ----------

# MAGIC %run ../generic/SCDFunctions

# COMMAND ----------

# MAGIC %md # Initiate variables

# COMMAND ----------

storage_account_name = 'sadevdp00001'
storage_account_access_key = <access_key>
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
silver_container = 'silver'
gold_container = 'gold'
silver_path = "wasbs://" + silver_container + "@" + storage_account_name + ".blob.core.windows.net/"
gold_path = "wasbs://" + gold_container + "@" + storage_account_name + ".blob.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data into gold

# COMMAND ----------

dim_type = table_name.split('_')[0]
dim_name = '_'.join(table_name.split('_')[1:])
silver_tablePath = os.path.join(silver_path, table_name)
gold_tablePath = os.path.join(gold_path, table_name)

silver_df = spark.read.format("delta").load(silver_tablePath)

if PathExists(silver_tablePath):
  unique_cols = [c for c in silver_df.columns if "KEY_" in c]
  
  if PathExists(gold_tablePath):
    if silver_df.count() > 0:
      if dim_type.upper() == "DIM1":
        merge_scd1(currentDate, silver_path, gold_path, unique_cols)

      elif dim_type.upper() == "DIM2":
        merge_scd2(currentDate, silver_path, gold_path, unique_cols)

      else:
        raise Exception("It was not possible to identify the type of the table for " + table_name + ' during merge.')
    else:
      dbutils.notebook.exit("Table " + silver_table + " is empty. Nothing to be merged.")
  else:
    if dim_type.upper() == "DIM1":
      create_scd1(silver_path, gold_path, table_name, unique_cols)

    elif dim_type.upper() == "DIM2":
      create_scd2(silver_path, gold_path, table_name, unique_cols)

    else:
      raise Exception("It was not possible to identify the type of the table for " + table_name + ' during table creation.')
else:
  dbutils.notebook.exit('Delta table ' + table_name + ' does not exist on folder ' + silver_path + '.')