# Databricks notebook source
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import *
import json
# import re
# from functools import reduce
# from pyspark.sql import DataFrame
# import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### create_scd1
# MAGIC
# MAGIC Creates the delta table in the gold layer for the first time.  
# MAGIC This is a simple copy activity from the silver layer with the creation of a Surrogate Key in between.  
# MAGIC The Surrogate Key will always be in the format "SK_name" where name is the name of the table without the prefix. 
# MAGIC
# MAGIC Params:
# MAGIC   - silver_folder: folder where the table is stored in the silver layer.
# MAGIC   - gold_folder: folder where the table is going to be stored in the gold layer.
# MAGIC   - table_name: name of table. As of right now, this function should only be applied to tables with prefix "DIM1".
# MAGIC   - unique_cols: array of columns that form the unique key for each row. This should be passed as an array of strings.

# COMMAND ----------

def create_scd1(silver_folder, gold_folder, table_name, unique_cols):
  
  silver_tablePath = os.path.join(silver_folder, table_name)
  gold_tablePath = os.path.join(gold_path, table_name)
  sk_name = 'SK_'+ '_'.join(table_name.split('_')[1:])
  
  df = spark.read.format("delta").load(silver_tablePath)\
          .withColumn(sk_name, row_number().over(Window().orderBy(unique_cols)).cast("bigint"))
  
  df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').save(gold_tablePath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### merge_scd1
# MAGIC
# MAGIC Performs a SCD1 merge on the target table.

# COMMAND ----------

def merge_scd1(current_date, source_table, target_table, unique_cols):
  
  silver_tablePath = os.path.join(source_table, table_name)
  gold_tablePath = os.path.join(target_table, table_name)

  silver_df = spark.read.format("delta").load(silver_tablePath)
  gold_df = spark.read.format("delta").load(gold_tablePath)
  cols_exclude = unique_cols + ["SK_", "HASHCOLUMN"]
  cols_array = [c for c in gold_df.columns if c not in cols_exclude]
  sk_name = ''.join([c for c in gold_df.columns if "SK_" in c])  
  
  max_sk = gold_df.select(max(sk_name).alias("max_sk")).collect()[0][0]
  silver_df = silver_df.withColumn("sk_nm", try_add(row_number().over(Window().orderBy(cols_array)).cast("bigint"), coalesce(lit(max_sk), lit(0))))
  
  gold_delta = DeltaTable.forPath(spark, gold_tablePath)
  gold_delta.alias('gold') \
  .merge(
    silver_df.alias('silver'),
    ' and '.join(['"gold.' + c + '" = "silver.' + c + '"' for c in unique_cols]) 
  ) \
  .whenMatchedUpdate( condition = "gold.HASHCOLUMN != silver.HASHCOLUMN",
    set =
    {
      json.loads(','.join(['"' + c + '":"silver.' + c + '"' for c in cols_array]) + ' , "HASHCOLUMN": "silver.HASHCOLUMN"')
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      json.loads(','.join(['"' + c + '":"silver.' + c + '"' for c in cols_array]) +' , "HASHCOLUMN" : "silver.HASHCOLUMN"')
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### create_scd2
# MAGIC
# MAGIC Creates the delta table in the gold layer for the first time.  
# MAGIC This is a simple copy activity from the silver layer with the creation of a Surrogate Key and the columns "created_date" and "updated_date" in between.  
# MAGIC The Surrogate Key will always be in the format "SK_name" where name is the name of the table without the prefix. 
# MAGIC
# MAGIC created_date will always be 190001010000 by default.  
# MAGIC updated_date will always be 999912312359 by default.
# MAGIC
# MAGIC
# MAGIC Params:
# MAGIC   - silver_folder: folder where the table is stored in the silver layer.
# MAGIC   - gold_folder: folder where the table is going to be stored in the gold layer.
# MAGIC   - table_name: name of table. As of right now, this function should only be applied to tables with prefix "DIM2".
# MAGIC   - unique_cols: array of columns that form the unique key for each row. This should be passed as an array of strings.

# COMMAND ----------

def create_scd2(silver_folder, gold_folder, table_name, unique_cols):
  
  silver_tablePath = os.path.join(silver_folder, table_name)
  gold_tablePath = os.path.join(gold_folder, table_name)

  silver_df = spark.read.format("delta").load(silver_tablePath)
  sk_name = 'SK_'+ '_'.join(table_name.split('_')[1:])

  df = spark.read.format("delta").load(silver_tablePath)\
          .withColumn(sk_name, row_number().over(Window().orderBy(unique_cols)).cast("bigint"))\
          .withColumn("created_date", lit("190001010000"))\
          .withColumn("updated_date", lit("999912312359"))
  df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').save(gold_tablePath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### action_to_perform
# MAGIC
# MAGIC Returns a SQL query with each action (new_insert, exist_insert, update, delete) to perform in a SCD2 merge statement. This function is used by merge_scd2.
# MAGIC
# MAGIC Params:
# MAGIC   - source_table: source table with the differences to the target table.
# MAGIC   - target_table: target table to merge.
# MAGIC   - unique_cols: array of columns that form the unique key for each row.

# COMMAND ----------

def action_to_perform(source_table, target_table, unique_cols, current_date):
  sk_name = 'SK_'+ '_'.join(table_name.split('_')[1:])
  cols = unique_cols + [sk_name]

  df_targetMax = target_table.groupby(unique_cols).agg(max(sk_name).alias(sk_name))
  df_target = target_table.alias("t1").join(df_targetMax.alias("tmax"), cols, "inner").select("t1.*")
  df = source_table.alias("S").join(df_target.alias("T"), unique_cols, "left")\
            .withColumn("Action", when(col("T.HASHCOLUMN").isNull(), lit('new_insert'))\
                                .when((col("T.updated_date") < lit("999912312359")) & (col("T.updated_date").isNotNull()), lit('exist_insert'))\
                                .when((col("T.HASHCOLUMN") != col("S.HASHCOLUMN")) & (col("T.updated_date") == lit("999912312359")), lit('update'))\
                                .when((col("T.HASHCOLUMN") == col("S.HASHCOLUMN")) & (col("T.updated_date") == lit("999912312359")), lit('delete'))\
                                .otherwise(None))\
            .withColumn("updated_date", when(col("Action") == 'new_insert', lit("999912312359"))\
                                .when(col("Action") == 'exist_insert', lit("999912312359"))\
                                .when(col("Action") == 'update', date_format(lit(current_date), "yyyyMMddhhmm"))\
                                .when(col("Action") == 'delete', date_format(lit(current_date), "yyyyMMddhhmm"))\
                                .otherwise(None))\
            .withColumn("created_date", when(col("Action") == 'new_insert', lit("190001010000"))\
                                .when(col("Action") == 'exist_insert', date_format(lit(current_date), "yyyyMMddhhmm"))\
                                .when(col("Action") == 'update', col("T.created_date"))\
                                .when(col("Action") == 'delete', col("T.created_date"))\
                                .otherwise(None))\
            .select("S.*", "Action", "created_date", "updated_date")
  
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### merge_scd2
# MAGIC
# MAGIC Performs a SCD2 merge on the target table.
# MAGIC
# MAGIC Info:  
# MAGIC The **target table** should have the following columns: created_date, updated_date, HASHCOLUMN.  
# MAGIC The **source table** should have the following columns: HASHCOLUMN.  
# MAGIC The source and the target tables should have the same attributes.  
# MAGIC By default, when a new row is inserted, created_date will be set to "190001010000" and updated_date will be set to "999912312359".
# MAGIC
# MAGIC Params:
# MAGIC   - current_date: current datetime in the format "yyyyMMddHHmm". This should be passed as a string.
# MAGIC   - source_table: source table with the differences to the target table. This should be passed as a string.
# MAGIC   - target_table: target table to merge. This should be passed as a string.
# MAGIC   - unique_cols: array of columns that form the unique key for each row. This should be passed as an array of strings.
# MAGIC   
# MAGIC Example:
# MAGIC
# MAGIC merge_scd2('202107071058', 'src_tbl', 'target_tbl', ['col1, col2'])

# COMMAND ----------

def merge_scd2(current_date, source_table, target_table, unique_cols):
  silver_tablePath = os.path.join(source_table, table_name)
  gold_tablePath = os.path.join(target_table, table_name)

  silver_df = spark.read.format("delta").load(silver_tablePath)
  gold_df = spark.read.format("delta").load(gold_tablePath)

  df_action = action_to_perform(silver_df, gold_df, unique_cols, current_date)

  sk_name = ''.join([c for c in gold_df.columns if "SK_" in c])
  cols_exclude = unique_cols + [sk_name, "HASHCOLUMN", "created_date", "updated_date"]
  cols_array = [c for c in gold_df.columns if c not in cols_exclude]
  
  max_sk = gold_df.select(max(sk_name).alias("max_sk")).collect()[0][0]
  df_action = df_action.withColumn(sk_name, try_add(row_number().over(Window().orderBy(cols_array)).cast("bigint"), coalesce(lit(max_sk), lit(0))))
  
  gold_delta = DeltaTable.forPath(spark, gold_tablePath)

  ni_json = '{'+','.join(['"' + c + '":"silver.' + c + '"' for c in df_action.columns if c != 'Action'])+ ' , "HASHCOLUMN" : "silver.HASHCOLUMN", "created_date": "silver.created_date", "updated_date": "silver.updated_date"' + '}'
  ei_json = '{'+','.join(['"' + c + '":"silver.' + c + '"' for c in df_action.columns if c != 'Action']) + ' , "HASHCOLUMN" : "silver.HASHCOLUMN", "created_date": "silver.created_date", "updated_date": "silver.updated_date"' + '}'
  
  gold_delta.alias('gold') \
  .merge(
    df_action.alias('silver'),
    ' AND '.join(['silver.' + column + ' = gold.' + column for column in unique_cols]) + ' AND gold.updated_date = 999912312359'
  ) \
  .whenMatchedUpdate( condition = "silver.Action in ('updated', 'delete')",
    set =
    {
      "updated_date": "silver.updated_date"
    }
  ) \
  .whenNotMatchedInsert( condition = "silver.Action in ('new_insert')",
    values = json.loads(ni_json)
  ) \
  .whenNotMatchedInsert(condition = "silver.Action in ('exist_insert')",
  values = json.loads(ei_json)
  ) \
  .execute()