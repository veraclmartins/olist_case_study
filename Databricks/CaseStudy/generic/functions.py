# Databricks notebook source
import os
from pyspark.sql.functions import *
import unicodedata

# COMMAND ----------

# MAGIC %md # PathExists
# MAGIC Given a path, detrmines if it exists on file system
# MAGIC
# MAGIC Parameters:
# MAGIC  - path;
# MAGIC  
# MAGIC Returns:
# MAGIC  - True if exists

# COMMAND ----------

def PathExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# MAGIC %md # AddHashKey
# MAGIC Given a dataframe and a column name, creates a new column on the dataframe with an hash key
# MAGIC
# MAGIC Function:
# MAGIC  - AddHashKey
# MAGIC  
# MAGIC Parameters:
# MAGIC  - param_df: Spark Dataframe;
# MAGIC  - param_ColName: Name of the columne to create;
# MAGIC  - param_ignoreList: list of column names to ignore in the hash key
# MAGIC
# MAGIC Returns:
# MAGIC  - result: Spark Dataframe

# COMMAND ----------

def AddHashKey(param_df, param_colName, param_ignoreList = []):
  colList = []
  
  for c in param_df.columns:
    if c not in param_ignoreList:
      colList.append(c)
  
  res = param_df.withColumn(param_colName,sha2(concat(*(when(col(column).isNull(), '').otherwise(col(column).cast("string")) for column in param_df.select(colList).columns)), 256))
      
  return res

# COMMAND ----------

# MAGIC %md # ValidationResults
# MAGIC This class represents an object that will return validation information at execution of function ValidateDuplicateKeys
# MAGIC
# MAGIC Constructor parameters:
# MAGIC - valid: True/False if it is valid (no duplicate keys) or not (with duplicate keys);
# MAGIC - keu_columns: List of Key Columns on the dataframe
# MAGIC
# MAGIC Properties:
# MAGIC - valid: True/False if it is valid (no duplicate keys) or not (with duplicate keys);
# MAGIC - keu_columns: List of Key Columns on the dataframe

# COMMAND ----------


class ValidationResult:
  def __init__(self, valid, key_columns):
    self.valid = valid
    self.key_columns = key_columns

# COMMAND ----------

# MAGIC
# MAGIC %md # ValidateDuplicateKeys
# MAGIC
# MAGIC Given a dataframe with column names starting by 'KEY_', validates if the dataframe has duplicate KEY rows
# MAGIC
# MAGIC Function:
# MAGIC  - ValidateDuplicateKeys
# MAGIC  
# MAGIC Parameters:
# MAGIC  - param_df: dataframe object;
# MAGIC
# MAGIC Returns:
# MAGIC  - Object: ValidationResults
# MAGIC

# COMMAND ----------

def ValidateDuplicateKeys(param_df, param_ignoreCols = []):
  final_df = param_df
  key_columns = [column for column in final_df.columns if 'KEY_' in column]
  cols = [column for column in final_df.columns if column not in param_ignoreCols]
  res = ValidationResult(True, key_columns)
  df_test = final_df.select(cols).distinct()
  
  if len(key_columns) != 0:
    df_dups = df_test.drop_duplicates(key_columns)
    res.valid = df_dups.count() == df_test.count()
  
  return res

# COMMAND ----------

# MAGIC
# MAGIC %md # PrepareToSilver
# MAGIC
# MAGIC Given a dataframe adds a Hash column to it and validates if for duplicate rows
# MAGIC
# MAGIC Function:
# MAGIC  - PrepareToSilver
# MAGIC  
# MAGIC Parameters:
# MAGIC  - param_df: dataframe to be prepared
# MAGIC
# MAGIC Returns:
# MAGIC  - dataframe: same input dataframe with the hash column

# COMMAND ----------

def PrepareToSilver(param_df, param_ignoreList = []):

  final_df = AddHashKey(param_df, 'HASHCOLUMN', param_ignoreList)
  notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  
  validRes = ValidateDuplicateKeys(final_df, param_ignoreList)

  if validRes.valid == False:
    raise Exception('The result query for table ' + notebook_name + ' has duplicate key values (' + ', '.join(validRes.key_columns) + ')')
  
  return final_df

# COMMAND ----------

# MAGIC %md # SaveToSilver
# MAGIC Given a dataframe and a table name, saves the dataframe as delta table into Silver layer
# MAGIC
# MAGIC Function:
# MAGIC  - SaveToSilver
# MAGIC  
# MAGIC Parameters:
# MAGIC  - param_df: Spark Dataframe;
# MAGIC  - param_folder: Silver folder
# MAGIC  - param_tableName: table name

# COMMAND ----------

def SaveToSilver(param_df, param_folder, param_tableName):
    path = os.path.join(param_folder, param_tableName)
    param_df.write.mode('append').format("delta").save(path)

# COMMAND ----------

# MAGIC %md # RemoveAccents
# MAGIC Given a string returns the same string without accented characters

# COMMAND ----------

def RemoveAccents(param_string):
  if param_string == None:
    return None
  else:
    return ''.join((c for c in unicodedata.normalize('NFD', param_string) if unicodedata.category(c) != 'Mn'))

# COMMAND ----------

RemoveAccentsUDF = udf(lambda z: RemoveAccents(z),StringType())

# COMMAND ----------

spark.udf.register("RemoveAccentsUDF", RemoveAccentsUDF)