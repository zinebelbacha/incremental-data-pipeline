# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Flag Parameter

# COMMAND ----------

# incremental_flag with default value 0
dbutils.widgets.text('incremental_flag', 'o')

# COMMAND ----------

# get the value of the incremental_flag (will be used for incremental load and intial load)
# incremental flag is a string
# incremental_flad is used to determine if the data is incremental or initial load
incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimenion Model 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql(
    """
    SELECT DISTINCT(MODEL_ID) as Model_ID, model_category 
    FROM PARQUET.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`
    """
)

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink -  Initial & Incremental

# COMMAND ----------

# this query is never true, so it will not return any data but it will only return the SCHEMA of the dimension table that we want to create
# this condition is only for initial load
# if the table does not exist, I want to get the schema
# if the table exists (incremental load), bring all the data (which will be used in the left join)
# WHERE 1=0 is a SQL trick to return no rows
if not spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_model_key, Model_ID, model_category 
        FROM PARQUET.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_model_key, Model_ID, model_category 
        FROM cars_catalog.gold.dim_model
        '''
    )

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering new records & old records

# COMMAND ----------

# Perform a left join between the source and sink (target) using Model_ID.
# This helps identify new vs existing records.
# df_src is the left table
df_filter = df_src.join(df_sink, df_src.Model_ID == df_sink.Model_ID, 'left').select(df_src.Model_ID, df_src.model_category, df_sink.dim_model_key)
# All rows from df_src are included.
# If there’s a matching Model_ID in df_sink, we get dim_model_key filled in.
# If not, dim_model_key is NULL.

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_model_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_model_key.isNull()).select('Model_ID', 'model_category')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key 

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch the max Surrogate Key from existing table**

# COMMAND ----------

if (incremental_flag == '0'): 
  max_value = 1
else:
  if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    max_value_df = spark.sql("SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    max_value_df = spark.sql("SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column & ADD the max surrogate key**

# COMMAND ----------

from pyspark.sql import functions as F
df_filter_new = df_filter_new.withColumn('dim_model_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create final DF - df_filter_olf + df_filter_new**

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slowly Changing Dimension (SCD) TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental RUN 
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@carsdatalake.dfs.core.windows.net/dim_model")
    # update when the value exists
    # insert when new value 
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_model_key = source.dim_model_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial RUN 
else: # no table exists
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@carsdatalake.dfs.core.windows.net/dim_model")\
        .saveAsTable("cars_catalog.gold.dim_model")
    

# MATCH on dim_model_key
# If matched → update all columns
#If not matched → insert as new row

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_model

# COMMAND ----------

