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
    SELECT Branch_ID,BranchName 
    FROM PARQUET.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`
    """
)

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink -  Initial & Incremental

# COMMAND ----------


if not spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_branch_key, Branch_ID, BranchName 
        FROM PARQUET.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_branch_key, Branch_ID, BranchName
        FROM cars_catalog.gold.dim_branch
        '''
    )

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering new records & old records

# COMMAND ----------

# df_src is the left table, df_sink left table in the left join. 
df_filter = df_src.join(df_sink, df_src.Branch_ID == df_sink.Branch_ID, 'left').select(df_src.Branch_ID, df_src.BranchName, df_sink.dim_branch_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select('Branch_ID', 'BranchName')

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
  if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    max_value_df = spark.sql("SELECT max(dim_branch_key) FROM cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]
  else:
    max_value = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column & ADD the max surrogate key**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.orderBy("Branch_ID")  # or another stable column
df_filter_new = df_filter_new.withColumn("rn", row_number().over(w))
df_filter_new = df_filter_new.withColumn("dim_branch_key", F.lit(max_value) + F.col("rn"))
df_filter_new = df_filter_new.drop("rn")

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
if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@carsdatalake.dfs.core.windows.net/dim_branch")
    # update when the value exists
    # insert when new value 
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_branch_key = source.dim_branch_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial RUN 
else: # no table exists
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@carsdatalake.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_branch

# COMMAND ----------

df_final.groupBy("dim_branch_key").count().filter("count > 1").show()

# COMMAND ----------

