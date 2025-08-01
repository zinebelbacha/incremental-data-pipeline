# Databricks notebook source
# MAGIC %md
# MAGIC **Data reading**

# COMMAND ----------

df=spark.read.format("parquet")\
    .option('inferSchema',True)\
        .load('abfss://bronze@carsdatalake.dfs.core.windows.net/raw data')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Transformation**

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df=df.withColumn('model_category', F.split(df['Model_ID'], '-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('revenue_per_unit', df['Revenue']/df['Units_Sold'])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **AD-HOC Analysis (Data aggregation)**

# COMMAND ----------

from pyspark.sql.functions import sum as F_sum
df.groupBy('Year', 'BranchName').agg(
    F_sum('Units_Sold').alias('Total_Units_Sold')
).sort('Year', 'Total_Units_Sold', ascending=[True, False]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data writing**

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@carsdatalake.dfs.core.windows.net/carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Querying silver data**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`abfss://silver@carsdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

