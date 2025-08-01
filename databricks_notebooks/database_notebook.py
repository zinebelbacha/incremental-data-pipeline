# Databricks notebook source
# MAGIC %md
# MAGIC **Create Catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Schemas**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold

# COMMAND ----------

