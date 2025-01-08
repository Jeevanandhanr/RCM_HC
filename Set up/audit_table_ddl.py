# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS rcm.audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS rcm.audit.load_logs (
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rcm.audit.load_logs

# COMMAND ----------


