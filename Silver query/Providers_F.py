# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/providers")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/providers")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("providers")


# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.providers (
 ProviderID string,
 FirstName string,
 LastName string,
 Specialization string,
 DeptID string,
 NPI long,
 datasource string,
 is_quarantined boolean
 )
 USING DELTA;

# COMMAND ----------

 %sql
 truncate table silver.providers

# COMMAND ----------

 %sql 
 insert into silver.providers
 select 
 distinct
 ProviderID,
 FirstName,
 LastName,
 Specialization,
 DeptID,
 cast(NPI as INT) NPI,
 datasource,
     CASE 
         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
         ELSE FALSE
     END AS is_quarantined
 from providers