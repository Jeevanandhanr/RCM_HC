# Databricks notebook source
 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_provider
 (
 ProviderID string,
 FirstName string,
 LastName string,
 DeptID string,
 NPI long,
 datasource string
 )

# COMMAND ----------

 %sql 
 truncate TABLE gold.dim_provider 

# COMMAND ----------

 %sql
 insert into gold.dim_provider
 select 
 ProviderID ,
 FirstName ,
 LastName ,
 concat(DeptID,'-',datasource) deptid,
 NPI ,
 datasource 
  from silver.providers
  where is_quarantined=false