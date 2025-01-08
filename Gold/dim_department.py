# Databricks notebook source
 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_department
 (
 Dept_Id string,
 SRC_Dept_Id string,
 Name string,
 datasource string
 )

# COMMAND ----------

 %sql 
 truncate TABLE gold.dim_department 

# COMMAND ----------

 %sql
 insert into gold.dim_department
 select 
 distinct
 Dept_Id ,
 SRC_Dept_Id ,
 Name ,
 datasource 
  from silver.departments
  where is_quarantined=false

# COMMAND ----------

 %sql 
 select * from gold.dim_department
