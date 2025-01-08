# Databricks notebook source
 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_cpt_code
 (
 cpt_codes string,
 procedure_code_category string,
 procedure_code_descriptions string,
 code_status string,
 refreshed_at timestamp
 )

# COMMAND ----------

 %sql 
 truncate TABLE gold.dim_cpt_code 

# COMMAND ----------

 %sql
 insert into gold.dim_cpt_code
 select 
 cpt_codes,
 procedure_code_category,
 procedure_code_descriptions ,
 code_status,
 current_timestamp() as refreshed_at
  from silver.cptcodes
  where is_quarantined=false and is_current=true

# COMMAND ----------

 %sql 
 select * from gold.dim_cpt_code