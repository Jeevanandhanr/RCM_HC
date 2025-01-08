# Databricks notebook source
 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_patient
 (
     patient_key STRING,
     src_patientid STRING,
     firstname STRING,
     lastname STRING,
     middlename STRING,
     ssn STRING,
     phonenumber STRING,
     gender STRING,
     dob DATE,
     address STRING,
     datasource STRING
 )

# COMMAND ----------

 %sql 
 truncate TABLE gold.dim_patient 

# COMMAND ----------

 %sql
 insert into gold.dim_patient
 select 
      patient_key ,
     src_patientid ,
     firstname ,
     lastname ,
     middlename ,
     ssn ,
     phonenumber ,
     gender ,
     dob ,
     address ,
     datasource 
  from silver.patients
  where is_current=true and is_quarantined=false