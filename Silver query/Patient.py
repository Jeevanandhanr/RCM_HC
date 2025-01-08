# Databricks notebook source
#Reading Hospital A patient data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/patients")
df_hosa.createOrReplaceTempView("patients_hosa")

#Reading Hospital B patient data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/patients")
df_hosb.createOrReplaceTempView("patients_hosb")

# COMMAND ----------

 %sql
 select * from patients_hosa

# COMMAND ----------

 %sql
 select * from patients_hosb

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW cdm_patients AS
 SELECT CONCAT(SRC_PatientID,'-', datasource) AS Patient_Key, *
 FROM (
     SELECT 
     PatientID AS SRC_PatientID ,
     FirstName,
     LastName,
     MiddleName,
     SSN,
     PhoneNumber,
     Gender,
     DOB,
     Address,
     ModifiedDate,
     datasource
     FROM patients_hosa
     UNION ALL
     SELECT 
     ID AS SRC_PatientID,
     F_Name AS FirstName,
     L_Name AS LastName,
     M_Name ASMiddleName,
     SSN,
     PhoneNumber,
     Gender,
     DOB,
     Address,
     Updated_Date AS ModifiedDate,
     datasource
      FROM patients_hosb
 )

# COMMAND ----------

 %sql
 select * from cdm_patients

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW quality_checks AS
 SELECT 
     Patient_Key,
     SRC_PatientID,
     FirstName,
     LastName,
     MiddleName,
     SSN,
     PhoneNumber,
     Gender,
     DOB,
     Address,
     ModifiedDate As SRC_ModifiedDate,
     datasource,
     CASE 
         WHEN SRC_PatientID IS NULL OR dob IS NULL OR firstname IS NULL or lower(firstname)='null' THEN TRUE
         ELSE FALSE
     END AS is_quarantined
 FROM cdm_patients

# COMMAND ----------

 %sql
 select * from quality_checks
 order by is_quarantined desc

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.patients (
     Patient_Key STRING,
     SRC_PatientID STRING,
     FirstName STRING,
     LastName STRING,
     MiddleName STRING,
     SSN STRING,
     PhoneNumber STRING,
     Gender STRING,
     DOB DATE,
     Address STRING,
     SRC_ModifiedDate TIMESTAMP,
     datasource STRING,
     is_quarantined BOOLEAN,
     inserted_date TIMESTAMP,
     modified_date TIMESTAMP,
     is_current BOOLEAN
 )
 USING DELTA;

# COMMAND ----------

 %sql
 -- Step 1: Mark existing records as historical (is_current = false) for patients that will be updated
 MERGE INTO silver.patients AS target
 USING quality_checks AS source
 ON target.Patient_Key = source.Patient_Key
 AND target.is_current = true 
 WHEN MATCHED
 AND (
     target.SRC_PatientID <> source.SRC_PatientID OR
     target.FirstName <> source.FirstName OR
     target.LastName <> source.LastName OR
     target.MiddleName <> source.MiddleName OR
     target.SSN <> source.SSN OR
     target.PhoneNumber <> source.PhoneNumber OR
     target.Gender <> source.Gender OR
     target.DOB <> source.DOB OR
     target.Address <> source.Address OR
     target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
     target.datasource <> source.datasource OR
     target.is_quarantined <> source.is_quarantined
 )
 THEN UPDATE SET
     target.is_current = false,
     target.modified_date = current_timestamp()


 WHEN NOT MATCHED
 THEN INSERT (
     Patient_Key,
     SRC_PatientID,
     FirstName,
     LastName,
     MiddleName,
     SSN,
     PhoneNumber,
     Gender,
     DOB,
     Address,
     SRC_ModifiedDate,
     datasource,
     is_quarantined,
     inserted_date,
     modified_date,
     is_current
 )
 VALUES (
     source.Patient_Key,
     source.SRC_PatientID,
     source.FirstName,
     source.LastName,
     source.MiddleName,
     source.SSN,
     source.PhoneNumber,
     source.Gender,
     source.DOB,
     source.Address,
     source.SRC_ModifiedDate,
     source.datasource,
     source.is_quarantined,
     current_timestamp(), -- Set inserted_date to current timestamp
     current_timestamp(), -- Set modified_date to current timestamp
     true -- Mark as current
 );


# COMMAND ----------

 %sql
 MERGE INTO silver.patients AS target
 USING quality_checks AS source
 ON target.Patient_Key = source.Patient_Key
 AND target.is_current = true 
 -- Step 2: Insert new and updated records into the Delta table, marking them as current
 WHEN NOT MATCHED
 THEN INSERT (
     Patient_Key,
     SRC_PatientID,
     FirstName,
     LastName,
     MiddleName,
     SSN,
     PhoneNumber,
     Gender,
     DOB,
     Address,
     SRC_ModifiedDate,
     datasource,
     is_quarantined,
     inserted_date,
     modified_date,
     is_current
 )
 VALUES (
     source.Patient_Key,
     source.SRC_PatientID,
     source.FirstName,
     source.LastName,
     source.MiddleName,
     source.SSN,
     source.PhoneNumber,
     source.Gender,
     source.DOB,
     source.Address,
     source.SRC_ModifiedDate,
     source.datasource,
     source.is_quarantined,
     current_timestamp(), -- Set inserted_date to current timestamp
     current_timestamp(), -- Set modified_date to current timestamp
     true -- Mark as current
 );

# COMMAND ----------

 %sql
 select count(*),Patient_Key from silver.patients
 group by patient_key
 order by 1 desc

# COMMAND ----------