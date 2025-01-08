# Databricks notebook source
 %sql
 -- Create temporary views for the parquet files
 CREATE OR REPLACE TEMP VIEW hosa_encounters
 USING parquet
 OPTIONS (
   path "dbfs:/mnt/bronze/hosa/encounters"
 );

 CREATE OR REPLACE TEMP VIEW hosb_encounters
 USING parquet
 OPTIONS (
   path "dbfs:/mnt/bronze/hosb/encounters"
 );

 -- Union the two views
 CREATE OR REPLACE TEMP VIEW encounters AS
 SELECT * FROM hosa_encounters
 UNION ALL
 SELECT * FROM hosb_encounters;

 -- Display the merged data
 SELECT * FROM encounters;

# COMMAND ----------

 %sql
 select * from encounters

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW quality_checks AS
 SELECT 
 concat(EncounterID,'-',datasource) as EncounterID,
 EncounterID SRC_EncounterID,
 PatientID,
 EncounterDate,
 EncounterType,
 ProviderID,
 DepartmentID,
 ProcedureCode,
 InsertedDate as SRC_InsertedDate,
 ModifiedDate as SRC_ModifiedDate,
 datasource,
     CASE 
         WHEN EncounterID IS NULL OR PatientID IS NULL THEN TRUE
         ELSE FALSE
     END AS is_quarantined
 FROM encounters

# COMMAND ----------

 %sql
 select * from quality_checks
 where datasource='hos-b'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.encounters (
 EncounterID string,
 SRC_EncounterID string,
 PatientID string,
 EncounterDate date,
 EncounterType string,
 ProviderID string,
 DepartmentID string,
 ProcedureCode integer,
 SRC_InsertedDate date,
 SRC_ModifiedDate date,
 datasource string,
 is_quarantined boolean,
 audit_insertdate timestamp,
 audit_modifieddate timestamp,
 is_current boolean
 )
 USING DELTA;

# COMMAND ----------

 %sql
 -- Update old record to implement SCD Type 2
 MERGE INTO silver.encounters AS target
 USING quality_checks AS source
 ON target.EncounterID = source.EncounterID AND target.is_current = true
 WHEN MATCHED AND (
     target.SRC_EncounterID != source.SRC_EncounterID OR
     target.PatientID != source.PatientID OR
     target.EncounterDate != source.EncounterDate OR
     target.EncounterType != source.EncounterType OR
     target.ProviderID != source.ProviderID OR
     target.DepartmentID != source.DepartmentID OR
     target.ProcedureCode != source.ProcedureCode OR
     target.SRC_InsertedDate != source.SRC_InsertedDate OR
     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
     target.datasource != source.datasource OR
     target.is_quarantined != source.is_quarantined
 ) THEN
   UPDATE SET
     target.is_current = false,
     target.audit_modifieddate = current_timestamp()


# COMMAND ----------

 %sql
 -- Insert new record to implement SCD Type 2
 MERGE INTO silver.encounters AS target USING quality_checks AS source ON target.EncounterID = source.EncounterID
 AND target.is_current = true
 WHEN NOT MATCHED THEN
 INSERT
   (
     EncounterID,
     SRC_EncounterID,
     PatientID,
     EncounterDate,
     EncounterType,
     ProviderID,
     DepartmentID,
     ProcedureCode,
     SRC_InsertedDate,
     SRC_ModifiedDate,
     datasource,
     is_quarantined,
     audit_insertdate,
     audit_modifieddate,
     is_current
   )
 VALUES
   (
     source.EncounterID,
     source.SRC_EncounterID,
     source.PatientID,
     source.EncounterDate,
     source.EncounterType,
     source.ProviderID,
     source.DepartmentID,
     source.ProcedureCode,
     source.SRC_InsertedDate,
     source.SRC_ModifiedDate,
     source.datasource,
     source.is_quarantined,
     current_timestamp(),
     current_timestamp(),
     true
   );

# COMMAND ----------



# COMMAND ----------

 %sql
 select SRC_EncounterID,datasource,count(patientid) from  silver.encounters
 group by all
 order by 3 desc