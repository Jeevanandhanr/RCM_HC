# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/transactions")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/transactions")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged)

df_merged.createOrReplaceTempView("transactions")



 %sql
  CREATE OR REPLACE TEMP VIEW quality_checks AS
  SELECT 
  concat(TransactionID,'-',datasource) as TransactionID,
  TransactionID as SRC_TransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  VisitDate,
  ServiceDate,
  PaidDate,
  VisitType,
  Amount,
  AmountType,
  PaidAmount,
  ClaimID,
  PayorID,
  ProcedureCode,
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  InsertDate as SRC_InsertDate,
  ModifiedDate as SRC_ModifiedDate,
  datasource,
      CASE 
          WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
          ELSE FALSE
      END AS is_quarantined
  FROM transactions


  %sql
  CREATE TABLE IF NOT EXISTS silver.transactions (
    TransactionID string,
    SRC_TransactionID string,
    EncounterID string,
    PatientID string,
    ProviderID string,
    DeptID string,
    VisitDate date,
    ServiceDate date,
    PaidDate date,
    VisitType string,
    Amount double,
    AmountType string,
    PaidAmount double,
    ClaimID string,
    PayorID string,
    ProcedureCode integer,
    ICDCode string,
    LineOfBusiness string,
    MedicaidID string,
    MedicareID string,
    SRC_InsertDate date,
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
  MERGE INTO silver.transactions AS target USING quality_checks AS source ON target.TransactionID = source.TransactionID
  AND target.is_current = true
  WHEN MATCHED
  AND (
    target.SRC_TransactionID != source.SRC_TransactionID
    OR target.EncounterID != source.EncounterID
    OR target.PatientID != source.PatientID
    OR target.ProviderID != source.ProviderID
    OR target.DeptID != source.DeptID
    OR target.VisitDate != source.VisitDate
    OR target.ServiceDate != source.ServiceDate
    OR target.PaidDate != source.PaidDate
    OR target.VisitType != source.VisitType
    OR target.Amount != source.Amount
    OR target.AmountType != source.AmountType
    OR target.PaidAmount != source.PaidAmount
    OR target.ClaimID != source.ClaimID
    OR target.PayorID != source.PayorID
    OR target.ProcedureCode != source.ProcedureCode
    OR target.ICDCode != source.ICDCode
    OR target.LineOfBusiness != source.LineOfBusiness
    OR target.MedicaidID != source.MedicaidID
    OR target.MedicareID != source.MedicareID
    OR target.SRC_InsertDate != source.SRC_InsertDate
    OR target.SRC_ModifiedDate != source.SRC_ModifiedDate
    OR target.datasource != source.datasource
    OR target.is_quarantined != source.is_quarantined
  ) THEN
  UPDATE
  SET
    target.is_current = false,
    target.audit_modifieddate = current_timestamp()

# COMMAND ----------

  %sql
  -- Insert new record to implement SCD Type 2
  MERGE INTO silver.transactions AS target USING quality_checks AS source ON target.TransactionID = source.TransactionID
  AND target.is_current = true
  WHEN NOT MATCHED THEN
  INSERT
    (
      TransactionID,
      SRC_TransactionID,
      EncounterID,
      PatientID,
      ProviderID,
      DeptID,
      VisitDate,
      ServiceDate,
      PaidDate,
      VisitType,
      Amount,
      AmountType,
      PaidAmount,
      ClaimID,
      PayorID,
      ProcedureCode,
      ICDCode,
      LineOfBusiness,
      MedicaidID,
      MedicareID,
      SRC_InsertDate,
      SRC_ModifiedDate,
      datasource,
      is_quarantined,
      audit_insertdate,
      audit_modifieddate,
      is_current
    )
  VALUES
    (
      source.TransactionID,
      source.SRC_TransactionID,
      source.EncounterID,
      source.PatientID,
      source.ProviderID,
      source.DeptID,
      source.VisitDate,
      source.ServiceDate,
      source.PaidDate,
      source.VisitType,
      source.Amount,
      source.AmountType,
      source.PaidAmount,
      source.ClaimID,
      source.PayorID,
      source.ProcedureCode,
      source.ICDCode,
      source.LineOfBusiness,
      source.MedicaidID,
      source.MedicareID,
      source.SRC_InsertDate,
      source.SRC_ModifiedDate,
      source.datasource,
      source.is_quarantined,
      current_timestamp(),
      current_timestamp(),
      true
    );