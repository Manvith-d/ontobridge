-- OntoBridge healthcare star schema for Fabric Lakehouse (Delta); upload data/sources to Files/raw first.
-- Generated from alignment run-20261021T084623-b7c8 by `ontobridge model`. Regenerate rather than edit.

SET spark.sql.session.timeZone = UTC;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS healthcare;

CREATE OR REPLACE TEMPORARY VIEW raw_healthcare_ehr_encounters USING json OPTIONS (path 'Files/raw/healthcare/ehr_encounters.json', multiLine 'true');

DROP TABLE IF EXISTS bronze.healthcare_ehr_encounters;

CREATE TABLE bronze.healthcare_ehr_encounters USING DELTA AS
SELECT * FROM raw_healthcare_ehr_encounters;

DROP TABLE IF EXISTS silver.healthcare_ehr_encounters;

CREATE TABLE silver.healthcare_ehr_encounters USING DELTA AS
SELECT
  CAST(`encounterId` AS STRING) AS encounter_id,
  CAST(`patient`.`mrn` AS STRING) AS patient_mrn,
  CAST(`patient`.`dob` AS DATE) AS patient_birth_date,
  CAST(`patient`.`sex` AS STRING) AS patient_sex,
  CAST(`admitDate` AS DATE) AS admit_date,
  CAST(`dischargeDate` AS DATE) AS discharge_date,
  CAST(`encounterType` AS STRING) AS encounter_type,
  CAST(`primaryDx` AS STRING) AS primary_diagnosis_code,
  CAST(`attendingNpi` AS STRING) AS provider_npi,
  CAST(`facility`.`code` AS STRING) AS facility_code,
  CAST(`facility`.`state` AS STRING) AS state_code,
  CAST(`totalCharges` AS DECIMAL(18,2)) AS total_charges,
  'healthcare/ehr_encounters.json' AS _source
FROM bronze.healthcare_ehr_encounters;

CREATE OR REPLACE TEMPORARY VIEW raw_healthcare_pharmacy_fills USING csv OPTIONS (path 'Files/raw/healthcare/pharmacy_fills.csv', header 'true');

DROP TABLE IF EXISTS bronze.healthcare_pharmacy_fills;

CREATE TABLE bronze.healthcare_pharmacy_fills USING DELTA AS
SELECT * FROM raw_healthcare_pharmacy_fills;

DROP TABLE IF EXISTS silver.healthcare_pharmacy_fills;

CREATE TABLE silver.healthcare_pharmacy_fills USING DELTA AS
SELECT
  CAST(`rx_id` AS STRING) AS prescription_id,
  CAST(`patient_mrn` AS STRING) AS patient_mrn,
  CAST(`ndc_code` AS STRING) AS ndc_code,
  CAST(`fill_dt` AS DATE) AS fill_date,
  CAST(`days_supply` AS INT) AS days_supply,
  CAST(`qty_dispensed` AS DECIMAL(18,2)) AS quantity_dispensed,
  CAST(`copay_amt` AS DECIMAL(18,2)) AS copay_amount,
  CAST(`prescriber_npi` AS STRING) AS provider_npi,
  'healthcare/pharmacy_fills.csv' AS _source
FROM bronze.healthcare_pharmacy_fills;

CREATE OR REPLACE TEMPORARY VIEW raw_healthcare_provider_directory USING csv OPTIONS (path 'Files/raw/healthcare/provider_directory.csv', header 'true');

DROP TABLE IF EXISTS bronze.healthcare_provider_directory;

CREATE TABLE bronze.healthcare_provider_directory USING DELTA AS
SELECT * FROM raw_healthcare_provider_directory;

DROP TABLE IF EXISTS silver.healthcare_provider_directory;

CREATE TABLE silver.healthcare_provider_directory USING DELTA AS
SELECT
  CAST(`npi` AS STRING) AS provider_npi,
  CAST(`provider_name` AS STRING) AS provider_name,
  CAST(`specialty` AS STRING) AS provider_specialty,
  CAST(`practice_zip` AS STRING) AS postal_code,
  CAST(`practice_state` AS STRING) AS state_code,
  CAST(`accepting_new` AS BOOLEAN) AS accepting_new_patients,
  'healthcare/provider_directory.csv' AS _source
FROM bronze.healthcare_provider_directory;

DROP TABLE IF EXISTS healthcare.dim_facility;

CREATE TABLE healthcare.dim_facility USING DELTA AS
SELECT xxhash64(facility_code) AS facility_key, facility_code
FROM (
  SELECT facility_code FROM silver.healthcare_ehr_encounters
) src
WHERE facility_code IS NOT NULL
GROUP BY facility_code
UNION ALL
SELECT -1, 'UNKNOWN';

DROP TABLE IF EXISTS healthcare.dim_patient;

CREATE TABLE healthcare.dim_patient USING DELTA AS
SELECT xxhash64(patient_mrn) AS patient_key, patient_mrn, max(patient_birth_date) AS patient_birth_date, max(patient_sex) AS patient_sex
FROM (
  SELECT patient_mrn, patient_birth_date, patient_sex FROM silver.healthcare_ehr_encounters
  UNION ALL
  SELECT patient_mrn, CAST(NULL AS DATE) AS patient_birth_date, CAST(NULL AS STRING) AS patient_sex FROM silver.healthcare_pharmacy_fills
) src
WHERE patient_mrn IS NOT NULL
GROUP BY patient_mrn
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL;

DROP TABLE IF EXISTS healthcare.dim_provider;

CREATE TABLE healthcare.dim_provider USING DELTA AS
SELECT xxhash64(provider_npi) AS provider_key, provider_npi, max(accepting_new_patients) AS accepting_new_patients, max(provider_name) AS provider_name, max(provider_specialty) AS provider_specialty, max(postal_code) AS postal_code, max(state_code) AS state_code
FROM (
  SELECT provider_npi, CAST(NULL AS BOOLEAN) AS accepting_new_patients, CAST(NULL AS STRING) AS provider_name, CAST(NULL AS STRING) AS provider_specialty, CAST(NULL AS STRING) AS postal_code, state_code FROM silver.healthcare_ehr_encounters
  UNION ALL
  SELECT provider_npi, CAST(NULL AS BOOLEAN) AS accepting_new_patients, CAST(NULL AS STRING) AS provider_name, CAST(NULL AS STRING) AS provider_specialty, CAST(NULL AS STRING) AS postal_code, CAST(NULL AS STRING) AS state_code FROM silver.healthcare_pharmacy_fills
  UNION ALL
  SELECT provider_npi, accepting_new_patients, provider_name, provider_specialty, postal_code, state_code FROM silver.healthcare_provider_directory
) src
WHERE provider_npi IS NOT NULL
GROUP BY provider_npi
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS healthcare.dim_location;

CREATE TABLE healthcare.dim_location USING DELTA AS
SELECT DISTINCT CASE WHEN coalesce(state_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(state_code AS STRING), '')) END AS location_key, state_code
FROM (
  SELECT state_code FROM silver.healthcare_ehr_encounters
) src
WHERE coalesce(state_code) IS NOT NULL
UNION ALL
SELECT -1, NULL;

DROP TABLE IF EXISTS healthcare.dim_date;

CREATE TABLE healthcare.dim_date USING DELTA AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS year,
  quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
  dayofmonth(d) AS day_of_month, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d
  FROM (SELECT min(d) AS lo, max(d) AS hi FROM (
    SELECT CAST(admit_date AS DATE) AS d FROM silver.healthcare_ehr_encounters
    UNION ALL
    SELECT CAST(discharge_date AS DATE) AS d FROM silver.healthcare_ehr_encounters
    UNION ALL
    SELECT CAST(fill_date AS DATE) AS d FROM silver.healthcare_pharmacy_fills
  ) dates) bounds)
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS healthcare.fact_ehr_encounters;

CREATE TABLE healthcare.fact_ehr_encounters USING DELTA AS
SELECT
  CASE WHEN facility_code IS NULL THEN -1 ELSE xxhash64(facility_code) END AS facility_key,
  CASE WHEN patient_mrn IS NULL THEN -1 ELSE xxhash64(patient_mrn) END AS patient_key,
  CASE WHEN provider_npi IS NULL THEN -1 ELSE xxhash64(provider_npi) END AS provider_key,
  CASE WHEN coalesce(state_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(state_code AS STRING), '')) END AS location_key,
  coalesce(CAST(date_format(admit_date, 'yyyyMMdd') AS INT), -1) AS admit_date_key,
  coalesce(CAST(date_format(discharge_date, 'yyyyMMdd') AS INT), -1) AS discharge_date_key,
  encounter_id,
  encounter_type,
  primary_diagnosis_code,
  total_charges,
  _source
FROM silver.healthcare_ehr_encounters;

DROP TABLE IF EXISTS healthcare.fact_pharmacy_fills;

CREATE TABLE healthcare.fact_pharmacy_fills USING DELTA AS
SELECT
  CASE WHEN patient_mrn IS NULL THEN -1 ELSE xxhash64(patient_mrn) END AS patient_key,
  CASE WHEN provider_npi IS NULL THEN -1 ELSE xxhash64(provider_npi) END AS provider_key,
  coalesce(CAST(date_format(fill_date, 'yyyyMMdd') AS INT), -1) AS fill_date_key,
  prescription_id,
  ndc_code,
  days_supply,
  quantity_dispensed,
  copay_amount,
  _source
FROM silver.healthcare_pharmacy_fills;

-- Validation: every count must be 0
SELECT 'unique healthcare.dim_facility.facility_key' AS check, (SELECT count(*) - count(DISTINCT facility_key) FROM healthcare.dim_facility) AS violations
UNION ALL
SELECT 'unique healthcare.dim_patient.patient_key' AS check, (SELECT count(*) - count(DISTINCT patient_key) FROM healthcare.dim_patient) AS violations
UNION ALL
SELECT 'unique healthcare.dim_provider.provider_key' AS check, (SELECT count(*) - count(DISTINCT provider_key) FROM healthcare.dim_provider) AS violations
UNION ALL
SELECT 'unique healthcare.dim_location.location_key' AS check, (SELECT count(*) - count(DISTINCT location_key) FROM healthcare.dim_location) AS violations
UNION ALL
SELECT 'unique healthcare.dim_date.date_key' AS check, (SELECT count(*) - count(DISTINCT date_key) FROM healthcare.dim_date) AS violations
UNION ALL
SELECT 'rows healthcare.fact_ehr_encounters' AS check, (SELECT (SELECT count(*) FROM healthcare.fact_ehr_encounters) - (SELECT count(*) FROM silver.healthcare_ehr_encounters)) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.facility_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_facility d ON f.facility_key = d.facility_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.patient_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_patient d ON f.patient_key = d.patient_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.provider_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_provider d ON f.provider_key = d.provider_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.location_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_location d ON f.location_key = d.location_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.admit_date_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_date d ON f.admit_date_key = d.date_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_ehr_encounters.discharge_date_key' AS check, (SELECT count(*) FROM healthcare.fact_ehr_encounters f LEFT ANTI JOIN healthcare.dim_date d ON f.discharge_date_key = d.date_key) AS violations
UNION ALL
SELECT 'rows healthcare.fact_pharmacy_fills' AS check, (SELECT (SELECT count(*) FROM healthcare.fact_pharmacy_fills) - (SELECT count(*) FROM silver.healthcare_pharmacy_fills)) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_pharmacy_fills.patient_key' AS check, (SELECT count(*) FROM healthcare.fact_pharmacy_fills f LEFT ANTI JOIN healthcare.dim_patient d ON f.patient_key = d.patient_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_pharmacy_fills.provider_key' AS check, (SELECT count(*) FROM healthcare.fact_pharmacy_fills f LEFT ANTI JOIN healthcare.dim_provider d ON f.provider_key = d.provider_key) AS violations
UNION ALL
SELECT 'orphans healthcare.fact_pharmacy_fills.fill_date_key' AS check, (SELECT count(*) FROM healthcare.fact_pharmacy_fills f LEFT ANTI JOIN healthcare.dim_date d ON f.fill_date_key = d.date_key) AS violations;
