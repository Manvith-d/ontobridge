-- OntoBridge insurance star schema for Fabric Lakehouse (Delta); upload data/sources to Files/raw first.
-- Generated from alignment run-20261021T084623-b7c8 by `ontobridge model`. Regenerate rather than edit.

SET spark.sql.session.timeZone = UTC;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS insurance;

CREATE OR REPLACE TEMPORARY VIEW raw_insurance_agents_crm USING csv OPTIONS (path 'Files/raw/insurance/agents_crm.csv', header 'true');

DROP TABLE IF EXISTS bronze.insurance_agents_crm;

CREATE TABLE bronze.insurance_agents_crm USING DELTA AS
SELECT * FROM raw_insurance_agents_crm;

DROP TABLE IF EXISTS silver.insurance_agents_crm;

CREATE TABLE silver.insurance_agents_crm USING DELTA AS
SELECT
  CAST(`agent_code` AS STRING) AS agent_id,
  CAST(`agent_full_name` AS STRING) AS agent_name,
  CAST(`agency` AS STRING) AS agency_name,
  CAST(`region` AS STRING) AS sales_region,
  CAST(`hire_date` AS DATE) AS agent_hire_date,
  CAST(`email` AS STRING) AS agent_email,
  'insurance/agents_crm.csv' AS _source
FROM bronze.insurance_agents_crm;

CREATE OR REPLACE TEMPORARY VIEW raw_insurance_claims_events USING json OPTIONS (path 'Files/raw/insurance/claims_events.json', multiLine 'true');

DROP TABLE IF EXISTS bronze.insurance_claims_events;

CREATE TABLE bronze.insurance_claims_events USING DELTA AS
SELECT * FROM raw_insurance_claims_events;

DROP TABLE IF EXISTS silver.insurance_claims_events;

CREATE TABLE silver.insurance_claims_events USING DELTA AS
SELECT
  CAST(`claimId` AS STRING) AS claim_id,
  CAST(`policy`.`number` AS STRING) AS policy_number,
  CAST(`lossDate` AS DATE) AS loss_date,
  CAST(`reportedAt` AS TIMESTAMP) AS claim_reported_timestamp,
  CAST(`claimant`.`customerRef` AS STRING) AS customer_id,
  CAST(`claimant`.`fullName` AS STRING) AS customer_name,
  CAST(`reserveAmount` AS DECIMAL(18,2)) AS reserve_amount,
  CAST(`paidAmount` AS DECIMAL(18,2)) AS paid_amount,
  CAST(`status` AS STRING) AS claim_status,
  CAST(`lossCause` AS STRING) AS loss_cause,
  CAST(`location`.`zip` AS STRING) AS postal_code,
  CAST(`location`.`state` AS STRING) AS state_code,
  'insurance/claims_events.json' AS _source
FROM bronze.insurance_claims_events;

CREATE OR REPLACE TEMPORARY VIEW raw_insurance_policy_admin USING csv OPTIONS (path 'Files/raw/insurance/policy_admin.csv', header 'true');

DROP TABLE IF EXISTS bronze.insurance_policy_admin;

CREATE TABLE bronze.insurance_policy_admin USING DELTA AS
SELECT * FROM raw_insurance_policy_admin;

DROP TABLE IF EXISTS silver.insurance_policy_admin;

CREATE TABLE silver.insurance_policy_admin USING DELTA AS
SELECT
  CAST(`POL_NO` AS STRING) AS policy_number,
  CAST(`HOLDER_ID` AS STRING) AS customer_id,
  CAST(`HOLDER_NM` AS STRING) AS customer_name,
  CAST(`EFF_DT` AS DATE) AS policy_effective_date,
  CAST(`EXP_DT` AS DATE) AS policy_expiration_date,
  CAST(`PREM_AMT` AS DECIMAL(18,2)) AS premium_amount,
  CAST(`LOB_CD` AS STRING) AS line_of_business,
  CAST(`ST_CD` AS STRING) AS state_code,
  CAST(`AGT_ID` AS STRING) AS agent_id,
  'insurance/policy_admin.csv' AS _source
FROM bronze.insurance_policy_admin;

DROP TABLE IF EXISTS insurance.dim_agent;

CREATE TABLE insurance.dim_agent USING DELTA AS
SELECT xxhash64(agent_id) AS agent_key, agent_id, max(agency_name) AS agency_name, max(agent_email) AS agent_email, max(agent_name) AS agent_name, max(sales_region) AS sales_region, max(agent_hire_date) AS agent_hire_date
FROM (
  SELECT agent_id, agency_name, agent_email, agent_name, sales_region, agent_hire_date FROM silver.insurance_agents_crm
  UNION ALL
  SELECT agent_id, CAST(NULL AS STRING) AS agency_name, CAST(NULL AS STRING) AS agent_email, CAST(NULL AS STRING) AS agent_name, CAST(NULL AS STRING) AS sales_region, CAST(NULL AS DATE) AS agent_hire_date FROM silver.insurance_policy_admin
) src
WHERE agent_id IS NOT NULL
GROUP BY agent_id
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS insurance.dim_customer;

CREATE TABLE insurance.dim_customer USING DELTA AS
SELECT xxhash64(customer_id) AS customer_key, customer_id, max(customer_name) AS customer_name
FROM (
  SELECT customer_id, customer_name FROM silver.insurance_claims_events
  UNION ALL
  SELECT customer_id, customer_name FROM silver.insurance_policy_admin
) src
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT -1, 'UNKNOWN', NULL;

DROP TABLE IF EXISTS insurance.dim_policy;

CREATE TABLE insurance.dim_policy USING DELTA AS
SELECT xxhash64(policy_number) AS policy_key, policy_number, max(line_of_business) AS line_of_business
FROM (
  SELECT policy_number, CAST(NULL AS STRING) AS line_of_business FROM silver.insurance_claims_events
  UNION ALL
  SELECT policy_number, line_of_business FROM silver.insurance_policy_admin
) src
WHERE policy_number IS NOT NULL
GROUP BY policy_number
UNION ALL
SELECT -1, 'UNKNOWN', NULL;

DROP TABLE IF EXISTS insurance.dim_location;

CREATE TABLE insurance.dim_location USING DELTA AS
SELECT DISTINCT CASE WHEN coalesce(postal_code, state_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(postal_code AS STRING), ''), coalesce(CAST(state_code AS STRING), '')) END AS location_key, postal_code, state_code
FROM (
  SELECT postal_code, state_code FROM silver.insurance_claims_events
  UNION ALL
  SELECT CAST(NULL AS STRING) AS postal_code, state_code FROM silver.insurance_policy_admin
) src
WHERE coalesce(postal_code, state_code) IS NOT NULL
UNION ALL
SELECT -1, NULL, NULL;

DROP TABLE IF EXISTS insurance.dim_date;

CREATE TABLE insurance.dim_date USING DELTA AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS year,
  quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
  dayofmonth(d) AS day_of_month, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d
  FROM (SELECT min(d) AS lo, max(d) AS hi FROM (
    SELECT CAST(loss_date AS DATE) AS d FROM silver.insurance_claims_events
    UNION ALL
    SELECT CAST(claim_reported_timestamp AS DATE) AS d FROM silver.insurance_claims_events
    UNION ALL
    SELECT CAST(policy_effective_date AS DATE) AS d FROM silver.insurance_policy_admin
    UNION ALL
    SELECT CAST(policy_expiration_date AS DATE) AS d FROM silver.insurance_policy_admin
  ) dates) bounds)
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS insurance.fact_claims_events;

CREATE TABLE insurance.fact_claims_events USING DELTA AS
SELECT
  CASE WHEN customer_id IS NULL THEN -1 ELSE xxhash64(customer_id) END AS customer_key,
  CASE WHEN policy_number IS NULL THEN -1 ELSE xxhash64(policy_number) END AS policy_key,
  CASE WHEN coalesce(postal_code, state_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(postal_code AS STRING), ''), coalesce(CAST(state_code AS STRING), '')) END AS location_key,
  coalesce(CAST(date_format(loss_date, 'yyyyMMdd') AS INT), -1) AS loss_date_key,
  coalesce(CAST(date_format(claim_reported_timestamp, 'yyyyMMdd') AS INT), -1) AS claim_reported_date_key,
  claim_id,
  claim_reported_timestamp,
  claim_status,
  loss_cause,
  reserve_amount,
  paid_amount,
  _source
FROM silver.insurance_claims_events;

DROP TABLE IF EXISTS insurance.fact_policy_admin;

CREATE TABLE insurance.fact_policy_admin USING DELTA AS
SELECT
  CASE WHEN agent_id IS NULL THEN -1 ELSE xxhash64(agent_id) END AS agent_key,
  CASE WHEN customer_id IS NULL THEN -1 ELSE xxhash64(customer_id) END AS customer_key,
  CASE WHEN policy_number IS NULL THEN -1 ELSE xxhash64(policy_number) END AS policy_key,
  CASE WHEN coalesce(state_code) IS NULL THEN -1 ELSE xxhash64('', coalesce(CAST(state_code AS STRING), '')) END AS location_key,
  coalesce(CAST(date_format(policy_effective_date, 'yyyyMMdd') AS INT), -1) AS policy_effective_date_key,
  coalesce(CAST(date_format(policy_expiration_date, 'yyyyMMdd') AS INT), -1) AS policy_expiration_date_key,
  premium_amount,
  _source
FROM silver.insurance_policy_admin;

-- Validation: every count must be 0
SELECT 'unique insurance.dim_agent.agent_key' AS check, (SELECT count(*) - count(DISTINCT agent_key) FROM insurance.dim_agent) AS violations
UNION ALL
SELECT 'unique insurance.dim_customer.customer_key' AS check, (SELECT count(*) - count(DISTINCT customer_key) FROM insurance.dim_customer) AS violations
UNION ALL
SELECT 'unique insurance.dim_policy.policy_key' AS check, (SELECT count(*) - count(DISTINCT policy_key) FROM insurance.dim_policy) AS violations
UNION ALL
SELECT 'unique insurance.dim_location.location_key' AS check, (SELECT count(*) - count(DISTINCT location_key) FROM insurance.dim_location) AS violations
UNION ALL
SELECT 'unique insurance.dim_date.date_key' AS check, (SELECT count(*) - count(DISTINCT date_key) FROM insurance.dim_date) AS violations
UNION ALL
SELECT 'rows insurance.fact_claims_events' AS check, (SELECT (SELECT count(*) FROM insurance.fact_claims_events) - (SELECT count(*) FROM silver.insurance_claims_events)) AS violations
UNION ALL
SELECT 'orphans insurance.fact_claims_events.customer_key' AS check, (SELECT count(*) FROM insurance.fact_claims_events f LEFT ANTI JOIN insurance.dim_customer d ON f.customer_key = d.customer_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_claims_events.policy_key' AS check, (SELECT count(*) FROM insurance.fact_claims_events f LEFT ANTI JOIN insurance.dim_policy d ON f.policy_key = d.policy_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_claims_events.location_key' AS check, (SELECT count(*) FROM insurance.fact_claims_events f LEFT ANTI JOIN insurance.dim_location d ON f.location_key = d.location_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_claims_events.loss_date_key' AS check, (SELECT count(*) FROM insurance.fact_claims_events f LEFT ANTI JOIN insurance.dim_date d ON f.loss_date_key = d.date_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_claims_events.claim_reported_date_key' AS check, (SELECT count(*) FROM insurance.fact_claims_events f LEFT ANTI JOIN insurance.dim_date d ON f.claim_reported_date_key = d.date_key) AS violations
UNION ALL
SELECT 'rows insurance.fact_policy_admin' AS check, (SELECT (SELECT count(*) FROM insurance.fact_policy_admin) - (SELECT count(*) FROM silver.insurance_policy_admin)) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.agent_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_agent d ON f.agent_key = d.agent_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.customer_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_customer d ON f.customer_key = d.customer_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.policy_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_policy d ON f.policy_key = d.policy_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.location_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_location d ON f.location_key = d.location_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.policy_effective_date_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_date d ON f.policy_effective_date_key = d.date_key) AS violations
UNION ALL
SELECT 'orphans insurance.fact_policy_admin.policy_expiration_date_key' AS check, (SELECT count(*) FROM insurance.fact_policy_admin f LEFT ANTI JOIN insurance.dim_date d ON f.policy_expiration_date_key = d.date_key) AS violations;
