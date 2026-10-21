-- OntoBridge finance star schema for Fabric Lakehouse (Delta); upload data/sources to Files/raw first.
-- Generated from alignment run-20261021T084623-b7c8 by `ontobridge model`. Regenerate rather than edit.

SET spark.sql.session.timeZone = UTC;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS finance;

CREATE OR REPLACE TEMPORARY VIEW raw_finance_branch_registry USING csv OPTIONS (path 'Files/raw/finance/branch_registry.csv', header 'true');

DROP TABLE IF EXISTS bronze.finance_branch_registry;

CREATE TABLE bronze.finance_branch_registry USING DELTA AS
SELECT * FROM raw_finance_branch_registry;

DROP TABLE IF EXISTS silver.finance_branch_registry;

CREATE TABLE silver.finance_branch_registry USING DELTA AS
SELECT
  CAST(`branch_id` AS STRING) AS branch_id,
  CAST(`branch_name` AS STRING) AS branch_name,
  CAST(`branch_city` AS STRING) AS city,
  CAST(`branch_state` AS STRING) AS state_code,
  CAST(`opened_on` AS DATE) AS branch_open_date,
  CAST(`manager_name` AS STRING) AS manager_name,
  'finance/branch_registry.csv' AS _source
FROM bronze.finance_branch_registry;

CREATE OR REPLACE TEMPORARY VIEW raw_finance_card_transactions USING json OPTIONS (path 'Files/raw/finance/card_transactions.json', multiLine 'true');

DROP TABLE IF EXISTS bronze.finance_card_transactions;

CREATE TABLE bronze.finance_card_transactions USING DELTA AS
SELECT * FROM raw_finance_card_transactions;

DROP TABLE IF EXISTS silver.finance_card_transactions;

CREATE TABLE silver.finance_card_transactions USING DELTA AS
SELECT
  CAST(`txnRef` AS STRING) AS card_transaction_id,
  CAST(`card`.`last4` AS STRING) AS card_last_4,
  CAST(`card`.`holderId` AS STRING) AS customer_id,
  CAST(`auth_ts` AS TIMESTAMP) AS authorization_timestamp,
  CAST(`amount`.`value` AS DECIMAL(18,2)) AS transaction_amount,
  CAST(`amount`.`currency` AS STRING) AS currency_code,
  CAST(`merchant`.`name` AS STRING) AS merchant_name,
  CAST(`merchant`.`mcc` AS STRING) AS merchant_category_code,
  CAST(`merchant`.`city` AS STRING) AS city,
  CAST(`merchant`.`country` AS STRING) AS country_code,
  CAST(`isFraudFlag` AS BOOLEAN) AS fraud_flag,
  'finance/card_transactions.json' AS _source
FROM bronze.finance_card_transactions;

CREATE OR REPLACE TEMPORARY VIEW raw_finance_ledger_entries USING csv OPTIONS (path 'Files/raw/finance/ledger_entries.csv', header 'true');

DROP TABLE IF EXISTS bronze.finance_ledger_entries;

CREATE TABLE bronze.finance_ledger_entries USING DELTA AS
SELECT * FROM raw_finance_ledger_entries;

DROP TABLE IF EXISTS silver.finance_ledger_entries;

CREATE TABLE silver.finance_ledger_entries USING DELTA AS
SELECT
  CAST(`je_id` AS STRING) AS journal_entry_id,
  CAST(`acct_no` AS STRING) AS account_number,
  CAST(`posting_date` AS DATE) AS posting_date,
  CAST(`debit_amt` AS DECIMAL(18,2)) AS debit_amount,
  CAST(`credit_amt` AS DECIMAL(18,2)) AS credit_amount,
  CAST(`cost_center` AS STRING) AS cost_center,
  CAST(`currency_cd` AS STRING) AS currency_code,
  'finance/ledger_entries.csv' AS _source
FROM bronze.finance_ledger_entries;

DROP TABLE IF EXISTS finance.dim_account;

CREATE TABLE finance.dim_account USING DELTA AS
SELECT xxhash64(account_number) AS account_key, account_number, max(cost_center) AS cost_center
FROM (
  SELECT account_number, cost_center FROM silver.finance_ledger_entries
) src
WHERE account_number IS NOT NULL
GROUP BY account_number
UNION ALL
SELECT -1, 'UNKNOWN', NULL;

DROP TABLE IF EXISTS finance.dim_branch;

CREATE TABLE finance.dim_branch USING DELTA AS
SELECT xxhash64(branch_id) AS branch_key, branch_id, max(branch_name) AS branch_name, max(manager_name) AS manager_name, max(city) AS city, max(state_code) AS state_code, max(branch_open_date) AS branch_open_date
FROM (
  SELECT branch_id, branch_name, manager_name, city, state_code, branch_open_date FROM silver.finance_branch_registry
) src
WHERE branch_id IS NOT NULL
GROUP BY branch_id
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS finance.dim_currency;

CREATE TABLE finance.dim_currency USING DELTA AS
SELECT xxhash64(currency_code) AS currency_key, currency_code
FROM (
  SELECT currency_code FROM silver.finance_card_transactions
  UNION ALL
  SELECT currency_code FROM silver.finance_ledger_entries
) src
WHERE currency_code IS NOT NULL
GROUP BY currency_code
UNION ALL
SELECT -1, 'UNKNOWN';

DROP TABLE IF EXISTS finance.dim_customer;

CREATE TABLE finance.dim_customer USING DELTA AS
SELECT xxhash64(customer_id) AS customer_key, customer_id
FROM (
  SELECT customer_id FROM silver.finance_card_transactions
) src
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT -1, 'UNKNOWN';

DROP TABLE IF EXISTS finance.dim_merchant;

CREATE TABLE finance.dim_merchant USING DELTA AS
SELECT xxhash64(merchant_name) AS merchant_key, merchant_name, max(merchant_category_code) AS merchant_category_code
FROM (
  SELECT merchant_name, merchant_category_code FROM silver.finance_card_transactions
) src
WHERE merchant_name IS NOT NULL
GROUP BY merchant_name
UNION ALL
SELECT -1, 'UNKNOWN', NULL;

DROP TABLE IF EXISTS finance.dim_location;

CREATE TABLE finance.dim_location USING DELTA AS
SELECT DISTINCT CASE WHEN coalesce(city, country_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(city AS STRING), ''), coalesce(CAST(country_code AS STRING), '')) END AS location_key, city, country_code
FROM (
  SELECT city, country_code FROM silver.finance_card_transactions
) src
WHERE coalesce(city, country_code) IS NOT NULL
UNION ALL
SELECT -1, NULL, NULL;

DROP TABLE IF EXISTS finance.dim_date;

CREATE TABLE finance.dim_date USING DELTA AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS year,
  quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
  dayofmonth(d) AS day_of_month, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d
  FROM (SELECT min(d) AS lo, max(d) AS hi FROM (
    SELECT CAST(authorization_timestamp AS DATE) AS d FROM silver.finance_card_transactions
    UNION ALL
    SELECT CAST(posting_date AS DATE) AS d FROM silver.finance_ledger_entries
  ) dates) bounds)
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS finance.fact_card_transactions;

CREATE TABLE finance.fact_card_transactions USING DELTA AS
SELECT
  CASE WHEN currency_code IS NULL THEN -1 ELSE xxhash64(currency_code) END AS currency_key,
  CASE WHEN customer_id IS NULL THEN -1 ELSE xxhash64(customer_id) END AS customer_key,
  CASE WHEN merchant_name IS NULL THEN -1 ELSE xxhash64(merchant_name) END AS merchant_key,
  CASE WHEN coalesce(city, country_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(city AS STRING), ''), coalesce(CAST(country_code AS STRING), '')) END AS location_key,
  coalesce(CAST(date_format(authorization_timestamp, 'yyyyMMdd') AS INT), -1) AS authorization_date_key,
  card_transaction_id,
  card_last_4,
  authorization_timestamp,
  fraud_flag,
  transaction_amount,
  _source
FROM silver.finance_card_transactions;

DROP TABLE IF EXISTS finance.fact_ledger_entries;

CREATE TABLE finance.fact_ledger_entries USING DELTA AS
SELECT
  CASE WHEN account_number IS NULL THEN -1 ELSE xxhash64(account_number) END AS account_key,
  CASE WHEN currency_code IS NULL THEN -1 ELSE xxhash64(currency_code) END AS currency_key,
  coalesce(CAST(date_format(posting_date, 'yyyyMMdd') AS INT), -1) AS posting_date_key,
  journal_entry_id,
  debit_amount,
  credit_amount,
  _source
FROM silver.finance_ledger_entries;

-- Validation: every count must be 0
SELECT 'unique finance.dim_account.account_key' AS check, (SELECT count(*) - count(DISTINCT account_key) FROM finance.dim_account) AS violations
UNION ALL
SELECT 'unique finance.dim_branch.branch_key' AS check, (SELECT count(*) - count(DISTINCT branch_key) FROM finance.dim_branch) AS violations
UNION ALL
SELECT 'unique finance.dim_currency.currency_key' AS check, (SELECT count(*) - count(DISTINCT currency_key) FROM finance.dim_currency) AS violations
UNION ALL
SELECT 'unique finance.dim_customer.customer_key' AS check, (SELECT count(*) - count(DISTINCT customer_key) FROM finance.dim_customer) AS violations
UNION ALL
SELECT 'unique finance.dim_merchant.merchant_key' AS check, (SELECT count(*) - count(DISTINCT merchant_key) FROM finance.dim_merchant) AS violations
UNION ALL
SELECT 'unique finance.dim_location.location_key' AS check, (SELECT count(*) - count(DISTINCT location_key) FROM finance.dim_location) AS violations
UNION ALL
SELECT 'unique finance.dim_date.date_key' AS check, (SELECT count(*) - count(DISTINCT date_key) FROM finance.dim_date) AS violations
UNION ALL
SELECT 'rows finance.fact_card_transactions' AS check, (SELECT (SELECT count(*) FROM finance.fact_card_transactions) - (SELECT count(*) FROM silver.finance_card_transactions)) AS violations
UNION ALL
SELECT 'orphans finance.fact_card_transactions.currency_key' AS check, (SELECT count(*) FROM finance.fact_card_transactions f LEFT ANTI JOIN finance.dim_currency d ON f.currency_key = d.currency_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_card_transactions.customer_key' AS check, (SELECT count(*) FROM finance.fact_card_transactions f LEFT ANTI JOIN finance.dim_customer d ON f.customer_key = d.customer_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_card_transactions.merchant_key' AS check, (SELECT count(*) FROM finance.fact_card_transactions f LEFT ANTI JOIN finance.dim_merchant d ON f.merchant_key = d.merchant_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_card_transactions.location_key' AS check, (SELECT count(*) FROM finance.fact_card_transactions f LEFT ANTI JOIN finance.dim_location d ON f.location_key = d.location_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_card_transactions.authorization_date_key' AS check, (SELECT count(*) FROM finance.fact_card_transactions f LEFT ANTI JOIN finance.dim_date d ON f.authorization_date_key = d.date_key) AS violations
UNION ALL
SELECT 'rows finance.fact_ledger_entries' AS check, (SELECT (SELECT count(*) FROM finance.fact_ledger_entries) - (SELECT count(*) FROM silver.finance_ledger_entries)) AS violations
UNION ALL
SELECT 'orphans finance.fact_ledger_entries.account_key' AS check, (SELECT count(*) FROM finance.fact_ledger_entries f LEFT ANTI JOIN finance.dim_account d ON f.account_key = d.account_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_ledger_entries.currency_key' AS check, (SELECT count(*) FROM finance.fact_ledger_entries f LEFT ANTI JOIN finance.dim_currency d ON f.currency_key = d.currency_key) AS violations
UNION ALL
SELECT 'orphans finance.fact_ledger_entries.posting_date_key' AS check, (SELECT count(*) FROM finance.fact_ledger_entries f LEFT ANTI JOIN finance.dim_date d ON f.posting_date_key = d.date_key) AS violations;
