-- OntoBridge retail star schema for Fabric Lakehouse (Delta); upload data/sources to Files/raw first.
-- Generated from alignment run-20261021T084623-b7c8 by `ontobridge model`. Regenerate rather than edit.

SET spark.sql.session.timeZone = UTC;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS retail;

CREATE OR REPLACE TEMPORARY VIEW raw_retail_ecommerce_orders USING json OPTIONS (path 'Files/raw/retail/ecommerce_orders.json', multiLine 'true');

DROP TABLE IF EXISTS bronze.retail_ecommerce_orders;

CREATE TABLE bronze.retail_ecommerce_orders USING DELTA AS
SELECT * FROM raw_retail_ecommerce_orders;

DROP TABLE IF EXISTS silver.retail_ecommerce_orders;

CREATE TABLE silver.retail_ecommerce_orders USING DELTA AS
SELECT
  CAST(`order_id` AS STRING) AS order_id,
  CAST(`customer`.`id` AS STRING) AS customer_id,
  CAST(`customer`.`email` AS STRING) AS customer_email,
  CAST(`customer`.`name` AS STRING) AS customer_name,
  CAST(`placed_at` AS TIMESTAMP) AS order_timestamp,
  CAST(`items_total` AS DECIMAL(18,2)) AS order_subtotal,
  CAST(`shipping_fee` AS DECIMAL(18,2)) AS shipping_fee,
  CAST(`ship_to`.`postal_code` AS STRING) AS postal_code,
  CAST(`ship_to`.`country` AS STRING) AS country_code,
  CAST(`channel` AS STRING) AS sales_channel,
  'retail/ecommerce_orders.json' AS _source
FROM bronze.retail_ecommerce_orders;

CREATE OR REPLACE TEMPORARY VIEW raw_retail_pos_sales USING csv OPTIONS (path 'Files/raw/retail/pos_sales.csv', header 'true');

DROP TABLE IF EXISTS bronze.retail_pos_sales;

CREATE TABLE bronze.retail_pos_sales USING DELTA AS
SELECT * FROM raw_retail_pos_sales;

DROP TABLE IF EXISTS silver.retail_pos_sales;

CREATE TABLE silver.retail_pos_sales USING DELTA AS
SELECT
  CAST(`txn_id` AS STRING) AS transaction_id,
  CAST(`store_no` AS STRING) AS store_id,
  CAST(`sku` AS STRING) AS product_sku,
  CAST(`qty` AS INT) AS quantity,
  CAST(`unit_price` AS DECIMAL(18,2)) AS unit_price,
  CAST(`disc_amt` AS DECIMAL(18,2)) AS discount_amount,
  CAST(`txn_ts` AS TIMESTAMP) AS sale_timestamp,
  CAST(`cust_loyalty_id` AS STRING) AS customer_id,
  'retail/pos_sales.csv' AS _source
FROM bronze.retail_pos_sales;

CREATE OR REPLACE TEMPORARY VIEW raw_retail_product_catalog USING csv OPTIONS (path 'Files/raw/retail/product_catalog.csv', header 'true');

DROP TABLE IF EXISTS bronze.retail_product_catalog;

CREATE TABLE bronze.retail_product_catalog USING DELTA AS
SELECT * FROM raw_retail_product_catalog;

DROP TABLE IF EXISTS silver.retail_product_catalog;

CREATE TABLE silver.retail_product_catalog USING DELTA AS
SELECT
  CAST(`product_sku` AS STRING) AS product_sku,
  CAST(`product_title` AS STRING) AS product_name,
  CAST(`category_lvl1` AS STRING) AS product_category,
  CAST(`brand_name` AS STRING) AS brand_name,
  CAST(`list_price` AS DECIMAL(18,2)) AS list_price,
  CAST(`launch_dt` AS DATE) AS product_launch_date,
  'retail/product_catalog.csv' AS _source
FROM bronze.retail_product_catalog;

DROP TABLE IF EXISTS retail.dim_customer;

CREATE TABLE retail.dim_customer USING DELTA AS
SELECT xxhash64(customer_id) AS customer_key, customer_id, max(customer_email) AS customer_email, max(customer_name) AS customer_name
FROM (
  SELECT customer_id, customer_email, customer_name FROM silver.retail_ecommerce_orders
  UNION ALL
  SELECT customer_id, CAST(NULL AS STRING) AS customer_email, CAST(NULL AS STRING) AS customer_name FROM silver.retail_pos_sales
) src
WHERE customer_id IS NOT NULL
GROUP BY customer_id
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL;

DROP TABLE IF EXISTS retail.dim_product;

CREATE TABLE retail.dim_product USING DELTA AS
SELECT xxhash64(product_sku) AS product_key, product_sku, max(brand_name) AS brand_name, max(list_price) AS list_price, max(product_category) AS product_category, max(product_name) AS product_name, max(product_launch_date) AS product_launch_date
FROM (
  SELECT product_sku, CAST(NULL AS STRING) AS brand_name, CAST(NULL AS DECIMAL(18,2)) AS list_price, CAST(NULL AS STRING) AS product_category, CAST(NULL AS STRING) AS product_name, CAST(NULL AS DATE) AS product_launch_date FROM silver.retail_pos_sales
  UNION ALL
  SELECT product_sku, brand_name, list_price, product_category, product_name, product_launch_date FROM silver.retail_product_catalog
) src
WHERE product_sku IS NOT NULL
GROUP BY product_sku
UNION ALL
SELECT -1, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS retail.dim_store;

CREATE TABLE retail.dim_store USING DELTA AS
SELECT xxhash64(store_id) AS store_key, store_id
FROM (
  SELECT store_id FROM silver.retail_pos_sales
) src
WHERE store_id IS NOT NULL
GROUP BY store_id
UNION ALL
SELECT -1, 'UNKNOWN';

DROP TABLE IF EXISTS retail.dim_location;

CREATE TABLE retail.dim_location USING DELTA AS
SELECT DISTINCT CASE WHEN coalesce(postal_code, country_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(postal_code AS STRING), ''), coalesce(CAST(country_code AS STRING), '')) END AS location_key, postal_code, country_code
FROM (
  SELECT postal_code, country_code FROM silver.retail_ecommerce_orders
) src
WHERE coalesce(postal_code, country_code) IS NOT NULL
UNION ALL
SELECT -1, NULL, NULL;

DROP TABLE IF EXISTS retail.dim_date;

CREATE TABLE retail.dim_date USING DELTA AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS year,
  quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
  dayofmonth(d) AS day_of_month, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d
  FROM (SELECT min(d) AS lo, max(d) AS hi FROM (
    SELECT CAST(order_timestamp AS DATE) AS d FROM silver.retail_ecommerce_orders
    UNION ALL
    SELECT CAST(sale_timestamp AS DATE) AS d FROM silver.retail_pos_sales
  ) dates) bounds)
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;

DROP TABLE IF EXISTS retail.fact_ecommerce_orders;

CREATE TABLE retail.fact_ecommerce_orders USING DELTA AS
SELECT
  CASE WHEN customer_id IS NULL THEN -1 ELSE xxhash64(customer_id) END AS customer_key,
  CASE WHEN coalesce(postal_code, country_code) IS NULL THEN -1 ELSE xxhash64(coalesce(CAST(postal_code AS STRING), ''), coalesce(CAST(country_code AS STRING), '')) END AS location_key,
  coalesce(CAST(date_format(order_timestamp, 'yyyyMMdd') AS INT), -1) AS order_date_key,
  order_id,
  order_timestamp,
  sales_channel,
  order_subtotal,
  shipping_fee,
  _source
FROM silver.retail_ecommerce_orders;

DROP TABLE IF EXISTS retail.fact_pos_sales;

CREATE TABLE retail.fact_pos_sales USING DELTA AS
SELECT
  CASE WHEN customer_id IS NULL THEN -1 ELSE xxhash64(customer_id) END AS customer_key,
  CASE WHEN product_sku IS NULL THEN -1 ELSE xxhash64(product_sku) END AS product_key,
  CASE WHEN store_id IS NULL THEN -1 ELSE xxhash64(store_id) END AS store_key,
  coalesce(CAST(date_format(sale_timestamp, 'yyyyMMdd') AS INT), -1) AS sale_date_key,
  transaction_id,
  sale_timestamp,
  quantity,
  unit_price,
  discount_amount,
  _source
FROM silver.retail_pos_sales;

-- Validation: every count must be 0
SELECT 'unique retail.dim_customer.customer_key' AS check, (SELECT count(*) - count(DISTINCT customer_key) FROM retail.dim_customer) AS violations
UNION ALL
SELECT 'unique retail.dim_product.product_key' AS check, (SELECT count(*) - count(DISTINCT product_key) FROM retail.dim_product) AS violations
UNION ALL
SELECT 'unique retail.dim_store.store_key' AS check, (SELECT count(*) - count(DISTINCT store_key) FROM retail.dim_store) AS violations
UNION ALL
SELECT 'unique retail.dim_location.location_key' AS check, (SELECT count(*) - count(DISTINCT location_key) FROM retail.dim_location) AS violations
UNION ALL
SELECT 'unique retail.dim_date.date_key' AS check, (SELECT count(*) - count(DISTINCT date_key) FROM retail.dim_date) AS violations
UNION ALL
SELECT 'rows retail.fact_ecommerce_orders' AS check, (SELECT (SELECT count(*) FROM retail.fact_ecommerce_orders) - (SELECT count(*) FROM silver.retail_ecommerce_orders)) AS violations
UNION ALL
SELECT 'orphans retail.fact_ecommerce_orders.customer_key' AS check, (SELECT count(*) FROM retail.fact_ecommerce_orders f LEFT ANTI JOIN retail.dim_customer d ON f.customer_key = d.customer_key) AS violations
UNION ALL
SELECT 'orphans retail.fact_ecommerce_orders.location_key' AS check, (SELECT count(*) FROM retail.fact_ecommerce_orders f LEFT ANTI JOIN retail.dim_location d ON f.location_key = d.location_key) AS violations
UNION ALL
SELECT 'orphans retail.fact_ecommerce_orders.order_date_key' AS check, (SELECT count(*) FROM retail.fact_ecommerce_orders f LEFT ANTI JOIN retail.dim_date d ON f.order_date_key = d.date_key) AS violations
UNION ALL
SELECT 'rows retail.fact_pos_sales' AS check, (SELECT (SELECT count(*) FROM retail.fact_pos_sales) - (SELECT count(*) FROM silver.retail_pos_sales)) AS violations
UNION ALL
SELECT 'orphans retail.fact_pos_sales.customer_key' AS check, (SELECT count(*) FROM retail.fact_pos_sales f LEFT ANTI JOIN retail.dim_customer d ON f.customer_key = d.customer_key) AS violations
UNION ALL
SELECT 'orphans retail.fact_pos_sales.product_key' AS check, (SELECT count(*) FROM retail.fact_pos_sales f LEFT ANTI JOIN retail.dim_product d ON f.product_key = d.product_key) AS violations
UNION ALL
SELECT 'orphans retail.fact_pos_sales.store_key' AS check, (SELECT count(*) FROM retail.fact_pos_sales f LEFT ANTI JOIN retail.dim_store d ON f.store_key = d.store_key) AS violations
UNION ALL
SELECT 'orphans retail.fact_pos_sales.sale_date_key' AS check, (SELECT count(*) FROM retail.fact_pos_sales f LEFT ANTI JOIN retail.dim_date d ON f.sale_date_key = d.date_key) AS violations;
