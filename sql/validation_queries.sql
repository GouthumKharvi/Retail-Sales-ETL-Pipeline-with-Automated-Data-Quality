-- =====================================================================
-- validation_queries.sql
-- Post-ETL data quality checks for Retail Sales ETL Pipeline
-- (Safe: These queries DO NOT modify any data)
-- =====================================================================

USE retail_db;

-- ================================================================
-- 1. BASIC ROW COUNT CHECKS
-- ================================================================

-- Count rows in staging tables
SELECT 'sales_staging' AS table_name, COUNT(*) AS total_rows FROM sales_staging;
SELECT 'features_staging' AS table_name, COUNT(*) AS total_rows FROM features_staging;
SELECT 'stores_staging' AS table_name, COUNT(*) AS total_rows FROM stores_staging;

-- Count rows in clean/final tables
SELECT 'sales_clean' AS table_name, COUNT(*) AS total_rows FROM sales_clean;
SELECT 'features_clean' AS table_name, COUNT(*) AS total_rows FROM features_clean;
SELECT 'dim_store' AS table_name, COUNT(*) AS total_rows FROM dim_store;


-- ================================================================
-- 2. NULL / MISSING VALUE CHECKS
-- ================================================================

-- Sales: Check for missing values in critical columns
SELECT 
    SUM(CASE WHEN store IS NULL THEN 1 END) AS null_store,
    SUM(CASE WHEN dept IS NULL THEN 1 END) AS null_dept,
    SUM(CASE WHEN sale_date IS NULL THEN 1 END) AS null_date
FROM sales_clean;

-- Features: Missing feature_date
SELECT
    SUM(CASE WHEN feature_date IS NULL THEN 1 END) AS null_feature_date
FROM features_clean;


-- ================================================================
-- 3. DUPLICATE RECORD CHECKS
-- (Sales should not have duplicates because of UNIQUE KEY)
-- ================================================================

-- Duplicate check in sales_clean (store + dept + sale_date)
SELECT 
    store, dept, sale_date, COUNT(*) AS cnt
FROM sales_clean
GROUP BY store, dept, sale_date
HAVING COUNT(*) > 1;


-- ================================================================
-- 4. REFERENTIAL INTEGRITY CHECKS
-- Show any sales rows referencing stores not in dim_store
-- ================================================================

SELECT 
    s.store, COUNT(*) AS missing_store_refs
FROM sales_clean s
LEFT JOIN dim_store d ON s.store = d.store
WHERE d.store IS NULL
GROUP BY s.store;


-- ================================================================
-- 5. VALUE RANGE CHECKS
-- ================================================================

-- Detect negative weekly sales (unusual but possible)
SELECT *
FROM sales_clean
WHERE weekly_sales < 0
LIMIT 20;

-- Detect impossible temperature values
SELECT *
FROM features_clean
WHERE temperature NOT BETWEEN -50 AND 150
LIMIT 20;

-- Detect invalid unemployment values
SELECT *
FROM features_clean
WHERE unemployment < 0
LIMIT 20;


-- ================================================================
-- 6. DATE CONSISTENCY CHECK
-- Sales date must match feature date for same store
-- ================================================================

SELECT 
    s.store, s.sale_date, f.feature_date
FROM sales_clean s
LEFT JOIN features_clean f
ON s.store = f.store AND s.sale_date = f.feature_date
WHERE f.feature_date IS NULL
LIMIT 20;


-- =====================================================================
-- END OF FILE
-- =====================================================================
