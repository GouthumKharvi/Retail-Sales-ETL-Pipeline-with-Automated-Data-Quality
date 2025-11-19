-- =====================================================================
-- load_clean.sql
-- SQL logic to load cleaned data from staging into final tables
-- (Python load.py already performs this — DO NOT RUN)
-- =====================================================================

USE retail_db;

-- =====================================================================
-- 1. LOAD CLEAN SALES (From sales_staging → sales_clean)
-- =====================================================================

-- Empty table before insert
TRUNCATE TABLE sales_clean;

INSERT INTO sales_clean (
    store, dept, sale_date, weekly_sales, is_holiday
)
SELECT
    CAST(store AS SIGNED),
    CAST(dept AS SIGNED),
    STR_TO_DATE(sale_date_raw, '%Y-%m-%d'),
    weekly_sales,
    CASE 
        WHEN is_holiday IN ('True', '1', 'true') THEN TRUE
        ELSE FALSE
    END
FROM sales_staging;


-- =====================================================================
-- 2. LOAD CLEAN FEATURES (From features_staging → features_clean)
-- =====================================================================

TRUNCATE TABLE features_clean;

INSERT INTO features_clean (
    store, feature_date, temperature, fuel_price,
    markdown1, markdown2, markdown3, markdown4, markdown5,
    cpi, unemployment, is_holiday
)
SELECT
    CAST(store AS SIGNED),
    STR_TO_DATE(feature_date_raw, '%Y-%m-%d'),
    temperature, fuel_price,
    markdown1, markdown2, markdown3, markdown4, markdown5,
    cpi, unemployment,
    CASE 
        WHEN is_holiday IN ('True', '1', 'true') THEN TRUE
        ELSE FALSE
    END
FROM features_staging;


-- =====================================================================
-- 3. LOAD STORE DIMENSION (From stores_staging → dim_store)
-- =====================================================================

TRUNCATE TABLE dim_store;

INSERT INTO dim_store (store, store_type, size)
SELECT
    CAST(store AS SIGNED),
    store_type,
    size
FROM stores_staging;


-- =====================================================================
-- 4. OPTIONAL: MERGE INTO FACT TABLE (Sales + Features)
-- =====================================================================

-- SQL equivalent of full_dataset_clean.csv
-- Used for analytics only — NOT loaded into a table.

-- SELECT
--     s.store,
--     s.dept,
--     s.sale_date,
--     s.weekly_sales,
--     s.is_holiday AS sale_is_holiday,
--     f.temperature,
--     f.fuel_price,
--     f.markdown1,
--     f.markdown2,
--     f.markdown3,
--     f.markdown4,
--     f.markdown5,
--     f.cpi,
--     f.unemployment,
--     f.is_holiday AS feature_is_holiday,
--     d.store_type,
--     d.size
-- FROM sales_clean s
-- LEFT JOIN features_clean f
--     ON s.store = f.store
--    AND s.sale_date = f.feature_date
-- LEFT JOIN dim_store d
--     ON s.store = d.store;

-- =====================================================================
-- END OF FILE
-- =====================================================================
