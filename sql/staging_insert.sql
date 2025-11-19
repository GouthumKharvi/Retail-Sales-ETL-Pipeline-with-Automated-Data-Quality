-- ==========================================================
-- 1) LOAD RAW → STAGING (ONLY IF USING SQL INSTEAD OF PYTHON)
-- ==========================================================

-- SALES (train.csv + test.csv)
INSERT INTO sales_staging (store, dept, sale_date_raw, weekly_sales, is_holiday)
SELECT store, dept, sale_date_raw, weekly_sales, is_holiday
FROM sales_staging;

-- FEATURES (features.csv)
INSERT INTO features_staging (
    store, feature_date_raw, temperature, fuel_price,
    markdown1, markdown2, markdown3, markdown4, markdown5,
    cpi, unemployment, is_holiday
)
SELECT store, feature_date_raw, temperature, fuel_price,
       markdown1, markdown2, markdown3, markdown4, markdown5,
       cpi, unemployment, is_holiday
FROM features_staging;

-- STORES (stores.csv)
INSERT INTO stores_staging (store, store_type, size)
SELECT store, store_type, size
FROM stores_staging;
