-- =========================================================
--  CREATE DATABASE
-- =========================================================
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- =========================================================
--  STAGING TABLES (RAW DATA)
-- =========================================================

-- 1) Sales staging (from train.csv + test.csv)
DROP TABLE IF EXISTS sales_staging;
CREATE TABLE sales_staging (
    store INT,
    dept INT,
    sale_date_raw VARCHAR(50),    -- Date as string
    weekly_sales DECIMAL(14,2),   -- NULL for test.csv values
    is_holiday VARCHAR(5)
);

-- 2) Features staging (from features.csv)
DROP TABLE IF EXISTS features_staging;
CREATE TABLE features_staging (
    store INT,
    feature_date_raw VARCHAR(50),   -- Date as string
    temperature DOUBLE,
    fuel_price DOUBLE,
    markdown1 DOUBLE,
    markdown2 DOUBLE,
    markdown3 DOUBLE,
    markdown4 DOUBLE,
    markdown5 DOUBLE,
    cpi DOUBLE,
    unemployment DOUBLE,
    is_holiday VARCHAR(5)
);

-- 3) Stores staging (from stores.csv)
DROP TABLE IF EXISTS stores_staging;
CREATE TABLE stores_staging (
    store INT,
    store_type VARCHAR(10),
    size INT
);

-- =========================================================
--  CLEAN / FINAL TABLES (TRANSFORMED DATA)
-- =========================================================

-- 4) Final sales fact table
DROP TABLE IF EXISTS sales_clean;
CREATE TABLE sales_clean (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store INT NOT NULL,
    dept INT NOT NULL,
    sale_date DATE NOT NULL,
    weekly_sales DECIMAL(14,2),
    is_holiday BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_store_dept_date (store, dept, sale_date)
);

-- 5) Final features table
DROP TABLE IF EXISTS features_clean;
CREATE TABLE features_clean (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store INT,
    feature_date DATE,
    temperature DOUBLE,
    fuel_price DOUBLE,
    markdown1 DOUBLE,
    markdown2 DOUBLE,
    markdown3 DOUBLE,
    markdown4 DOUBLE,
    markdown5 DOUBLE,
    cpi DOUBLE,
    unemployment DOUBLE,
    is_holiday BOOLEAN
);

-- 6) Store dimension table
DROP TABLE IF EXISTS dim_store;
CREATE TABLE dim_store (
    store INT PRIMARY KEY,
    store_type VARCHAR(10),
    size INT
);

-- =========================================================
--  INDEXES (PERFORMANCE)
-- =========================================================
CREATE INDEX idx_sales_store_date 
    ON sales_clean(store, sale_date);

CREATE INDEX idx_features_store_date 
    ON features_clean(store, feature_date);
    
    
show tables;
