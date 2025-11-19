# scripts/extract.py

import pandas as pd
import json
from sqlalchemy import create_engine, text
import os
from datetime import datetime

# ========== PATHS ==========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl_log.txt")

# ========== LOGGER ==========
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

# ========== LOAD DB CONFIG ==========
def load_db_config():
    cfg_path = os.path.join(BASE_DIR, "config", "db_config.json")
    print("USING CONFIG FILE:", cfg_path)   # ← ADDED
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)
    print("CONFIG VALUES LOADED:", cfg)      # ← ADDED
    return cfg

# ========== GET SQL ENGINE ==========
def get_engine(cfg):
    uri = f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(uri, pool_recycle=3600)

# ========== SAFE CSV READER ==========
def read_csv_chunked(path, chunksize=100000):
    """Generator that yields chunks (DataFrame) from CSV."""
    try:
        for chunk in pd.read_csv(path, chunksize=chunksize):
            yield chunk
    except ValueError:
        # file too small for chunksize -> read at once
        yield pd.read_csv(path)
    except Exception as e:
        log(f"ERROR reading CSV {path}: {e}")
        yield from ()

# ========== TRUNCATE STAGING (idempotent run) ==========
def truncate_staging(engine):
    log("Truncating staging tables before load (idempotent run)...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE sales_staging;"))
        conn.execute(text("TRUNCATE TABLE features_staging;"))
        conn.execute(text("TRUNCATE TABLE stores_staging;"))
    log("Staging tables truncated.")

# ========== NORMALIZE COLUMN NAMES ==========
def normalize_columns(df):
    # remove BOM, whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]
    return df

# ========== MAIN EXTRACT FUNCTION ==========
def main():
    log("==== EXTRACT STEP STARTED ====")
    print("Using DB Config from:", os.path.join(BASE_DIR, "config", "db_config.json"))
    cfg = load_db_config()
    engine = get_engine(cfg)

    # CSV paths
    train_csv = os.path.join(RAW_DIR, "train.csv")
    test_csv = os.path.join(RAW_DIR, "test.csv")
    features_csv = os.path.join(RAW_DIR, "features.csv")
    stores_csv = os.path.join(RAW_DIR, "stores.csv")

    # Ensure files exist
    for p in [train_csv, features_csv, stores_csv, test_csv]:
        if not os.path.exists(p):
            log(f"WARNING: expected file missing: {p}")

    try:
        # 0) Truncate staging to avoid duplicates on repeated runs
        truncate_staging(engine)

        # 1) Load train.csv and test.csv into sales_staging (chunked)
        total_train = 0
        if os.path.exists(train_csv):
            for chunk in read_csv_chunked(train_csv):
                chunk = normalize_columns(chunk)
                chunk = chunk.rename(columns={
                    "Store": "store",
                    "Dept": "dept",
                    "Date": "sale_date_raw",
                    "Weekly_Sales": "weekly_sales",
                    "IsHoliday": "is_holiday"
                })
                chunk.to_sql("sales_staging", engine, if_exists="append", index=False, method="multi")
                total_train += len(chunk)
            log(f"Loaded train.csv -> sales_staging ({total_train} rows)")

        total_test = 0
        if os.path.exists(test_csv):
            for chunk in read_csv_chunked(test_csv):
                chunk = normalize_columns(chunk)
                chunk = chunk.rename(columns={
                    "Store": "store",
                    "Dept": "dept",
                    "Date": "sale_date_raw",
                    "IsHoliday": "is_holiday"
                })
                # test has no weekly_sales -> set None
                if "weekly_sales" not in chunk.columns:
                    chunk["weekly_sales"] = None
                chunk.to_sql("sales_staging", engine, if_exists="append", index=False, method="multi")
                total_test += len(chunk)
            log(f"Loaded test.csv -> sales_staging ({total_test} rows)")

        # 2) Load features.csv
        total_features = 0
        if os.path.exists(features_csv):
            for chunk in read_csv_chunked(features_csv):
                chunk = normalize_columns(chunk)
                chunk = chunk.rename(columns={
                    "Store": "store",
                    "Date": "feature_date_raw",
                    "Temperature": "temperature",
                    "Fuel_Price": "fuel_price",
                    "MarkDown1": "markdown1",
                    "MarkDown2": "markdown2",
                    "MarkDown3": "markdown3",
                    "MarkDown4": "markdown4",
                    "MarkDown5": "markdown5",
                    "CPI": "cpi",
                    "Unemployment": "unemployment",
                    "IsHoliday": "is_holiday"
                })
                chunk.to_sql("features_staging", engine, if_exists="append", index=False, method="multi")
                total_features += len(chunk)
            log(f"Loaded features.csv -> features_staging ({total_features} rows)")

        # 3) Load stores.csv (small, read at once)
        if os.path.exists(stores_csv):
            stores_df = pd.read_csv(stores_csv)
            stores_df = normalize_columns(stores_df)
            stores_df = stores_df.rename(columns={
                "Store": "store",
                "Type": "store_type",
                "Size": "size"
            })
            stores_df.to_sql("stores_staging", engine, if_exists="append", index=False, method="multi")
            log(f"Loaded stores.csv -> stores_staging ({len(stores_df)} rows)")

    except Exception as e:
        log(f"DB Load Error: {e}")
        raise

    finally:
        engine.dispose()

    log("==== EXTRACT STEP COMPLETED ====")

if __name__ == "__main__":
    main()
