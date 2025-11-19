# scripts/load.py

import pandas as pd
import json
from sqlalchemy import create_engine, text
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl_log.txt")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

def load_db_config():
    cfg_path = os.path.join(BASE_DIR, "config", "db_config.json")
    with open(cfg_path, encoding="utf-8") as f:
        return json.load(f)

def get_engine(cfg):
    uri = f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(uri, pool_recycle=3600)

def main():
    log("==== LOAD STEP STARTED ====")

    cfg = load_db_config()
    engine = get_engine(cfg)

    try:
        with engine.begin() as conn:
            log("Dropping clean tables (idempotent run)...")
            conn.execute(text("DROP TABLE IF EXISTS sales_clean;"))
            conn.execute(text("DROP TABLE IF EXISTS features_clean;"))
            conn.execute(text("DROP TABLE IF EXISTS stores_clean;"))
            conn.execute(text("DROP TABLE IF EXISTS fact_sales;"))

        # Load CSVs
        sales_df = pd.read_csv(os.path.join(CLEAN_DIR, "sales_clean.csv"))
        features_df = pd.read_csv(os.path.join(CLEAN_DIR, "features_clean.csv"))
        stores_df = pd.read_csv(os.path.join(CLEAN_DIR, "stores_clean.csv"))
        full_df = pd.read_csv(os.path.join(CLEAN_DIR, "full_dataset_clean.csv"))

        sales_df.to_sql("sales_clean", engine, if_exists="replace", index=False)
        features_df.to_sql("features_clean", engine, if_exists="replace", index=False)
        stores_df.to_sql("stores_clean", engine, if_exists="replace", index=False)
        full_df.to_sql("fact_sales", engine, if_exists="replace", index=False)

        log(f"Loaded sales_clean ({len(sales_df)} rows)")
        log(f"Loaded features_clean ({len(features_df)} rows)")
        log(f"Loaded stores_clean ({len(stores_df)} rows)")
        log(f"Loaded fact_sales ({len(full_df)} rows)")

    except Exception as e:
        log(f"LOAD ERROR: {e}")
        raise

    log("==== LOAD STEP COMPLETED ====")

if __name__ == "__main__":
    main()
