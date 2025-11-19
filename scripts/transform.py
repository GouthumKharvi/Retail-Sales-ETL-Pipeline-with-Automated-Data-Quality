# scripts/transform.py

import pandas as pd
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

# ---------- SIMPLE LOGGER (ASCII ONLY) ----------
def log(msg):
    print(msg)

# ---------- LOAD CLEAN FUNCTION ----------
def load_csv(name):
    path = os.path.join(RAW_DIR, name)
    log(f"Loading {name}")
    return pd.read_csv(path)

def save_clean(df, filename):
    out_path = os.path.join(CLEAN_DIR, filename)
    df.to_csv(out_path, index=False, encoding="utf-8")
    log(f"Saved clean file: {out_path} ({len(df)} rows)")

# ---------- CLEANING HELPERS ----------
def normalize(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

# ---------- BUILD FULL DATASET ----------
def build_full_dataset(sales, features, stores):
    # Merge features on store + date
    merged = sales.merge(
        features,
        left_on=["store", "sale_date"],
        right_on=["store", "feature_date"],
        how="left"
    )

    # Merge stores
    merged = merged.merge(stores, on="store", how="left")
    return merged


# ================= MAIN =================
def main():
    log("==== TRANSFORM STEP STARTED ====")

    # ---------- Load CSVs ----------
    sales = load_csv("train.csv")
    features = load_csv("features.csv")
    stores = load_csv("stores.csv")

    # ---------- Normalize columns ----------
    sales = normalize(sales)
    features = normalize(features)
    stores = normalize(stores)

    # ---------- Rename columns ----------
    sales = sales.rename(columns={
        "store": "store",
        "dept": "dept",
        "date": "sale_date",
        "weekly_sales": "weekly_sales",
        "isholiday": "is_holiday"
    })

    features = features.rename(columns={
        "store": "store",
        "date": "feature_date",
        "temperature": "temperature",
        "fuel_price": "fuel_price",
        "markdown1": "markdown1",
        "markdown2": "markdown2",
        "markdown3": "markdown3",
        "markdown4": "markdown4",
        "markdown5": "markdown5",
        "cpi": "cpi",
        "unemployment": "unemployment",
        "isholiday": "is_holiday"
    })

    stores = stores.rename(columns={
        "store": "store",
        "type": "store_type",
        "size": "size"
    })

    # ---------- Save individual clean files ----------
    save_clean(sales, "sales_clean.csv")
    save_clean(features, "features_clean.csv")
    save_clean(stores, "stores_clean.csv")

    # ---------- Build full dataset ----------
    full = build_full_dataset(sales, features, stores)

    save_clean(full, "full_dataset_clean.csv")

    log("==== TRANSFORM STEP COMPLETED ====")


if __name__ == "__main__":
    main()
