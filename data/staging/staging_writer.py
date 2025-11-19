import os
import pandas as pd

# === FIXED PATHS ===
# Get the BASE project directory correctly even from /data/staging/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # .../data/staging
BASE_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))  # .../retail_sales_etl_pipeline

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")

os.makedirs(STAGING_DIR, exist_ok=True)

def clean_basic(df):
    df.columns = [c.strip() for c in df.columns]
    return df

def process_file(filename):
    raw_path = os.path.join(RAW_DIR, filename)
    stage_path = os.path.join(STAGING_DIR, filename)

    if not os.path.exists(raw_path):
        print(f"⚠ Missing file: {raw_path}")
        return

    print(f"Loading {filename}...")
    df = pd.read_csv(raw_path)

    df = clean_basic(df)

    df.to_csv(stage_path, index=False)
    print(f"✔ Saved staging file → {stage_path} ({len(df)} rows)")

def main():
    print("==== STAGING WRITER STARTED ====")

    process_file("train.csv")
    process_file("test.csv")
    process_file("features.csv")
    process_file("stores.csv")

    print("==== STAGING WRITER COMPLETED ====")

if __name__ == "__main__":
    main()
