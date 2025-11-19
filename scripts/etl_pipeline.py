# scripts/etl_pipeline.py

import os
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl_pipeline_log.txt")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

def run_script(script_name):
    """Runs extract.py, transform.py, load.py sequentially"""
    log(f"---- Running {script_name} ----")
    script_path = os.path.join(BASE_DIR, "scripts", script_name)

    if not os.path.exists(script_path):
        log(f"ERROR: Script not found → {script_path}")
        return False

    result = subprocess.run(["python", script_path], capture_output=True, text=True)

    if result.returncode != 0:
        log(f"ERROR in {script_name}: {result.stderr}")
        return False

    log(f"{script_name} completed successfully.")
    return True

def main():
    log("===== ETL PIPELINE STARTED =====")

    steps = ["extract.py", "transform.py", "load.py"]

    for step in steps:
        success = run_script(step)
        if not success:
            log("PIPELINE FAILED. STOPPING.")
            return

    log("===== ETL PIPELINE COMPLETED SUCCESSFULLY =====")

if __name__ == "__main__":
    main()
