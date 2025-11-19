import os
import shutil
from datetime import datetime

# Current folder = archive directory
ARCHIVE_DIR = os.path.dirname(os.path.abspath(__file__))

# Clean folder is 1 level up → clean/
CLEAN_DIR = os.path.join(os.path.dirname(ARCHIVE_DIR), "clean")


def archive_clean_files():
    print("==== ARCHIVE STARTED ====")

    # Timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Ensure clean folder exists
    if not os.path.exists(CLEAN_DIR):
        print(f"❌ Clean folder not found: {CLEAN_DIR}")
        return

    # Archive all CSVs from clean folder
    for file_name in os.listdir(CLEAN_DIR):
        if file_name.endswith(".csv"):
            src = os.path.join(CLEAN_DIR, file_name)
            dst = os.path.join(ARCHIVE_DIR, f"{timestamp}_{file_name}")

            shutil.copy2(src, dst)
            print(f"✔ Archived: {file_name} → {dst}")

    print("==== ARCHIVE COMPLETED ====")


if __name__ == "__main__":
    archive_clean_files()
