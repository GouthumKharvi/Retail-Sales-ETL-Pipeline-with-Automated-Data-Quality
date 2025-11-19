#!/bin/bash
# ==========================================
#   RETAIL SALES ETL PIPELINE SCHEDULER (Linux/macOS)
# ==========================================

echo "==================================="
echo " Running Retail ETL Pipeline (SH) "
echo "==================================="

# ---- 1. Activate conda environment ----
# (Replace 'base' with your env name if needed)
source ~/anaconda3/bin/activate base

# ---- 2. Navigate to scripts directory ----
cd "/Users/gouthum/Downloads/retail_sales_etl_pipeline/scripts" || exit

# ---- 3. Run ETL pipeline ----
python etl_pipeline.py

echo "==================================="
echo " ETL Pipeline Completed Successfully"
echo "==================================="
