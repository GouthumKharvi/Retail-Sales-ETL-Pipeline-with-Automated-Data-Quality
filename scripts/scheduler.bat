@echo off
REM ==========================================
REM   RETAIL SALES ETL PIPELINE SCHEDULER
REM ==========================================

echo.
echo =============================
echo   Running Retail ETL Pipeline
echo =============================
echo.

REM ---- 1. Activate Anaconda environment ----
CALL "C:\ProgramData\anaconda3\Scripts\activate.bat"

REM ---- 2. Navigate to scripts directory ----
cd /d "C:\Users\Gouthum\Downloads\retail_sales_etl_pipeline\scripts"

REM ---- 3. Run the full ETL pipeline ----
python etl_pipeline.py

echo.
echo =====================================
echo   ETL Pipeline Finished Successfully
echo =====================================
echo.
pause
