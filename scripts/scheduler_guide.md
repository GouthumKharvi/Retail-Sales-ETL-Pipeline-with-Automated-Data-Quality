# ETL Pipeline Scheduler Guide (.bat / .sh)

This document explains how to run and schedule the ETL pipeline using:

- `scheduler.bat` (Windows)
- `scheduler.sh` (Linux/macOS)

---

# 1. Running ETL on Windows (.bat)

## A. Run Manually
Open CMD and run:

```cmd
cd C:\Users\Gouthum\Downloads\retail_sales_etl_pipeline\scripts
scheduler.bat
```

OR double-click the `.bat` file.

### Output Example
```
Running Retail ETL Pipeline...
extract.py completed
transform.py completed
load.py completed
ETL Pipeline Completed Successfully
```

---

## B. Schedule with Windows Task Scheduler

1. Open **Task Scheduler**
2. Click **Create Basic Task**
3. Enter name: *Retail ETL Daily*
4. Trigger: **Daily**
5. Time: Choose your time
6. Action: **Start a Program**
7. Program: Select `scheduler.bat`
8. Finish

The ETL will now run automatically.

---

# 2. Running ETL on Linux/Mac (.sh)

## A. Make the file executable

```sh
chmod +x scheduler.sh
```

## B. Run manually

```sh
./scheduler.sh
```

---

# 3. Schedule Daily with CRON (Linux/macOS)

Open crontab:

```sh
crontab -e
```

Add job:

```
0 2 * * * /path/to/scheduler.sh >> /path/to/logs/scheduler.log 2>&1
```

This runs the ETL every day at 2 AM.

---

# 4. Common Issues

### Issue: conda not activated
Add inside `.sh`:

```sh
source ~/anaconda3/etc/profile.d/conda.sh
conda activate base
```

### Issue: Permission denied
```sh
chmod +x scheduler.sh
```

### Issue: Windows batch closes instantly
Add at bottom of `.bat`:

```bat
pause
```

---

# Done!
Your ETL pipeline can now run:
✔ manually  
✔ automatically on schedule  
✔ on both Windows & Linux

