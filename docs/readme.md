
# 📘 **README.md — Retail Sales ETL Pipeline Project**

## 🏪 Retail Sales ETL Pipeline

End-to-end ETL pipeline designed to process Walmart-style retail sales data, transform it into clean analytical datasets, and load it into a MySQL database for reporting, dashboards, and machine-learning workflows.

---

# 📂 **Project Structure**

```
retail_sales_etl_pipeline/
├── data/
│   ├── raw/                 # Original Kaggle CSVs
│   ├── staging/             # Optional staging files
│   ├── clean/               # Final cleaned outputs
│   └── archive/             # Backup storage
│
├── scripts/
│   ├── extract.py           # Load raw CSV → staging tables
│   ├── transform.py         # Clean/merge/feature engineering
│   ├── load.py              # Load clean data into MySQL
│   └── etl_pipeline.py      # Combined Extract → Transform → Load
│
├── sql/
│   ├── create_tables.sql    # DDL for staging & clean tables
│   ├── staging_insert.sql   # Insert raw data into staging
│   ├── transformations.sql  # SQL cleaning logic (optional)
│   ├── load_clean.sql       # Load cleaned dataset into final tables
│   └── validation_queries.sql # Data quality checks
│
├── logs/
│   ├── etl_log.txt          # Extract & transform logs
│   ├── errors.log           # Error tracking
│   └── etl_pipeline_log.txt # Pipeline-level logs
│
├── config/
│   ├── db_config.json       # DB credentials
│   └── etl_config.json      # Paths & rules
│
├── docs/
│   ├── architecture_diagram.png   # ETL architecture
│   ├── entity_relationship.png    # ER diagram
│   ├── README.md                  # Extended documentation
│   └── interview_summary.md       # How to explain in interview
│
└── README.md
```

---

# 🎯 **Project Overview**

### ✔ **Objective**

Build a complete ETL pipeline to:

* Collect raw retail sales, store data, and feature data
* Clean, validate, merge, and enrich the dataset
* Load it into a MySQL database
* Prepare it for analytics, dashboards, and machine-learning models

### ✔ **Datasets Used**

From Kaggle Walmart Retail Dataset:

* `train.csv`
* `test.csv`
* `features.csv`
* `stores.csv`

---

# 🔧 **Technologies Used**

| Layer       | Tools                       |
| ----------- | --------------------------- |
| Programming | Python (pandas, SQLAlchemy) |
| Database    | MySQL 8                     |
| Logging     | Python logging + log files  |
| Versioning  | Git / GitHub                |
| Optional    | Jupyter Notebooks for EDA   |

---

# 🚀 **Pipeline Workflow**

## **1️⃣ Extract (extract.py)**

* Reads raw CSV files
* Normalizes column names
* Loads data into MySQL staging tables:

  * `sales_staging`
  * `features_staging`
  * `stores_staging`

## **2️⃣ Transform (transform.py)**

* Converts datatypes
* Handles missing values
* Standardizes date formats
* Merges:

  * sales + features
  * * store dimension
* Outputs cleaned files:

```
data/clean/sales_clean.csv
data/clean/features_clean.csv
data/clean/stores_clean.csv
data/clean/full_dataset_clean.csv
```

## **3️⃣ Load (load.py)**

Loads cleaned datasets into MySQL final tables:

* `sales_clean`
* `features_clean`
* `dim_store`
* (optional) `fact_sales`
* (optional) `full_dataset_clean`

## **4️⃣ Combined Script (etl_pipeline.py)**

Runs:

```
extract → transform → load
```

in sequence with logging.

---

# 🗄 **Database Schema**

### Clean Tables

* **sales_clean**
* **features_clean**
* **dim_store**

### Relationships

* `sales_clean.store` → `dim_store.store`
* `features_clean.store` → `dim_store.store`
* `sales_clean.sale_date` ↔ `features_clean.feature_date`

(See `docs/entity_relationship.png`)

---

# ✔ **How to Run the ETL Pipeline**

### Step 1 — Activate environment

```cmd
conda activate base
```

### Step 2 — Go to scripts folder

```cmd
cd retail_sales_etl_pipeline/scripts
```

### Step 3 — Run pipeline

```cmd
python etl_pipeline.py
```

### Successful Output

```
ETL PIPELINE COMPLETED SUCCESSFULLY
```

---

# ✓ **Data Validation**

Validation scripts include:

* Row count checks
* NULL value checks
* Date mismatches
* Orphan stores
  (run via SQL or scripts)

---

# 📊 **Notebooks**

Two Jupyter notebooks perform:

* **exploratory_analysis.ipynb** → raw data EDA
* **validation.ipynb** → final data validation

---

# 🧑‍💼 **Interview Summary**

A short, clear explanation of your entire project is inside:

```
docs/interview_summary.md
```

---

# ⭐ **Project Status**

✔ ETL complete
✔ SQL complete
✔ Clean data generated
✔ Logs working
✔ ER diagram + architecture diagram
⬜ Notebooks (optional)
⬜ Cloud deployment (optional)
⬜ Dashboard (Power BI / Tableau)

---

# 🏁 **Conclusion**

This project delivers a **full production-grade ETL system** with:

* automated ingestion
* robust transformations
* reliable loading
* clean database design
* reusable scripts
* and interview-ready documentation

