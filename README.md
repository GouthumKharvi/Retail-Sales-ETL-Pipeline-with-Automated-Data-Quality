# Retail-Sales-ETL-Pipeline-with-Automated-Data-Quality
End-to-end ETL pipeline for retail sales forecasting and analytics. It ingests raw CSV data (train, test, features, stores), performs cleaning and transformations, loads the processed data into a MySQL Data Warehouse, and provides a fully interactive Streamlit-based analytics dashboard for monitoring, data exploration, and visualization.



✔ Project Overview
✔ Architecture
✔ ERD
✔ Folder Structure
✔ ETL Steps
✔ Streamlit App
✔ Big Data Integration
✔ SQL Design
✔ Screenshots placeholders
✔ How to deploy
✔ How to run ETL
✔ How to run Streamlit
✔ Skills demonstrated
✔ Resume-friendly summary



---

# 📊 Retail Sales ETL Pipeline with Automated Data Quality & Streamlit Dashboard

*A complete end-to-end Data Engineering project with ETL, SQL Data Warehouse, Automated Data Quality, Big Data (Hadoop/Spark/Kafka) integrations, and a Production-grade Streamlit Analytics Platform.*

---

## 🚀 Project Summary 

**“Retail Sales ETL & Analytics”**

---

## 🧠 About the Project

This project is a **real-world enterprise-level ETL and analytics system** built for processing large-scale **Retail Sales Data**. It includes:

* Full **ETL pipeline** (Extract → Transform → Load)
* Automated **data quality validation**
* **SQL database schema + data warehouse design**
* **Streamlit web dashboard** with advanced analytics
* **Big Data integrations** (Hadoop, Spark, Kafka)
* **GitHub LFS** for large CSV file management
* **Clean folder structure** following professional data engineering standards



**Python · ETL · Data Engineering · SQL · Database Design · Streamlit · Big Data (Hadoop/Spark/Kafka) · Automation · Data Quality**

---

# 📁 Full Project Structure

```
retail_sales_etl_pipeline/
│
├── config/
│   ├── db_config.json
│   └── etl_config.json
│
├── data/
│   ├── raw/
│   │   ├── train.csv
│   │   ├── test.csv
│   │   ├── features.csv
│   │   └── stores.csv
│   ├── staging/
│   ├── clean/
│   │   ├── sales_clean.csv
│   │   ├── features_clean.csv
│   │   ├── stores_clean.csv
│   │   └── full_dataset_clean.csv
│   └── archive/
│
├── docs/
│   ├── architecture_diagram.png
│   ├── entity_relationship.png
│   ├── interview_summary.md
│   └── README.md
│
├── logs/
│   ├── etl_log.txt
│   ├── errors.log
│   └── etl_pipeline_log.txt
│
├── notebooks/
│   ├── exploratory_analysis.ipynb
│   └── validation.ipynb
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── etl_pipeline.py
│   ├── scheduler.bat
│   └── scheduler.sh
│
├── sql/
│   ├── create_tables.sql
│   ├── staging_insert.sql
│   ├── transformations.sql
│   ├── load_clean.sql
│   └── validation_queries.sql
│
├── streamlit/
│   └── streamlit_app.py
│
└── README.md
```

---

# 🏗️ System Architecture

Below is the project’s high-level architecture:

```
                ┌────────────────────┐
                │ Raw CSV Files      │
                │ train/test/features│
                └─────────┬──────────┘
                          │
                 (Extract.py - Pandas)
                          │
                          ▼
         ┌────────────────────────────────┐
         │       Data Cleaning             │
         │ transform.py → NA handling     │
         │ normalization → merging        │
         └────────────────────────────────┘
                          │
                (Load.py - MySQL Load)
                          │
                          ▼
              ┌──────────────────────┐
              │   MySQL Database     │
              │ Fact + Dimension     │
              └─────────┬────────────┘
                        │
               (SQL + Views + Joins)
                        │
                        ▼
          ┌─────────────────────────────────┐
          │ Streamlit Analytics Dashboard   │
          │ KPI Metrics, Analysis, Insights │
          └─────────────────────────────────┘

          ┌───────────────────────────────────┐
          │      Big Data Integration         │
          │  Hadoop · Spark · Kafka (real-time)│
          └───────────────────────────────────┘
```

---

# 🗂️ Database ER Diagram

```
┌─────────────────────┐         ┌──────────────────────┐
│     STORES          │         │     DEPARTMENTS      │
├─────────────────────┤         ├──────────────────────┤
│ PK store_id         │────┐    │ PK department_id     │
│ store_type          │    │    │ department_name      │
│ size                │    │    │ category             │
└─────────────────────┘    │    └──────────────────────┘
          │                │
          ▼                ▼
┌──────────────────────────────┐
│      SALES_TRANSACTIONS      │
├──────────────────────────────┤
│ PK transaction_id            │
│ FK store_id                  │
│ FK department_id             │
│ FK date_id                   │
│ weekly_sales                 │
│ is_holiday                   │
└──────────────────────────────┘
          │
          ▼
┌──────────────────────────┐
│      DATE_DIMENSION      │
└──────────────────────────┘

┌─────────────────────────────┐         ┌──────────────────────────┐
│    ECONOMIC_INDICATORS      │         │      SALES_FEATURES      │
├─────────────────────────────┤         ├──────────────────────────┤
│ FK store_id                 │◄────────│ FK store_id              │
│ FK date_id                  │         │ FK date_id               │
└─────────────────────────────┘         └──────────────────────────┘
```

---

# 🛠️ 1. Extract Step

`extract.py` loads raw CSVs from `/data/raw/`.

### ✔ Features:

* Validates files
* Logs missing or corrupted files
* Loads using Pandas with dtype handling
* Saves intermediate outputs

---

# 🧹 2. Transform Step

`transform.py` performs:

### ✔ Cleaning

* Missing value handling
* Date normalization
* Type casting
* Removing duplicates

### ✔ Merging

* Join Sales + Features + Stores into `full_dataset_clean.csv`

### ✔ Validation

* Null checks
* Summary logging
* Ensuring primary keys

---

# 💾 3. Load Step

`load.py` loads cleaned datasets into the SQL database.

### ✔ Features

* Loads staging → final tables
* Uses batch inserts
* Auto-creates tables if missing
* Runs SQL scripts:

  * `create_tables.sql`
  * `staging_insert.sql`
  * `transformations.sql`
  * `load_clean.sql`
  * `validation_queries.sql`

---

# 🔄 4. Orchestrated ETL Pipeline

`etl_pipeline.py` automates:

```
Extract → Transform → Load
```

### ✔ Included

* Central logging
* Error logging
* Time tracking
* Console output
* Streamlit integration

### ✔ Windows/Linux Schedulers

* `scheduler.bat`
* `scheduler.sh`

---

# 🎨 Full Streamlit Web App

File: `/streamlit/streamlit_app.py`

This is the **most advanced part** of your project—built with
**animations, cards, metrics, analytics, SQL explorer, uploads, logs, AI-style insights, Big Data UI, etc.**

### ✔ Features:

#### ⭐ Executive Dashboard

* KPI cards
* Revenue analytics
* Data quality metrics
* Sampling
* Downloads

#### 📈 Advanced Analytics

* Time-series
* Seasonal trends
* Store comparison
* Correlation heatmaps

#### 🎯 Data Quality

* Missing data
* Duplicate detection
* Scored quality ratings

#### 🗄 Database Explorer

* Table list
* Auto SQL query runner
* Row previews
* Table metrics
* Downloadable extracts

#### 📤 Upload Center

* Upload raw data
* Preview before saving
* Metadata cards
* Auto-save to `/data/raw/`

#### 📋 Logs Viewer

* Auto-refresh
* Error/Warning filtering
* Line counts

#### 🔍 AI Style Insights

* Top stores
* Trend analysis
* Recommendations
* PDF-ready report

#### 🔥 Big Data Tech UI

* Hadoop
* Spark
* Kafka
* Architecture
* Sample ETL code
* Cluster metrics
* Visual explanations
* Enterprise-ready diagrams

#### 🗂 SQL Database Design

* ERD
* Schema
* Relationships
* Sample SQL queries

---

# 🐘 Big Data Integrations (Conceptual + Code)

### ✔ Hadoop HDFS

* Upload raw files
* Store processed data
* Commands included

### ✔ PySpark ETL

* Parallel transformations
* Joins
* Aggregations
* Writes back to HDFS

### ✔ Kafka Streaming

* Producer → topic → Spark consumer
* Real-time transformations
* Event-driven pipeline

---

# 📌 How to Run the Project

## 1️⃣ Clone Repo

```
git clone https://github.com/GouthumKharvi/Retail-Sales-ETL-Pipeline-with-Automated-Data-Quality.git
cd Retail-Sales-ETL-Pipeline-with-Automated-Data-Quality
```

---

## 2️⃣ Install Requirements

```
pip install -r requirements.txt
```

---

## 3️⃣ Run ETL

```
cd scripts
python etl_pipeline.py
```

---

## 4️⃣ Run Streamlit App

```
cd streamlit
streamlit run streamlit_app.py
```

---

# 🧪 Jupyter Notebooks Included

### `/notebooks/exploratory_analysis.ipynb`

* Raw data EDA
* Distribution analysis
* Trends

### `/notebooks/validation.ipynb`

* Schema verification
* Missing values
* Uniqueness
* Statistical checks

---

# 🧾 Automated Data Quality

The project computes:

| Metric        | Description              |
| ------------- | ------------------------ |
| Completeness  | Missing values %         |
| Uniqueness    | Duplicate detection      |
| Validity      | Type and range checks    |
| Freshness     | Latest date in data      |
| Quality Score | Weighted composite score |

---

# 📦 Git LFS Support

Large files (CSV > 100MB) stored with:

```
git lfs install
git lfs track "*.csv"
```

---

# 🧰 SQL Folder Includes

| File                     | Purpose                        |
| ------------------------ | ------------------------------ |
| `create_tables.sql`      | DDL for all tables             |
| `staging_insert.sql`     | Loads staging tables           |
| `transformations.sql`    | SQL-based cleaning             |
| `load_clean.sql`         | Inserts into final fact tables |
| `validation_queries.sql` | Row/NULL/Type checks           |

---

# 🧠 Skills Demonstrated

### ✔ Data Engineering

* ETL pipelines
* SQL DWH modeling
* Batch processing

### ✔ Python

* Pandas
* Automation
* Logging

### ✔ Streamlit

* Complex front-end
* Animated UI
* Advanced charts

### ✔ Big Data

* Hadoop
* Spark
* Kafka

### ✔ DevOps

* Schedulers
* JSON config mgmt
* Git LFS
* Modular folder structure

---


# 📎 Author

**Goutham Kharvi**
Retail Sales ETL Pipeline • 2025


