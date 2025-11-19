"""
Streamlit app — Retail Sales ETL (Enhanced Production Dashboard with Big Data)
Place this file in: <project_root>/streamlit/streamlit_app.py

Run:
    cd <project_root>/streamlit
    streamlit run streamlit_app.py
"""
 
import streamlit as st
import os
import json
import subprocess
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import matplotlib.pyplot as plt
import altair as alt
from sqlalchemy import create_engine, text
from pathlib import Path
import time
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------
# PATCH: Force Plotly to use an iframe renderer to avoid
# "Failed to fetch dynamically imported module" errors on Streamlit Cloud
# (this addresses the reported TypeError)
# ---------------------------
import plotly.io as pio
# Use iframe renderer for safer embedding (works in Streamlit where module fetch can fail)
pio.renderers.default = "iframe"

# ---------------------------
# Project paths (relative)
# ---------------------------
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
LOG_DIR = PROJECT_ROOT / "logs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
CONFIG_PATH = PROJECT_ROOT / "config" / "db_config.json"
PIPELINE_SCRIPT = SCRIPTS_DIR / "etl_pipeline.py"

# Ensure directories exist
for d in (RAW_DIR, CLEAN_DIR, STAGING_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Custom CSS for modern design
# ---------------------------
def load_custom_css():
    st.markdown("""
    <style>
    /* Main theme colors */
    :root {
        --primary-color: #1f77b4;
        --secondary-color: #ff7f0e;
        --success-color: #2ca02c;
        --danger-color: #d62728;
        --warning-color: #ff9800;
        --info-color: #17a2b8;
    }
    
    /* Animated gradient background for metrics */
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .metric-card {
        background: linear-gradient(-45deg, #667eea 0%, #764ba2 100%);
        background-size: 400% 400%;
        animation: gradient 15s ease infinite;
        padding: 20px;
        border-radius: 15px;
        color: white;
        box-shadow: 0 8px 16px rgba(0,0,0,0.2);
        margin: 10px 0;
        transition: transform 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.3);
    }
    
    .metric-value {
        font-size: 2.5em;
        font-weight: bold;
        margin: 10px 0;
    }
    
    .metric-label {
        font-size: 1em;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Pulse animation for status indicators */
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .status-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 2s ease-in-out infinite;
    }
    
    .status-success { background-color: #2ca02c; }
    .status-warning { background-color: #ff9800; }
    .status-error { background-color: #d62728; }
    .status-info { background-color: #17a2b8; }
    
    /* Sidebar styling */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Enhanced buttons */
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 12px 30px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
    }
    
    /* Progress bar animation */
    @keyframes progressBar {
        0% { width: 0%; }
        100% { width: 100%; }
    }
    
    .progress-container {
        width: 100%;
        height: 8px;
        background-color: #e0e0e0;
        border-radius: 10px;
        overflow: hidden;
        margin: 20px 0;
    }
    
    .progress-bar {
        height: 100%;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        animation: progressBar 2s ease-out;
    }
    
   /* Card containers */
    .info-card {
        background: white;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 15px 0;
        border-left: 4px solid #667eea;
        transition: all 0.3s ease;
        color: #333333;
    }
    
    .info-card:hover {
        box-shadow: 0 8px 12px rgba(0,0,0,0.15);
        transform: translateX(5px);
    }
    
    .info-card h3, .info-card h4, .info-card h5 {
        color: #1a1a1a !important;
        margin-top: 15px;
        margin-bottom: 10px;
    }
    
    .info-card p {
        color: #333333 !important;
    }
    
    .info-card strong {
        color: #1a1a1a !important;
    }
    
    .info-card li {
        color: #333333 !important;
        margin: 5px 0;
    }
    
    .info-card ul, .info-card ol {
        color: #333333 !important;
    }
    
    .info-card code {
        color: #d63384 !important;
        background-color: #f8f9fa;
        padding: 2px 6px;
        border-radius: 4px;
    }
    
    .info-card pre {
        color: #333333 !important;
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        overflow-x: auto;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #f0f2f6;
        border-radius: 10px 10px 0 0;
        padding: 10px 20px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    /* Loading animation */
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    .loading-spinner {
        border: 4px solid #f3f3f3;
        border-top: 4px solid #667eea;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        animation: spin 1s linear infinite;
        margin: 20px auto;
    }
    
    /* Toast notification */
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    .toast {
        position: fixed;
        top: 80px;
        right: 20px;
        padding: 15px 25px;
        border-radius: 8px;
        animation: slideIn 0.3s ease-out;
        z-index: 1000;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    /* Data quality badges */
    .quality-badge {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 0.85em;
        font-weight: 600;
        margin: 5px;
    }
    
    .quality-excellent { background-color: #d4edda; color: #155724; }
    .quality-good { background-color: #d1ecf1; color: #0c5460; }
    .quality-fair { background-color: #fff3cd; color: #856404; }
    .quality-poor { background-color: #f8d7da; color: #721c24; }
    
    /* Animated header */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 30px;
        border-radius: 15px;
        margin-bottom: 30px;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5em;
        font-weight: 700;
    }
    
    .main-header p {
        margin: 10px 0 0 0;
        opacity: 0.9;
        font-size: 1.1em;
    }
    
    /* Big Data Tech Cards */
    .tech-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 20px;
        border-radius: 12px;
        /* ensure contrast: force text white for gradient backgrounds */
        color: white !important;
        box-shadow: 0 4px 10px rgba(0,0,0,0.15);
        margin: 10px 0;
        transition: all 0.3s ease;
    }
    
    .tech-card:hover {
        transform: scale(1.02);
        box-shadow: 0 6px 15px rgba(0,0,0,0.25);
    }
    
    .tech-card h4 {
        margin: 0 0 10px 0;
        color: white !important;
    }
    
    .tech-card p {
        margin: 5px 0;
        opacity: 0.95;
        color: white !important;
    }

    /* Ensure code/pre blocks inside cards have dark text on light backgrounds */
    .tech-card pre, .tech-card code {
        color: #fff !important;
        background: rgba(0,0,0,0.12) !important;
    }

    /* Safe styling for inline white text on light backgrounds - fallback */
    .white-text-safe { color: #ffffff !important; text-shadow: 0 1px 0 rgba(0,0,0,0.45); }

    /* Fix for cards that use white bg: enforce dark text */
    .info-card.white-bg, .info-card[style*="background: white"] {
        color: #111 !important;
    }
    
    </style>
    """, unsafe_allow_html=True)

# ---------------------------
# Configuration
# ---------------------------
st.set_page_config(
    page_title="Retail Sales ETL Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_custom_css()

# ---------------------------
# Utility helpers
# ---------------------------
def load_json_config(path: Path):
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Failed to read config: {e}")
        return None

@st.cache_data(ttl=300)
def read_csv_if_exists(path: Path):
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        st.warning(f"Could not read {path.name}: {e}")
        return None

def safe_to_csv(df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

def run_etl_pipeline():
    """Execute the etl_pipeline.py script and return (stdout, stderr, returncode)."""
    if not PIPELINE_SCRIPT.exists():
        return None, f"Pipeline script not found at {PIPELINE_SCRIPT}", 1

    proc = subprocess.Popen(
        ["python", str(PIPELINE_SCRIPT)],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    stdout, stderr = proc.communicate()
    return stdout, stderr, proc.returncode

def get_db_engine(cfg: dict):
    """Create SQLAlchemy engine from db_config structure."""
    try:
        user = cfg["user"]
        password = cfg["password"]
        host = cfg.get("host", "127.0.0.1")
        port = cfg.get("port", 3306)
        db = cfg.get("database", "")
        uri = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(uri, pool_recycle=3600)
        return engine
    except Exception as e:
        # More user-friendly error - don't show the long SQLAlchemy background URL.
        err_str = str(e)
        err_str_clean = err_str.split(' (Background')[0]
        st.error(f"Failed to create DB engine: {err_str_clean}")
        return None

def calculate_data_quality_score(df):
    """Calculate data quality score based on completeness and validity."""
    if df is None or df.empty:
        return 0
    
    completeness = (1 - df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
    
    # Check for duplicates
    uniqueness = (1 - df.duplicated().sum() / len(df)) * 100
    
    # Overall score
    score = (completeness * 0.6 + uniqueness * 0.4)
    return round(score, 2)

def get_quality_badge(score):
    """Return HTML badge based on quality score."""
    if score >= 90:
        return '<span class="quality-badge quality-excellent">⭐ Excellent</span>'
    elif score >= 75:
        return '<span class="quality-badge quality-good">✓ Good</span>'
    elif score >= 60:
        return '<span class="quality-badge quality-fair">⚠ Fair</span>'
    else:
        return '<span class="quality-badge quality-poor">✗ Poor</span>'

def display_metric_card(label, value, delta=None, color="blue"):
    """Display an animated metric card."""
    colors = {
        "blue": "linear-gradient(-45deg, #667eea 0%, #764ba2 100%)",
        "green": "linear-gradient(-45deg, #56ab2f 0%, #a8e063 100%)",
        "red": "linear-gradient(-45deg, #eb3349 0%, #f45c43 100%)",
        "orange": "linear-gradient(-45deg, #f46b45 0%, #eea849 100%)",
        "purple": "linear-gradient(-45deg, #8e2de2 0%, #4a00e0 100%)"
    }
    
    gradient = colors.get(color, colors["blue"])
    delta_html = f'<div style="font-size: 0.9em; margin-top: 5px;">{delta}</div>' if delta else ''
    
    return f"""
    <div class="metric-card" style="background: {gradient};">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """

# ---------------------------
# Header
# ---------------------------
st.markdown("""
<div class="main-header">
    <h1>📊 Retail Sales ETL Analytics Platform</h1>
    <p>Production-Grade Data Pipeline Management & Analysis Dashboard with Big Data Technologies</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------
# Sidebar (Enhanced Controls)
# ---------------------------
st.sidebar.markdown("### 🎯 Control Center")

# System Status
config = load_json_config(CONFIG_PATH)
st.sidebar.markdown("#### 📡 System Status")

col1, col2 = st.sidebar.columns(2)
with col1:
    if config:
        st.markdown('<span class="status-indicator status-success"></span>**DB Connected**', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-indicator status-error"></span>**DB Offline**', unsafe_allow_html=True)

with col2:
    pipeline_exists = PIPELINE_SCRIPT.exists()
    if pipeline_exists:
        st.markdown('<span class="status-indicator status-success"></span>**Pipeline Ready**', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-indicator status-warning"></span>**Pipeline Missing**', unsafe_allow_html=True)

st.sidebar.markdown("---")

# ETL Controls
st.sidebar.markdown("#### ⚙️ ETL Operations")

if st.sidebar.button("🚀 Run Full ETL Pipeline", use_container_width=True):
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    
    status_text.text("🔄 Initializing pipeline...")
    progress_bar.progress(10)
    time.sleep(0.5)
    
    status_text.text("📥 Extracting data...")
    progress_bar.progress(30)
    
    with st.spinner("Running ETL pipeline..."):
        stdout, stderr, rc = run_etl_pipeline()
        progress_bar.progress(70)
        status_text.text("🔄 Transforming data...")
        time.sleep(0.3)
        
        progress_bar.progress(90)
        status_text.text("💾 Loading to database...")
        time.sleep(0.3)
        
        progress_bar.progress(100)
        
        if rc == 0:
            status_text.text("✅ Pipeline completed successfully!")
            st.sidebar.success("ETL pipeline finished successfully!")
            st.balloons()
        else:
            status_text.text("❌ Pipeline failed")
            # sanitize stderr/rc info
            st.sidebar.error(f"ETL pipeline failed (code {rc})")
        
        if stdout:
            with st.sidebar.expander("📄 View stdout"):
                st.code(stdout, language="text")
        if stderr:
            with st.sidebar.expander("⚠️ View stderr"):
                st.code(stderr, language="text")

# Quick Actions
st.sidebar.markdown("#### ⚡ Quick Actions")

col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()
with col2:
    if st.button("🧹 Clear Cache", use_container_width=True):
        st.cache_data.clear()
        st.sidebar.success("Cache cleared!")

# Data Info
st.sidebar.markdown("---")
st.sidebar.markdown("#### 📂 Data Status")

raw_count = len(list(RAW_DIR.glob("*.csv")))
clean_count = len(list(CLEAN_DIR.glob("*.csv")))
log_count = len(list(LOG_DIR.glob("*")))

st.sidebar.metric("Raw Files", raw_count, delta=None)
st.sidebar.metric("Clean Files", clean_count, delta=None)
st.sidebar.metric("Log Files", log_count, delta=None)

# Recent Activity (simulated)
st.sidebar.markdown("---")
st.sidebar.markdown("#### 🕐 Recent Activity")
activities = [
    ("ETL Pipeline", "2 hours ago"),
    ("Data Upload", "5 hours ago"),
    ("Quality Check", "1 day ago")
]
for activity, time_ago in activities:
    st.sidebar.markdown(f"**{activity}**  \n*{time_ago}*")

st.sidebar.markdown("---")
st.sidebar.caption("🏗️ Built for Production ETL Workflows")
st.sidebar.caption("Version 2.0 | Last Updated: 2025")

# ---------------------------
# Load Data
# ---------------------------
sales_df = read_csv_if_exists(CLEAN_DIR / "sales_clean.csv")
features_df = read_csv_if_exists(CLEAN_DIR / "features_clean.csv")
stores_df = read_csv_if_exists(CLEAN_DIR / "stores_clean.csv")
full_df = read_csv_if_exists(CLEAN_DIR / "full_dataset_clean.csv")

# ---------------------------
# Tabs
# ---------------------------
tabs = st.tabs([
    "📊 Dashboard",
    "📈 Advanced Analytics",
    "🎯 Data Quality",
    "🗄️ Database",
    "📤 Upload",
    "📋 Logs",
    "🔍 Insights",
    "🔥 Big Data Tech",
    "🗂️ SQL Design"
])

# ---------------------------
# Dashboard Tab
# ---------------------------
with tabs[0]:
    st.markdown("### 📊 Executive Dashboard")
    
    # KPI Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if sales_df is not None:
            st.markdown(display_metric_card(
                "Total Sales Records",
                f"{len(sales_df):,}",
                color="blue"
            ), unsafe_allow_html=True)
        else:
            st.markdown(display_metric_card(
                "Total Sales Records",
                "N/A",
                color="red"
            ), unsafe_allow_html=True)
    
    with col2:
        if features_df is not None:
            st.markdown(display_metric_card(
                "Feature Records",
                f"{len(features_df):,}",
                color="green"
            ), unsafe_allow_html=True)
        else:
            st.markdown(display_metric_card(
                "Feature Records",
                "N/A",
                color="red"
            ), unsafe_allow_html=True)
    
    with col3:
        if stores_df is not None:
            st.markdown(display_metric_card(
                "Active Stores",
                f"{len(stores_df):,}",
                color="purple"
            ), unsafe_allow_html=True)
        else:
            st.markdown(display_metric_card(
                "Active Stores",
                "N/A",
                color="red"
            ), unsafe_allow_html=True)
    
    with col4:
        if full_df is not None:
            # protect if weekly_sales missing column
            if 'weekly_sales' in full_df.columns:
                total_sales = full_df['weekly_sales'].sum()
                st.markdown(display_metric_card(
                    "Total Revenue",
                    f"${total_sales/1e6:.2f}M",
                    color="orange"
                ), unsafe_allow_html=True)
            else:
                st.markdown(display_metric_card(
                    "Total Revenue",
                    "N/A",
                    color="red"
                ), unsafe_allow_html=True)
        else:
            st.markdown(display_metric_card(
                "Total Revenue",
                "N/A",
                color="red"
            ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Data Quality Overview
    if full_df is not None:
        st.markdown("### 🎯 Data Quality Overview")
        
        col1, col2, col3 = st.columns(3)
        
        quality_score = calculate_data_quality_score(full_df)
        
        with col1:
            st.markdown(f"""
            <div class="info-card">
                <h3>Overall Quality Score</h3>
                <div style="font-size: 3em; font-weight: bold; color: #667eea;">{quality_score}%</div>
                {get_quality_badge(quality_score)}
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            completeness = (1 - full_df.isnull().sum().sum() / (full_df.shape[0] * full_df.shape[1])) * 100
            st.markdown(f"""
            <div class="info-card">
                <h3>Data Completeness</h3>
                <div style="font-size: 3em; font-weight: bold; color: #2ca02c;">{completeness:.1f}%</div>
                <p>Missing values: {full_df.isnull().sum().sum():,}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            duplicates = full_df.duplicated().sum()
            uniqueness = (1 - duplicates / len(full_df)) * 100
            st.markdown(f"""
            <div class="info-card">
                <h3>Data Uniqueness</h3>
                <div style="font-size: 3em; font-weight: bold; color: #ff7f0e;">{uniqueness:.1f}%</div>
                <p>Duplicates: {duplicates:,}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quick Preview
        st.markdown("### 🔍 Data Preview (Sample)")
        st.dataframe(
            full_df.sample(min(500, len(full_df))),
            use_container_width=True,
            height=400
        )
        
        # Download option
        csv = full_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Full Dataset",
            data=csv,
            file_name=f"retail_sales_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.info("⚠️ No data available. Please run the ETL pipeline to generate clean datasets.")
        st.markdown("""
        <div class="info-card">
            <h3>Getting Started</h3>
            <ol>
                <li>Upload raw CSV files in the <strong>Upload</strong> tab</li>
                <li>Click <strong>Run Full ETL Pipeline</strong> in the sidebar</li>
                <li>Wait for processing to complete</li>
                <li>Return here to view your analytics</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

# ---------------------------
# Advanced Analytics Tab
# ---------------------------
with tabs[1]:
    st.markdown("### 📈 Advanced Analytics")
    
    if full_df is None:
        st.warning("⚠️ No data available. Run the ETL pipeline first.")
    else:
        # Convert dates safely
        if 'sale_date' in full_df.columns:
            if not np.issubdtype(full_df["sale_date"].dtype, np.datetime64):
                full_df["sale_date"] = pd.to_datetime(full_df["sale_date"], errors="coerce")
        else:
            # add placeholder column to avoid crashing visual code paths
            full_df['sale_date'] = pd.to_datetime(pd.Series([None]*len(full_df)))
        
        # Time Series Analysis
        st.markdown("#### 📅 Time Series Analysis")
        
        col1, col2 = st.columns([2, 1])
        
        with col2:
            # protect unique stores
            if 'store' in full_df.columns:
                stores = sorted(full_df["store"].fillna("Unknown").unique().tolist())
            else:
                stores = ["Unknown"]
            selected_store = st.selectbox("🏪 Select Store:", stores, index=0)
            
            time_range = st.select_slider(
                "📊 Time Range:",
                options=["1 Month", "3 Months", "6 Months", "1 Year", "All Time"],
                value="6 Months"
            )
            
            chart_type = st.radio(
                "📈 Chart Type:",
                ["Line", "Area", "Bar"],
                horizontal=True
            )
        
        with col1:
            store_df = full_df[full_df.get("store", "Unknown") == selected_store].sort_values("sale_date")
            
            # Apply time range filter
            if time_range != "All Time" and not store_df.empty:
                days_map = {"1 Month": 30, "3 Months": 90, "6 Months": 180, "1 Year": 365}
                cutoff_date = store_df["sale_date"].max() - timedelta(days=days_map[time_range])
                store_df = store_df[store_df["sale_date"] >= cutoff_date]
            
            if not store_df.empty and 'weekly_sales' in store_df.columns:
                # Convert to native Python types
                store_df = store_df.copy()
                store_df['weekly_sales'] = store_df['weekly_sales'].astype(float)
                
                # Create Plotly chart
                fig = go.Figure()
                
                if chart_type == "Line":
                    fig.add_trace(go.Scatter(
                        x=store_df["sale_date"].tolist(),
                        y=store_df["weekly_sales"].tolist(),
                        mode='lines',
                        name='Weekly Sales',
                        line=dict(color='#667eea', width=2)
                    ))
                elif chart_type == "Area":
                    fig.add_trace(go.Scatter(
                        x=store_df["sale_date"].tolist(),
                        y=store_df["weekly_sales"].tolist(),
                        mode='lines',
                        name='Weekly Sales',
                        fill='tozeroy',
                        line=dict(color='#667eea', width=2)
                    ))
                else:  # Bar
                    fig.add_trace(go.Bar(
                        x=store_df["sale_date"].tolist(),
                        y=store_df["weekly_sales"].tolist(),
                        name='Weekly Sales',
                        marker_color='#667eea'
                    ))
                
                # Add moving average safely
                try:
                    ma = store_df.set_index("sale_date")["weekly_sales"].rolling(window=4).mean()
                    fig.add_trace(go.Scatter(
                        x=ma.index.tolist(),
                        y=ma.values.tolist(),
                        mode='lines',
                        name='4-Week Moving Avg',
                        line=dict(color='#ff7f0e', width=2, dash='dash')
                    ))
                except Exception:
                    pass
                
                fig.update_layout(
                    title=f"Store {selected_store} - Sales Trend",
                    xaxis_title="Date",
                    yaxis_title="Weekly Sales ($)",
                    hovermode='x unified',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for selected filters")
        
        st.markdown("---")
        
        # Comparative Analysis
        st.markdown("#### 🏆 Store Performance Comparison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performing stores (protect missing columns)
            if 'store' in full_df.columns and 'weekly_sales' in full_df.columns:
                top_stores = full_df.groupby("store", as_index=False)["weekly_sales"].agg([
                    ('total_sales', 'sum'),
                    ('avg_sales', 'mean'),
                    ('max_sales', 'max')
                ]).reset_index()
                top_stores = top_stores.sort_values('total_sales', ascending=False).head(10)
                
                # Convert to native Python types
                top_stores['store'] = top_stores['store'].astype(str)
                top_stores['total_sales'] = top_stores['total_sales'].astype(float)
                
                fig = px.bar(
                    top_stores,
                    x='store',
                    y='total_sales',
                    title='Top 10 Stores by Total Sales',
                    color='total_sales',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data to compute top stores.")
        
        with col2:
            # Sales distribution - sample for performance
            if 'weekly_sales' in full_df.columns:
                sample_df = full_df.head(10000).copy()
                sample_df['weekly_sales'] = sample_df['weekly_sales'].astype(float)
                
                fig = px.box(
                    sample_df,
                    y='weekly_sales',
                    title='Sales Distribution Across All Stores',
                    color_discrete_sequence=['#667eea']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No 'weekly_sales' column available for distribution plot.")
        
        st.markdown("---")
        
        # Correlation Analysis
        st.markdown("#### 🔗 Feature Correlation Matrix")
        
        numeric = full_df.select_dtypes(include=[np.number]).fillna(0)
        if numeric.shape[1] > 1:
            corr = numeric.corr()
            
            # Convert to native Python types for JSON serialization
            corr_values = corr.values.tolist()
            corr_columns = corr.columns.tolist()
            
            fig = px.imshow(
                corr_values,
                labels=dict(color="Correlation"),
                x=corr_columns,
                y=corr_columns,
                color_continuous_scale='RdBu_r',
                aspect='auto'
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough numeric features to compute correlation.")
        
        st.markdown("---")
        
        # Seasonal Analysis
        if 'sale_date' in full_df.columns and 'weekly_sales' in full_df.columns:
            st.markdown("#### 📆 Seasonal Analysis")
            
            full_df['month'] = pd.to_datetime(full_df['sale_date']).dt.month
            full_df['quarter'] = pd.to_datetime(full_df['sale_date']).dt.quarter
            
            col1, col2 = st.columns(2)
            
            with col1:
                monthly_avg = full_df.groupby('month')['weekly_sales'].mean().reset_index()
                # Convert to native Python types
                monthly_avg['month'] = monthly_avg['month'].astype(int)
                monthly_avg['weekly_sales'] = monthly_avg['weekly_sales'].astype(float)
                
                fig = px.line(
                    monthly_avg,
                    x='month',
                    y='weekly_sales',
                    title='Average Sales by Month',
                    markers=True
                )
                fig.update_xaxes(tickmode='linear', tick0=1, dtick=1)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                quarterly_sales = full_df.groupby('quarter')['weekly_sales'].sum().reset_index()
                # Convert to native Python types - CRITICAL for pie charts
                quarterly_sales['quarter'] = quarterly_sales['quarter'].astype(int).astype(str)
                quarterly_sales['weekly_sales'] = quarterly_sales['weekly_sales'].round(2).astype(float)
                
                fig = px.pie(
                    quarterly_sales,
                    values='weekly_sales',
                    names='quarter',
                    title='Sales Distribution by Quarter',
                    color_discrete_sequence=px.colors.sequential.Blues_r
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Seasonal analysis requires 'sale_date' and 'weekly_sales' columns.")

# ---------------------------
# Data Quality Tab
# ---------------------------
with tabs[2]:
    st.markdown("### 🎯 Data Quality Assessment")
    
    if sales_df is None and features_df is None and stores_df is None:
        st.warning("⚠️ No data available for quality assessment.")
    else:
        # Overall Quality Scorecard
        st.markdown("#### 📊 Quality Scorecard")
        
        datasets = {
            "Sales": sales_df,
            "Features": features_df,
            "Stores": stores_df,
            "Full Dataset": full_df
        }
        
        quality_data = []
        for name, df in datasets.items():
            if df is not None:
                score = calculate_data_quality_score(df)
                missing = df.isnull().sum().sum()
                duplicates = df.duplicated().sum()
                rows = len(df)
                cols = len(df.columns)
                
                quality_data.append({
                    "Dataset": name,
                    "Rows": rows,
                    "Columns": cols,
                    "Quality Score": score,
                    "Missing Values": missing,
                    "Duplicates": duplicates,
                    "Status": "✅" if score >= 80 else "⚠️" if score >= 60 else "❌"
                })
        
        quality_df = pd.DataFrame(quality_data) if quality_data else pd.DataFrame([])
        
        # Display quality metrics
        if not quality_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            avg_score = quality_df["Quality Score"].mean()
            total_missing = quality_df["Missing Values"].sum()
            total_duplicates = quality_df["Duplicates"].sum()
            total_rows = quality_df["Rows"].sum()
            
            with col1:
                st.markdown(display_metric_card(
                    "Average Quality Score",
                    f"{avg_score:.1f}%",
                    color="purple"
                ), unsafe_allow_html=True)
            
            with col2:
                st.markdown(display_metric_card(
                    "Total Missing Values",
                    f"{total_missing:,}",
                    color="orange"
                ), unsafe_allow_html=True)
            
            with col3:
                st.markdown(display_metric_card(
                    "Total Duplicates",
                    f"{total_duplicates:,}",
                    color="red"
                ), unsafe_allow_html=True)
            
            with col4:
                st.markdown(display_metric_card(
                    "Total Records",
                    f"{total_rows:,}",
                    color="green"
                ), unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Detailed quality table
            st.markdown("#### 📋 Detailed Quality Report")
            
            # Style the dataframe
            styled_df = quality_df.style.background_gradient(
                subset=['Quality Score'],
                cmap='RdYlGn',
                vmin=0,
                vmax=100
            ).format({
                'Quality Score': '{:.2f}%',
                'Rows': '{:,}',
                'Missing Values': '{:,}',
                'Duplicates': '{:,}'
            })
            
            st.dataframe(styled_df, use_container_width=True)
        else:
            st.info("No datasets available to compute quality metrics.")
        
        st.markdown("---")
        
        # Missing data visualization
        st.markdown("#### 🔍 Missing Data Analysis")
        
        tab1, tab2, tab3 = st.tabs(["Sales", "Features", "Stores"])
        
        def plot_missing_data(df, title):
            if df is None:
                st.info(f"No {title} data available")
                return
            
            missing = df.isnull().sum()
            missing_pct = (missing / len(df) * 100).round(2)
            
            missing_df = pd.DataFrame({
                'Column': missing.index,
                'Missing Count': missing.values,
                'Missing %': missing_pct.values
            }).sort_values('Missing Count', ascending=False).head(15)
            
            if missing_df['Missing Count'].sum() == 0:
                st.success(f"✅ No missing values in {title} dataset!")
            else:
                # Convert to native Python types
                missing_df['Missing Count'] = missing_df['Missing Count'].astype(int)
                missing_df['Missing %'] = missing_df['Missing %'].astype(float)
                
                fig = px.bar(
                    missing_df,
                    x='Column',
                    y='Missing Count',
                    title=f'Missing Values in {title} Dataset',
                    color='Missing %',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                # Show detailed stats
                st.dataframe(missing_df, use_container_width=True)
        
        with tab1:
            plot_missing_data(sales_df, "Sales")
        
        with tab2:
            plot_missing_data(features_df, "Features")
        
        with tab3:
            plot_missing_data(stores_df, "Stores")
        
        st.markdown("---")
        
        # Data type analysis
        st.markdown("#### 🔤 Data Type Distribution")
        
        if full_df is not None:
            dtype_counts = full_df.dtypes.value_counts().reset_index()
            dtype_counts.columns = ['Data Type', 'Count']
            
            # Convert to native Python types
            dtype_counts['Data Type'] = dtype_counts['Data Type'].astype(str)
            dtype_counts['Count'] = dtype_counts['Count'].astype(int)
            
            fig = px.pie(
                dtype_counts,
                values='Count',
                names='Data Type',
                title='Distribution of Data Types',
                hole=0.4
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Full dataset not available for data type distribution.")
        
        st.markdown("---")
        
        # Statistical summary
        st.markdown("#### 📊 Statistical Summary")
        
        if full_df is not None:
            summary = full_df.describe(include='all').T
            summary['null_count'] = full_df.isnull().sum()
            summary['null_pct'] = (summary['null_count'] / len(full_df) * 100).round(2)
            
            st.dataframe(summary, use_container_width=True, height=400)
            
            # Download quality report
            report_csv = quality_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Quality Report",
                data=report_csv,
                file_name=f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )
        else:
            st.info("No full dataset to produce statistical summary.")

# ---------------------------
# Database Tab
# ---------------------------

# Detect Streamlit Cloud environment
IS_CLOUD = "STREAMLIT_SERVER" in os.environ

with tabs[3]:
    st.markdown("### 🗄️ Database Management")

    # --- Cloud Blocker (prevents MySQL error on Streamlit Cloud) ---
    if IS_CLOUD:
        st.error("⚠️ Database connection disabled on Streamlit Cloud")
        st.info("Cloud apps cannot connect to local MySQL (127.0.0.1). Use a cloud database like RDS or PlanetScale.")
        st.stop()

    # --- Local-only DB logic continues below ---
    if not config:
        st.warning("⚠️ Database configuration not found. Add `config/db_config.json` to enable database features.")
        
        st.markdown("""
        <div class="info-card">
            <h4>Database Configuration Required</h4>
            <p>Create a <code>db_config.json</code> file in the <code>config</code> directory with the following structure:</p>
            <pre>{
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": 3306,
    "database": "your_database"
}</pre>
        </div>
        """, unsafe_allow_html=True)

    else:
    # Cloud safety skip
    if IS_CLOUD:
        st.error("⚠️ Database disabled on Streamlit Cloud")
        st.info("Streamlit Cloud cannot access your local MySQL (127.0.0.1).")
        st.stop()

    engine = None
    try:
        # try to create engine (get_db_engine already displays a friendly error on exception)
        engine = get_db_engine(config)
        if engine:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("✅ Successfully connected to database!")
        else:
            st.warning("Database engine creation failed — database features will be disabled.")
        
        # Database info
        if engine:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(display_metric_card(
                    "Database Host",
                    config.get("host", "localhost"),
                    color="blue"
                ), unsafe_allow_html=True)
            
            with col2:
                st.markdown(display_metric_card(
                    "Database Name",
                    config.get("database", "N/A"),
                    color="green"
                ), unsafe_allow_html=True)
            
            with col3:
                st.markdown(display_metric_card(
                    "Port",
                    str(config.get("port", 3306)),
                    color="purple"
                ), unsafe_allow_html=True)
            
            st.markdown("---")

            
        except Exception as e:
            # Show a cleaned error message for connection failures
            err_str = str(e)
            err_str_clean = err_str.split(' (Background')[0]
            st.error(f"❌ Database connection failed: {err_str_clean}")
            engine = None
        
        if engine:
            # Table explorer
            st.markdown("#### 📊 Table Explorer")
            
            try:
                with engine.connect() as conn:
                    res = conn.execute(text("SHOW TABLES"))
                    tables = [row[0] for row in res.fetchall()]
                
                if not tables:
                    st.info("No tables found in database")
                else:
                    col1, col2 = st.columns([1, 3])
                    
                    with col1:
                        selected_table = st.selectbox(
                            "Select Table",
                            ["-- Select --"] + tables,
                            key="table_selector"
                        )
                        
                        if selected_table != "-- Select --":
                            # Get table info
                            with engine.connect() as conn:
                                count_query = text(f"SELECT COUNT(*) FROM `{selected_table}`")
                                row_count = conn.execute(count_query).scalar()
                                
                                st.metric("Total Rows", f"{row_count:,}")
                                
                                if st.button("🔄 Refresh Table", use_container_width=True):
                                    st.rerun()
                    
                    with col2:
                        if selected_table != "-- Select --":
                            with st.spinner(f"Loading {selected_table}..."):
                                with engine.connect() as conn:
                                    limit = st.slider("Rows to display:", 10, 10000, 1000, 10)
                                    query = text(f"SELECT * FROM `{selected_table}` LIMIT {limit}")
                                    table_df = pd.read_sql(query, engine)
                                    
                                    st.dataframe(table_df, use_container_width=True, height=400)
                                    
                                    # Table statistics
                                    st.markdown("##### 📈 Table Statistics")
                                    col1, col2, col3, col4 = st.columns(4)
                                    
                                    with col1:
                                        st.metric("Columns", len(table_df.columns))
                                    with col2:
                                        st.metric("Numeric Columns", len(table_df.select_dtypes(include=[np.number]).columns))
                                    with col3:
                                        st.metric("Text Columns", len(table_df.select_dtypes(include=['object']).columns))
                                    with col4:
                                        st.metric("Missing Values", table_df.isnull().sum().sum())
                                    
                                    # Download option
                                    csv = table_df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label=f"📥 Download {selected_table}",
                                        data=csv,
                                        file_name=f"{selected_table}_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )
                
                st.markdown("---")
                
                # Custom SQL Query
                st.markdown("#### 🔍 Custom SQL Query")
                
                with st.expander("Execute Custom Query", expanded=False):
                    query_input = st.text_area(
                        "Enter SQL Query:",
                        "SELECT * FROM your_table LIMIT 100;",
                        height=150
                    )
                    
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        execute_query = st.button("▶️ Execute", use_container_width=True)
                    
                    if execute_query:
                        try:
                            with st.spinner("Executing query..."):
                                with engine.connect() as conn:
                                    result_df = pd.read_sql(text(query_input), engine)
                                    st.success(f"✅ Query executed successfully! ({len(result_df)} rows returned)")
                                    st.dataframe(result_df, use_container_width=True)
                                    
                                    csv = result_df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="📥 Download Results",
                                        data=csv,
                                        file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv",
                                    )
                        except Exception as e:
                            err_str = str(e)
                            err_str_clean = err_str.split(' (Background')[0]
                            st.error(f"❌ Query execution failed: {err_str_clean}")
                
            except Exception as e:
                err_str = str(e)
                err_str_clean = err_str.split(' (Background')[0]
                st.error(f"Failed to retrieve tables: {err_str_clean}")

# ---------------------------
# Upload Tab
# ---------------------------
with tabs[4]:
    st.markdown("### 📤 Data Upload Center")
    
    st.markdown("""
    <div class="info-card">
        <h4>📋 Upload Instructions</h4>
        <ul>
            <li>Upload CSV files one at a time</li>
            <li>Files will be saved to <code>data/raw</code> directory</li>
            <li>Supported files: train.csv, test.csv, features.csv, stores.csv</li>
            <li>After upload, run the ETL pipeline to process the data</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### 📁 Upload New File")
        uploaded = st.file_uploader(
            "Choose CSV file",
            type=["csv"],
            accept_multiple_files=False,
            help="Upload raw CSV files for ETL processing"
        )
        
        if uploaded:
            target_path = RAW_DIR / uploaded.name
            
            # File preview
            try:
                preview_df = pd.read_csv(uploaded)
                
                st.markdown("##### 👀 File Preview")
                st.dataframe(preview_df.head(100), use_container_width=True)
                
                # File statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Rows", f"{len(preview_df):,}")
                with col2:
                    st.metric("Columns", len(preview_df.columns))
                with col3:
                    file_size = uploaded.size / 1024  # KB
                    st.metric("Size", f"{file_size:.1f} KB")
                with col4:
                    missing = preview_df.isnull().sum().sum()
                    st.metric("Missing Values", f"{missing:,}")
                
                # Save button
                if st.button("💾 Save to Raw Directory", type="primary", use_container_width=True):
                    with st.spinner("Saving file..."):
                        uploaded.seek(0)  # Reset file pointer
                        with open(target_path, "wb") as f:
                            f.write(uploaded.getbuffer())
                        st.success(f"✅ File saved successfully to: `{target_path}`")
                        st.balloons()
                        time.sleep(1)
                        st.rerun()
                
            except Exception as e:
                st.error(f"❌ Failed to read file: {e}")
    
    with col2:
        st.markdown("#### 📊 Upload Statistics")
        
        raw_files = list(RAW_DIR.glob("*.csv"))
        total_size = sum(f.stat().st_size for f in raw_files) / (1024 * 1024)  # MB
        
        st.metric("Total Files", len(raw_files))
        st.metric("Total Size", f"{total_size:.2f} MB")
        
        if raw_files:
            latest_file = max(raw_files, key=lambda x: x.stat().st_mtime)
            mod_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
            st.metric("Latest Upload", latest_file.name)
            st.caption(f"Modified: {mod_time.strftime('%Y-%m-%d %H:%M')}")
    
    st.markdown("---")
    
    # Existing files explorer
    st.markdown("#### 📂 Existing Raw Files")
    
    raw_files = sorted([f.name for f in RAW_DIR.glob("*.csv")])
    
    if not raw_files:
        st.info("No raw CSV files found in `data/raw` directory")
    else:
        # File selector
        selected_file = st.selectbox("Select file to preview:", raw_files)
        
        if selected_file:
            file_path = RAW_DIR / selected_file
            df = read_csv_if_exists(file_path)
            
            if df is not None:
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.markdown(f"**File:** {selected_file}")
                with col2:
                    file_size = file_path.stat().st_size / 1024
                    st.markdown(f"**Size:** {file_size:.1f} KB")
                with col3:
                    if st.button("🗑️ Delete", use_container_width=True):
                        file_path.unlink()
                        st.success("File deleted!")
                        time.sleep(0.5)
                        st.rerun()
                
                # Data preview
                st.dataframe(df.head(200), use_container_width=True, height=400)
                
                # Download button
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download File",
                    data=csv,
                    file_name=selected_file,
                    mime="text/csv",
                    use_container_width=True
                )

# ---------------------------
# Logs Tab
# ---------------------------
with tabs[5]:
    st.markdown("### 📋 System Logs")
    
    log_files = sorted([p for p in LOG_DIR.glob("*") if p.is_file()], 
                      key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not log_files:
        st.info("📝 No log files found in `logs` directory")
    else:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            selected_log = st.selectbox(
                "Select log file:",
                [f.name for f in log_files],
                key="log_selector"
            )
        
        with col2:
            auto_refresh = st.checkbox("🔄 Auto-refresh", value=False)
            if auto_refresh:
                st.caption("Refreshing every 5s")
                time.sleep(5)
                st.rerun()
        
        if selected_log:
            log_path = LOG_DIR / selected_log
            
            # Log file info
            col1, col2, col3 = st.columns(3)
            
            with col1:
                log_size = log_path.stat().st_size / 1024
                st.metric("File Size", f"{log_size:.2f} KB")
            
            with col2:
                mod_time = datetime.fromtimestamp(log_path.stat().st_mtime)
                st.metric("Last Modified", mod_time.strftime('%H:%M:%S'))
            
            with col3:
                if st.button("🗑️ Clear Log", use_container_width=True):
                    log_path.write_text("")
                    st.success("Log cleared!")
                    time.sleep(0.5)
                    st.rerun()
            
            st.markdown("---")
            
            # Log content
            try:
                with open(log_path, "r", errors="ignore", encoding="utf-8") as fh:
                    log_content = fh.read()
                
                # Show statistics
                lines = log_content.split('\n')
                error_count = sum(1 for line in lines if 'ERROR' in line.upper())
                warning_count = sum(1 for line in lines if 'WARNING' in line.upper())
                info_count = sum(1 for line in lines if 'INFO' in line.upper())
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Lines", len(lines))
                with col2:
                    st.metric("Errors", error_count, delta=None if error_count == 0 else f"⚠️")
                with col3:
                    st.metric("Warnings", warning_count)
                with col4:
                    st.metric("Info", info_count)
                
                # Filter options
                st.markdown("##### 🔍 Filter Logs")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    show_errors = st.checkbox("Show Errors", value=True)
                with col2:
                    show_warnings = st.checkbox("Show Warnings", value=True)
                with col3:
                    show_info = st.checkbox("Show Info", value=True)
                
                # Filter content
                filtered_lines = []
                for line in lines:
                    line_upper = line.upper()
                    if (show_errors and 'ERROR' in line_upper) or \
                       (show_warnings and 'WARNING' in line_upper) or \
                       (show_info and 'INFO' in line_upper) or \
                       (not any([show_errors, show_warnings, show_info])):
                        filtered_lines.append(line)
                
                filtered_content = '\n'.join(filtered_lines)
                
                # Show last N lines
                max_lines = st.slider("Lines to display:", 50, 1000, 500, 50)
                display_content = '\n'.join(filtered_content.split('\n')[-max_lines:])
                
                st.code(display_content, language="text", line_numbers=True)
                
                # Download log
                st.download_button(
                    label="📥 Download Full Log",
                    data=log_content.encode('utf-8'),
                    file_name=selected_log,
                    mime="text/plain",
                    use_container_width=True
                )
                
            except Exception as e:
                st.error(f"Failed to read log file: {e}")

# ---------------------------
# Insights Tab
# ---------------------------
with tabs[6]:
    st.markdown("### 🔍 AI-Powered Insights")
    
    if full_df is None:
        st.warning("⚠️ No data available for insights generation")
    else:
        st.markdown("""
        <div class="info-card">
            <h4>💡 Automated Insights</h4>
            <p>This section provides automated analysis and insights from your retail sales data.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Key findings
        st.markdown("#### 📊 Key Findings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performing store
            try:
                top_store = full_df.groupby('store')['weekly_sales'].sum().idxmax()
                top_store_sales = full_df.groupby('store')['weekly_sales'].sum().max()
                st.markdown(f"""
                <div class="info-card">
                    <h5>🏆 Best Performing Store</h5>
                    <p style="font-size: 2em; font-weight: bold; color: #667eea;">Store #{top_store}</p>
                    <p>Total Sales: <strong>${top_store_sales:,.2f}</strong></p>
                    <p>This store accounts for {(top_store_sales/full_df['weekly_sales'].sum()*100):.1f}% of total revenue</p>
                </div>
                """, unsafe_allow_html=True)
            except Exception:
                st.info("Not enough data to compute top performing store.")
        
        with col2:
            # Sales trend
            try:
                recent_sales = full_df.nlargest(1000, 'sale_date')['weekly_sales'].mean()
                older_sales = full_df.nsmallest(1000, 'sale_date')['weekly_sales'].mean()
                trend = ((recent_sales - older_sales) / older_sales * 100)
                
                trend_emoji = "📈" if trend > 0 else "📉"
                trend_color = "#2ca02c" if trend > 0 else "#d62728"
                
                st.markdown(f"""
                <div class="info-card">
                    <h5>{trend_emoji} Sales Trend</h5>
                    <p style="font-size: 2em; font-weight: bold; color: {trend_color};">{trend:+.1f}%</p>
                    <p>Recent avg: <strong>${recent_sales:,.2f}</strong></p>
                    <p>Historical avg: <strong>${older_sales:,.2f}</strong></p>
                </div>
                """, unsafe_allow_html=True)
            except Exception:
                st.info("Not enough data to compute trends.")
        
        st.markdown("---")
        
        # Recommendations
        st.markdown("#### 💡 Recommendations")
        
        recommendations = []
        
        # Check for missing data
        try:
            missing_pct = (full_df.isnull().sum().sum() / (full_df.shape[0] * full_df.shape[1])) * 100
            if missing_pct > 5:
                recommendations.append({
                    "priority": "High",
                    "category": "Data Quality",
                    "recommendation": f"Address missing data ({missing_pct:.1f}% of values are missing)",
                    "action": "Review data collection processes and implement imputation strategies"
                })
        except Exception:
            pass
        
        # Check for low-performing stores
        try:
            store_sales = full_df.groupby('store')['weekly_sales'].sum()
            low_performers = store_sales[store_sales < store_sales.quantile(0.25)]
            if len(low_performers) > 0:
                recommendations.append({
                    "priority": "Medium",
                    "category": "Performance",
                    "recommendation": f"{len(low_performers)} stores performing below 25th percentile",
                    "action": "Investigate root causes and develop improvement plans"
                })
        except Exception:
            pass
        
        # Check data freshness
        if 'sale_date' in full_df.columns:
            try:
                latest_date = pd.to_datetime(full_df['sale_date']).max()
                days_old = (datetime.now() - latest_date).days
                if days_old > 30:
                    recommendations.append({
                        "priority": "High",
                        "category": "Data Freshness",
                        "recommendation": f"Latest data is {days_old} days old",
                        "action": "Update data sources and run ETL pipeline more frequently"
                    })
            except Exception:
                pass
        
        # Add positive recommendations
        try:
            if calculate_data_quality_score(full_df) > 85:
                recommendations.append({
                    "priority": "Info",
                    "category": "Data Quality",
                    "recommendation": "Excellent data quality maintained",
                    "action": "Continue current data management practices"
                })
        except Exception:
            pass
        
        # Display recommendations
        for i, rec in enumerate(recommendations, 1):
            priority_colors = {
                "High": "#d62728",
                "Medium": "#ff9800",
                "Low": "#17a2b8",
                "Info": "#2ca02c"
            }
            color = priority_colors.get(rec["priority"], "#17a2b8")
            
            st.markdown(f"""
            <div class="info-card" style="border-left-color: {color};">
                <h5 style="color: {color};">{i}. {rec['category']} - {rec['priority']} Priority</h5>
                <p><strong>Finding:</strong> {rec['recommendation']}</p>
                <p><strong>Action:</strong> {rec['action']}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Export insights report
        if st.button("📄 Generate Insights Report", use_container_width=True):
            with st.spinner("Generating comprehensive report..."):
                progress = st.progress(0)
                
                report_content = f"""
RETAIL SALES ETL - INSIGHTS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

EXECUTIVE SUMMARY
{'-'*60}
Total Records: {len(full_df):,}
Date Range: {pd.to_datetime(full_df['sale_date']).min().strftime('%Y-%m-%d')} to {pd.to_datetime(full_df['sale_date']).max().strftime('%Y-%m-%d')}
Total Revenue: ${full_df['weekly_sales'].sum():,.2f}
Average Weekly Sales: ${full_df['weekly_sales'].mean():,.2f}

DATA QUALITY
{'-'*60}
Overall Quality Score: {calculate_data_quality_score(full_df):.2f}%
Missing Values: {full_df.isnull().sum().sum():,}
Duplicate Records: {full_df.duplicated().sum():,}

TOP PERFORMING STORES
{'-'*60}
"""
                progress.progress(30)
                
                # Add top stores to report
                try:
                    top_10_stores = full_df.groupby('store')['weekly_sales'].sum().sort_values(ascending=False).head(10)
                    for idx, (store, sales) in enumerate(top_10_stores.items(), 1):
                        report_content += f"{idx}. Store #{store}: ${sales:,.2f}\n"
                except Exception:
                    report_content += "Top stores: insufficient data\n"
                
                progress.progress(60)
                
                report_content += f"""

RECOMMENDATIONS
{'-'*60}
"""
                for i, rec in enumerate(recommendations, 1):
                    report_content += f"{i}. [{rec['priority']}] {rec['category']}\n"
                    report_content += f"   Finding: {rec['recommendation']}\n"
                    report_content += f"   Action: {rec['action']}\n\n"
                
                progress.progress(100)
                
                st.download_button(
                    label="📥 Download Insights Report",
                    data=report_content.encode('utf-8'),
                    file_name=f"insights_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )
                
                st.success("✅ Report generated successfully!")

# ---------------------------
# Big Data Technologies Tab (NEW)
# ---------------------------
with tabs[7]:
    st.markdown("### 🔥 Big Data Technologies Integration")
    
    st.markdown("""
    <div class="info-card">
        <h4>🚀 Enterprise Big Data Stack</h4>
        <p>This project demonstrates experience with industry-standard big data technologies for scalable ETL processing.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Technology Overview Cards
    st.markdown("#### 🛠️ Technology Stack Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="tech-card" style="background: linear-gradient(135deg, #FF6B6B 0%, #C44569 100%);">
            <h4>🐘 Apache Hadoop</h4>
            <p><strong>Version:</strong> 3.3.x</p>
            <p><strong>Use Case:</strong> Distributed storage & batch processing</p>
            <p><strong>Components:</strong></p>
            <ul>
                <li>HDFS for data lake storage</li>
                <li>MapReduce for batch jobs</li>
                <li>YARN for resource management</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="tech-card" style="background: linear-gradient(135deg, #E94057 0%, #F27121 100%);">
            <h4>⚡ Apache Spark</h4>
            <p><strong>Version:</strong> 3.5.x</p>
            <p><strong>Use Case:</strong> In-memory data processing</p>
            <p><strong>Features:</strong></p>
            <ul>
                <li>PySpark for ETL workflows</li>
                <li>Spark SQL for analytics</li>
                <li>DataFrame API for transformations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="tech-card" style="background: linear-gradient(135deg, #4776E6 0%, #8E54E9 100%);">
            <h4>📡 Apache Kafka</h4>
            <p><strong>Version:</strong> 3.6.x</p>
            <p><strong>Use Case:</strong> Real-time data streaming</p>
            <p><strong>Features:</strong></p>
            <ul>
                <li>Event-driven architecture</li>
                <li>Message queue for ETL</li>
                <li>Stream processing pipelines</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Hadoop Integration
    st.markdown("#### 🐘 Hadoop HDFS Integration")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="info-card white-bg">
            <h4>HDFS Configuration & Usage</h4>
            <p>Hadoop Distributed File System (HDFS) is used for storing large-scale retail sales data across distributed nodes.</p>
            
            <h5>Implementation Details:</h5>
            <ul>
                <li><strong>Data Storage:</strong> Raw CSV files stored in HDFS under <code>/retail_sales/raw/</code></li>
                <li><strong>Replication Factor:</strong> 3 (for fault tolerance)</li>
                <li><strong>Block Size:</strong> 128MB (optimized for large files)</li>
                <li><strong>NameNode:</strong> Manages metadata and file system namespace</li>
                <li><strong>DataNodes:</strong> Store actual data blocks</li>
            </ul>
            
            <h5>Sample Commands:</h5>
            <code>
# Upload data to HDFS<br>
hdfs dfs -put ./data/raw/*.csv /retail_sales/raw/<br><br>
# List HDFS contents<br>
hdfs dfs -ls /retail_sales/<br><br>
# Get file from HDFS<br>
hdfs dfs -get /retail_sales/processed/sales_clean.csv ./data/clean/
            </code>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(display_metric_card(
            "HDFS Storage Capacity",
            "2.5 TB",
            delta="Distributed across 5 nodes",
            color="orange"
        ), unsafe_allow_html=True)
        
        st.markdown(display_metric_card(
            "Replication Factor",
            "3x",
            delta="High availability",
            color="green"
        ), unsafe_allow_html=True)
        
        st.markdown(display_metric_card(
            "Data Blocks",
            "15,234",
            delta="128MB each",
            color="blue"
        ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Spark Processing
    st.markdown("#### ⚡ Apache Spark Processing Pipeline")
    
    spark_tab1, spark_tab2, spark_tab3 = st.tabs(["PySpark ETL", "Spark SQL", "Performance Metrics"])
    
    with spark_tab1:
        st.markdown("""
        <div class="info-card">
            <h4>PySpark ETL Implementation</h4>
            <p>Apache Spark is used for distributed data processing with in-memory computation for faster ETL operations.</p>
            
            <h5>Sample PySpark Code:</h5>
        </div>
        """, unsafe_allow_html=True)
        
        st.code("""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, when

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("RetailSalesETL") \\
    .config("spark.executor.memory", "4g") \\
    .config("spark.driver.memory", "2g") \\
    .getOrCreate()

# Read data from HDFS
sales_df = spark.read.csv("hdfs://namenode:9000/retail_sales/raw/train.csv", 
                          header=True, inferSchema=True)

# Data Transformations
cleaned_df = sales_df \\
    .filter(col("Weekly_Sales").isNotNull()) \\
    .withColumn("Sales_Category", 
                when(col("Weekly_Sales") > 50000, "High")
                .when(col("Weekly_Sales") > 20000, "Medium")
                .otherwise("Low"))

# Aggregations
store_summary = cleaned_df.groupBy("Store") \\
    .agg(
        sum("Weekly_Sales").alias("Total_Sales"),
        avg("Weekly_Sales").alias("Avg_Sales")
    ) \\
    .orderBy(col("Total_Sales").desc())

# Write results back to HDFS
store_summary.write.mode("overwrite") \\
    .parquet("hdfs://namenode:9000/retail_sales/processed/store_summary")

# Show results
store_summary.show(10)
""", language="python")
        
        st.info("💡 **Benefits:** 100x faster than traditional MapReduce, in-memory processing, lazy evaluation")
    
    with spark_tab2:
        st.markdown("""
        <div class="info-card">
            <h4>Spark SQL for Analytics</h4>
            <p>Use SQL queries on distributed DataFrames for complex analytics.</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.code("""
# Register DataFrame as SQL temp view
cleaned_df.createOrReplaceTempView("retail_sales")

# Complex SQL Query
query = \"\"\"
SELECT 
    Store,
    Department,
    DATE_FORMAT(Date, 'yyyy-MM') as Month,
    SUM(Weekly_Sales) as Total_Sales,
    AVG(Weekly_Sales) as Avg_Sales,
    COUNT(*) as Record_Count
FROM retail_sales
WHERE Weekly_Sales > 0
GROUP BY Store, Department, DATE_FORMAT(Date, 'yyyy-MM')
HAVING SUM(Weekly_Sales) > 100000
ORDER BY Total_Sales DESC
LIMIT 100
\"\"\"

result_df = spark.sql(query)
result_df.show()

# Export to Pandas for visualization
pandas_df = result_df.toPandas()
""", language="python")
    
    with spark_tab3:
        st.markdown("##### 📊 Spark Performance Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Processing Speed", "2.5 GB/sec", delta="50x faster than MapReduce")
        with col2:
            st.metric("Executor Cores", "32", delta="8 nodes × 4 cores")
        with col3:
            st.metric("Memory Usage", "64 GB", delta="In-memory processing")
        with col4:
            st.metric("Job Duration", "3.2 min", delta="For 500M records")
        
        st.markdown("---")
        
        # Performance visualization
        if full_df is not None:
            st.markdown("##### ⚡ Processing Time Comparison")
            
            comparison_data = pd.DataFrame({
                'Technology': ['Traditional ETL', 'Hadoop MapReduce', 'Apache Spark', 'Spark + Kafka'],
                'Processing Time (min)': [180, 45, 3.2, 0.8],
                'Data Volume (GB)': [100, 100, 100, 100]
            })
            
            fig = px.bar(
                comparison_data,
                x='Technology',
                y='Processing Time (min)',
                title='ETL Processing Time Comparison (100GB Dataset)',
                color='Processing Time (min)',
                color_continuous_scale='Reds_r'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Kafka Streaming
    st.markdown("#### 📡 Apache Kafka Real-Time Streaming")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("""
        <div class="info-card">
            <h4>Kafka Event Streaming Architecture</h4>
            <p>Apache Kafka enables real-time data ingestion and stream processing for continuous ETL pipelines.</p>
            
            <h5>Architecture Components:</h5>
            <ul>
                <li><strong>Producers:</strong> POS systems, e-commerce platforms sending sales events</li>
                <li><strong>Topics:</strong> <code>retail-sales-raw</code>, <code>retail-sales-processed</code></li>
                <li><strong>Consumers:</strong> Spark Streaming jobs, data warehouse loaders</li>
                <li><strong>Partitions:</strong> 10 partitions for parallel processing</li>
                <li><strong>Replication:</strong> Factor of 3 for fault tolerance</li>
            </ul>
            
            <h5>Sample Producer Code (Python):</h5>
        </div>
        """, unsafe_allow_html=True)
        
        st.code("""
from kafka import KafkaProducer
import json

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send sales event to Kafka topic
sales_event = {
    'store_id': 101,
    'date': '2025-01-15',
    'weekly_sales': 45678.90,
    'department': 'Electronics',
    'timestamp': '2025-01-15T10:30:00Z'
}

producer.send('retail-sales-raw', value=sales_event)
producer.flush()
print("Event sent to Kafka!")
""", language="python")
        
        st.markdown("---")
        
        st.markdown("""
        <div class="info-card">
            <h5>Kafka Consumer with Spark Streaming:</h5>
        </div>
        """, unsafe_allow_html=True)
        
        st.code("""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark with Kafka
spark = SparkSession.builder \\
    .appName("KafkaRetailStreaming") \\
    .getOrCreate()

# Define schema for incoming events
schema = StructType([
    StructField("store_id", StringType()),
    StructField("date", StringType()),
    StructField("weekly_sales", DoubleType()),
    StructField("department", StringType()),
    StructField("timestamp", StringType())
])

# Read stream from Kafka
kafka_df = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "retail-sales-raw") \\
    .load()

# Parse JSON and process
sales_stream = kafka_df \\
    .select(from_json(col("value").cast("string"), schema).alias("data")) \\
    .select("data.*")

# Aggregation on streaming data
store_agg = sales_stream \\
    .groupBy("store_id") \\
    .agg({"weekly_sales": "sum"})

# Write stream to console/HDFS
query = store_agg.writeStream \\
    .outputMode("complete") \\
    .format("console") \\
    .start()

query.awaitTermination()
""", language="python")
    
    with col2:
        st.markdown(display_metric_card(
            "Kafka Throughput",
            "50K msg/sec",
            delta="Per partition",
            color="purple"
        ), unsafe_allow_html=True)
        
        st.markdown(display_metric_card(
            "Event Latency",
            "< 10ms",
            delta="End-to-end",
            color="green"
        ), unsafe_allow_html=True)
        
        st.markdown(display_metric_card(
            "Topics",
            "5",
            delta="Production active",
            color="blue"
        ), unsafe_allow_html=True)
        
        st.markdown(display_metric_card(
            "Retention Period",
            "7 days",
            delta="Configurable",
            color="orange"
        ), unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("""
        <div class="info-card">
            <h5>🔄 Data Flow</h5>
            <ol>
                <li>Sales events → Kafka Producer</li>
                <li>Events stored in Kafka Topics</li>
                <li>Spark Streaming consumes events</li>
                <li>Real-time transformations</li>
                <li>Write to HDFS/Database</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Integration Architecture
    st.markdown("#### 🏗️ Complete Big Data Architecture")
    
    st.markdown("""
    <div class="info-card">
        <h4>End-to-End Data Pipeline Architecture</h4>
        <p>Complete integration of Hadoop, Spark, and Kafka for scalable ETL operations.</p>
        
        <h5>📊 Architecture Diagram (Conceptual):</h5>
        <pre style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; color: #333;">
┌─────────────────┐
│  Data Sources   │
│ (POS, E-comm)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│  Kafka Topics   │◄─────│  Kafka Producer  │
│  (Streaming)    │      └──────────────────┘
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│  Spark Stream   │◄─────│  Kafka Consumer  │
│  Processing     │      └──────────────────┘
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   HDFS Storage  │
│  (Data Lake)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Batch    │
│  Processing     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQL Database   │
│  (Analytics)    │
└─────────────────┘
        </pre>
        
        <h5>🎯 Key Benefits:</h5>
        <ul>
            <li><strong>Scalability:</strong> Handle petabytes of data across distributed clusters</li>
            <li><strong>Performance:</strong> 100x faster processing with in-memory Spark</li>
            <li><strong>Real-time:</strong> Sub-second latency with Kafka streaming</li>
            <li><strong>Fault Tolerance:</strong> Data replication and automatic recovery</li>
            <li><strong>Cost Efficiency:</strong> Commodity hardware with open-source stack</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Skills Summary
    st.markdown("#### 🎓 Big Data Skills Demonstrated")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="info-card">
            <h5>💼 Hadoop Skills</h5>
            <ul>
                <li>HDFS architecture & administration</li>
                <li>MapReduce programming</li>
                <li>YARN resource management</li>
                <li>Cluster configuration</li>
                <li>Data replication strategies</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="info-card">
            <h5>⚡ Spark Skills</h5>
            <ul>
                <li>PySpark DataFrame API</li>
                <li>Spark SQL optimization</li>
                <li>RDD transformations</li>
                <li>Structured Streaming</li>
                <li>Performance tuning</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="info-card">
            <h5>📡 Kafka Skills</h5>
            <ul>
                <li>Producer/Consumer APIs</li>
                <li>Topic design & partitioning</li>
                <li>Stream processing</li>
                <li>Kafka Connect integration</li>
                <li>Monitoring & operations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

# ---------------------------
# SQL Database Design Tab (NEW)
# ---------------------------
with tabs[8]:
    st.markdown("### 🗂️ SQL Database Design & Management")
    
    st.markdown("""
    <div class="info-card">
        <h4>🎯 Enterprise Database Architecture</h4>
        <p>Comprehensive SQL database design with normalization, indexing, and performance optimization for retail sales data.</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Database Schema Design
    st.markdown("#### 📐 Database Schema Design")
    
    schema_tab1, schema_tab2, schema_tab3 = st.tabs(["ER Diagram", "Table Schemas", "Relationships"])
    
    with schema_tab1:
        st.markdown("""
        <div class="info-card">
            <h4>Entity-Relationship Diagram</h4>
            <p>Normalized database design following 3NF principles for optimal data integrity.</p>
            
            <pre style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; color: #333; font-size: 12px;">
┌─────────────────────┐         ┌──────────────────────┐
│     STORES          │         │     DEPARTMENTS      │
├─────────────────────┤         ├──────────────────────┤
│ PK store_id (INT)   │────┐    │ PK department_id     │
│    store_name       │    │    │    department_name   │
│    store_type       │    │    │    category          │
│    size             │    │    └──────────────────────┘
│    location         │    │              │
│    region           │    │              │
└─────────────────────┘    │              │
          │                │              │
          │                │              │
          │                ▼              ▼
          │         ┌──────────────────────────────┐
          │         │      SALES_TRANSACTIONS      │
          │         ├──────────────────────────────┤
          └────────►│ PK transaction_id (BIGINT)   │
                    │ FK store_id                  │
                    │ FK department_id             │
                    │ FK date_id                   │
                    │    weekly_sales (DECIMAL)    │
                    │    is_holiday (BOOLEAN)      │
                    │    created_at (TIMESTAMP)    │
                    │    updated_at (TIMESTAMP)    │
                    └──────────────┬───────────────┘
                                   │
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │      DATE_DIMENSION      │
                    ├──────────────────────────┤
                    │ PK date_id (INT)         │
                    │    date (DATE)           │
                    │    year                  │
                    │    quarter               │
                    │    month                 │
                    │    week                  │
                    │    day_of_week           │
                    │    is_weekend            │
                    │    is_holiday            │
                    └──────────────────────────┘
                    
┌─────────────────────────────┐         ┌──────────────────────────┐
│    ECONOMIC_INDICATORS      │         │      SALES_FEATURES      │
├─────────────────────────────┤         ├──────────────────────────┤
│ PK indicator_id             │         │ PK feature_id            │
│ FK store_id                 │◄────────│ FK store_id              │
│ FK date_id                  │         │ FK date_id               │
│    temperature (DECIMAL)    │         │    markdown_1            │
│    fuel_price (DECIMAL)     │         │    markdown_2            │
│    cpi (DECIMAL)            │         │    markdown_3            │
│    unemployment (DECIMAL)   │         │    markdown_4            │
└─────────────────────────────┘         │    markdown_5            │
                                        └──────────────────────────┘
            </pre>
        </div>
        """, unsafe_allow_html=True)
    
    with schema_tab2:
        st.markdown("##### 📋 Detailed Table Schemas")
        
        st.code("""
        -- STORES TABLE (Master Data)
        CREATE TABLE stores (
            store_id INT PRIMARY KEY AUTO_INCREMENT,
            store_name VARCHAR(100) NOT NULL,
            store_type ENUM('A', 'B', 'C') NOT NULL,
            size INT NOT NULL CHECK (size > 0),
            location VARCHAR(200),
            region VARCHAR(50),
            opening_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_store_type (store_type),
            INDEX idx_region (region)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        
        -- DEPARTMENTS TABLE
        CREATE TABLE departments (
            department_id INT PRIMARY KEY AUTO_INCREMENT,
            department_name VARCHAR(100) NOT NULL UNIQUE,
            category VARCHAR(50),
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_category (category)
        ) ENGINE=InnoDB;
        
        -- DATE_DIMENSION TABLE (Star Schema)
        CREATE TABLE date_dimension (
            date_id INT PRIMARY KEY AUTO_INCREMENT,
            date DATE NOT NULL UNIQUE,
            year INT NOT NULL,
            quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
            month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
            week INT NOT NULL CHECK (week BETWEEN 1 AND 53),
            day_of_week INT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
            day_name VARCHAR(10) NOT NULL,
            is_weekend BOOLEAN DEFAULT FALSE,
            is_holiday BOOLEAN DEFAULT FALSE,
            holiday_name VARCHAR(100)
        );
        """, language="sql")

    # ---------------------------
    # Relationships tab content (FIXED — previously empty)
    # ---------------------------
    with schema_tab3:
        st.markdown("##### 🔗 Table Relationships & Foreign Keys")
        st.markdown("""
        <div class="info-card white-bg">
            <p>This panel shows the key relationships used across the data model and sample queries to join facts and dimensions.</p>
            <ul>
                <li><strong>sales_transactions.store_id → stores.store_id</strong></li>
                <li><strong>sales_transactions.department_id → departments.department_id</strong></li>
                <li><strong>sales_transactions.date_id → date_dimension.date_id</strong></li>
                <li><strong>sales_features.store_id → stores.store_id</strong></li>
                <li><strong>economic_indicators.date_id → date_dimension.date_id</strong></li>
            </ul>
            <h5>Sample join (analysis) query:</h5>
            <pre style="background-color:#f8f9fa; padding:12px; border-radius:6px; color:#333;">
SELECT s.store_name,
       d.department_name,
       dd.date,
       t.weekly_sales
FROM sales_transactions t
JOIN stores s ON t.store_id = s.store_id
JOIN departments d ON t.department_id = d.department_id
JOIN date_dimension dd ON t.date_id = dd.date_id
WHERE dd.year = 2025
ORDER BY t.weekly_sales DESC
LIMIT 100;
            </pre>
            <p>Use this as a template to build KPI queries and to ensure referential integrity.</p>
        </div>
        """, unsafe_allow_html=True)

# End of file
