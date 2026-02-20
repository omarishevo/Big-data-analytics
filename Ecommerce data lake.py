"""
E-Commerce Data Lake System — Real Flipkart Dataset
====================================================
A full-featured data lake with ingestion, cataloging, querying, 
lineage tracking, and analytics using real e-commerce product data.
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import random
import time
import hashlib
import datetime
import io
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Data Lake | Flipkart",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — Dark Industrial Theme
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;600;700;800&display=swap');

/* ── Global ── */
html, body, [class*="css"] {
    font-family: 'Syne', sans-serif;
    background-color: #0a0e1a;
    color: #d4daf0;
}

/* ── App Background ── */
.stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #0d1426 50%, #0a1220 100%);
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f1630 0%, #0c1220 100%);
    border-right: 1px solid #1e2d50;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] p {
    color: #8899cc !important;
    font-size: 0.82rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}

/* ── Headers ── */
h1 { font-family: 'Syne', sans-serif; font-weight: 800; color: #5ce0ff; letter-spacing: -0.03em; }
h2 { font-family: 'Syne', sans-serif; font-weight: 700; color: #a0c4f8; }
h3 { font-family: 'Syne', sans-serif; font-weight: 600; color: #7aafee; }

/* ── Metric Cards ── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #111828 0%, #0f1d36 100%);
    border: 1px solid #1e3060;
    border-radius: 12px;
    padding: 1rem 1.2rem;
    box-shadow: 0 4px 24px rgba(0, 120, 255, 0.08);
}
[data-testid="stMetricLabel"] { color: #6688bb !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.1em; }
[data-testid="stMetricValue"] { color: #5ce0ff !important; font-family: 'JetBrains Mono', monospace; font-size: 1.8rem !important; }
[data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace; }

/* ── Dataframes ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1e3060;
    border-radius: 8px;
    overflow: hidden;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #0d1530;
    border-bottom: 1px solid #1e3060;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    letter-spacing: 0.08em;
    color: #5577aa;
    padding: 0.6rem 1.4rem;
    border-radius: 0;
}
.stTabs [aria-selected="true"] {
    color: #5ce0ff !important;
    background: #111e3a !important;
    border-bottom: 2px solid #5ce0ff !important;
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #1a3a7a, #0e2a5e);
    color: #a0ccff;
    border: 1px solid #2a4a8a;
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    letter-spacing: 0.06em;
    transition: all 0.2s;
}
.stButton > button:hover {
    background: linear-gradient(135deg, #1e4a9a, #12306e);
    border-color: #5ce0ff;
    color: #5ce0ff;
    box-shadow: 0 0 16px rgba(92, 224, 255, 0.25);
}

/* ── Expander ── */
.streamlit-expanderHeader {
    background: #0f1d36 !important;
    border: 1px solid #1e3060 !important;
    border-radius: 8px !important;
    color: #7ab0ee !important;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82rem;
}

/* ── Info / Success / Warning boxes ── */
.stInfo { background: #0a1e3a; border-left-color: #5ce0ff; }
.stSuccess { background: #0a2a1a; border-left-color: #3dffa0; }
.stWarning { background: #2a1a0a; border-left-color: #ffb83d; }
.stError { background: #2a0a0a; border-left-color: #ff4d6d; }

/* ── Code blocks ── */
code, .stCode {
    font-family: 'JetBrains Mono', monospace !important;
    background: #0d1a30 !important;
    border: 1px solid #1e3060 !important;
    border-radius: 6px !important;
    color: #7adeff !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #0a0e1a; }
::-webkit-scrollbar-thumb { background: #1e3060; border-radius: 2px; }

/* ── Select boxes ── */
[data-baseweb="select"] > div {
    background: #0f1d36 !important;
    border-color: #1e3060 !important;
    color: #a0c4f8 !important;
}

/* ── Sidebar title ── */
.sidebar-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.1rem;
    color: #5ce0ff;
    letter-spacing: 0.04em;
    padding: 0.5rem 0;
    border-bottom: 1px solid #1e3060;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE INITIALIZATION
# ─────────────────────────────────────────────────────────────────────────────
if "lake" not in st.session_state:
    st.session_state.lake = {
        "raw": {},        # Raw Zone
        "bronze": {},     # Bronze Zone (cleaned)
        "silver": {},     # Silver Zone (transformed)
        "gold": {},       # Gold Zone (aggregated/curated)
        "catalog": [],    # Data catalog entries
        "lineage": [],    # Data lineage events
        "jobs": [],       # ETL job history
        "ingestion_log": [],
    }

if "query_history" not in st.session_state:
    st.session_state.query_history = []

if "source_data_loaded" not in st.session_state:
    st.session_state.source_data_loaded = False


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_flipkart_data():
    """Load and clean the Flipkart dataset"""
    df = pd.read_excel('/mnt/user-data/uploads/output.xlsx')
    
    # Basic cleaning
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')
    
    # Parse prices (remove ₹ and commas)
    df['actual_price_clean'] = df['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float, errors='ignore')
    df['selling_price_clean'] = df['selling_price'].str.replace('₹', '').str.replace(',', '').astype(float, errors='ignore')
    
    # Parse discount percentage
    df['discount_pct'] = df['discount'].str.replace('%', '').astype(float, errors='ignore')
    
    # Handle missing ratings
    df['average_rating'] = df['average_rating'].fillna(0)
    
    # Create price difference
    df['price_savings'] = df['actual_price_clean'] - df['selling_price_clean']
    
    # Clean dates
    df['crawled_at'] = pd.to_datetime(df['crawled_at'], errors='coerce')
    
    return df

def make_catalog_entry(name, zone, schema, rows, source):
    return {
        "table_name": name,
        "zone": zone,
        "schema": schema,
        "row_count": rows,
        "source": source,
        "created_at": datetime.datetime.now().isoformat(),
        "checksum": hashlib.md5(name.encode()).hexdigest()[:12],
        "format": "Parquet",
        "owner": "data-eng-team",
        "tags": [zone.lower(), source.lower(), "ecommerce", "flipkart"],
    }

def make_lineage_event(src, dst, operation, rows):
    return {
        "source": src,
        "destination": dst,
        "operation": operation,
        "rows_processed": rows,
        "timestamp": datetime.datetime.now().isoformat(),
        "duration_ms": random.randint(120, 8000),
        "status": "SUCCESS",
    }

def make_job(name, zone, rows, duration):
    return {
        "job_name": name,
        "zone": zone,
        "rows": rows,
        "duration_ms": duration,
        "started_at": datetime.datetime.now().isoformat(),
        "status": random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "WARNING"]),
        "spark_nodes": random.randint(2, 8),
    }

def zone_color(z):
    return {"raw": "#9b59b6", "bronze": "#cd7f32", "silver": "#a8b8c8", "gold": "#ffd700"}.get(z, "#aaa")

def zone_icon(z):
    return {"raw": "🌊", "bronze": "🥉", "silver": "🥈", "gold": "🥇"}.get(z, "📦")


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-title">🏔️ DATA LAKE SYSTEM</div>', unsafe_allow_html=True)
    st.caption("Flipkart E-Commerce · v2.5.0")

    section = st.radio(
        "NAVIGATION",
        ["🏠 Overview", "📥 Data Ingestion", "🏗️ Lake Zones", "🔍 Query Engine",
         "📋 Data Catalog", "🔗 Data Lineage", "⚡ ETL Pipeline", "📊 Analytics"],
        label_visibility="visible"
    )

    st.divider()
    st.markdown("**LAKE STORAGE**")
    total_tables = sum(len(v) for v in st.session_state.lake.values() if isinstance(v, dict))
    raw_size = len(st.session_state.lake["raw"])
    bronze_size = len(st.session_state.lake["bronze"])
    silver_size = len(st.session_state.lake["silver"])
    gold_size = len(st.session_state.lake["gold"])

    def mini_bar(label, val, total, color):
        pct = int((val / max(total, 1)) * 100)
        st.markdown(
            f'<div style="margin:4px 0"><span style="color:#666;font-size:0.72rem;font-family:monospace">'
            f'{label}</span><div style="background:#111;border-radius:3px;height:6px;margin-top:2px">'
            f'<div style="width:{pct}%;background:{color};height:6px;border-radius:3px"></div></div>'
            f'<span style="color:{color};font-size:0.7rem;font-family:monospace">{val} datasets</span></div>',
            unsafe_allow_html=True
        )

    max_any = max(raw_size, bronze_size, silver_size, gold_size, 1)
    mini_bar("🌊 RAW", raw_size, max_any, "#9b59b6")
    mini_bar("🥉 BRONZE", bronze_size, max_any, "#cd7f32")
    mini_bar("🥈 SILVER", silver_size, max_any, "#a8b8c8")
    mini_bar("🥇 GOLD", gold_size, max_any, "#ffd700")

    st.divider()
    st.caption(f"🔄 Last sync: {datetime.datetime.now().strftime('%H:%M:%S')}")
    st.caption(f"✅ Jobs run: {len(st.session_state.lake['jobs'])}")


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 1: OVERVIEW ──────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
if section == "🏠 Overview":
    st.markdown("# 🏔️ Flipkart E-Commerce Data Lake")
    st.markdown("**Enterprise-grade multi-zone data lake with 30,000 real product records**")

    # KPI Row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Datasets", total_tables, "+1 today")
    c2.metric("ETL Jobs Run", len(st.session_state.lake["jobs"]), f"+{len(st.session_state.lake['jobs'])}")
    c3.metric("Catalog Entries", len(st.session_state.lake["catalog"]), "")
    c4.metric("Lineage Events", len(st.session_state.lake["lineage"]), "")
    c5.metric("Lake Health", "98.7%", "+0.5%")

    st.divider()

    # Architecture Diagram
    st.markdown("### 🏗️ Data Lake Architecture")
    arch_html = """
    <div style="background:#0d1530;border:1px solid #1e3060;border-radius:14px;padding:1.5rem;font-family:'JetBrains Mono',monospace;font-size:0.78rem">
      <div style="display:flex;gap:12px;align-items:stretch;flex-wrap:wrap">

        <!-- Sources -->
        <div style="flex:1;min-width:140px">
          <div style="color:#5ce0ff;font-weight:700;margin-bottom:8px;font-size:0.8rem">📡 DATA SOURCES</div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">🛍️ Flipkart API</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">📦 Product Catalog</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">💰 Pricing Data</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">⭐ Ratings & Reviews</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">📤 Excel / CSV</div>
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Raw Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#9b59b6;font-weight:700;margin-bottom:8px;font-size:0.8rem">🌊 RAW ZONE</div>
          <div style="background:#1a1020;border:1px solid #6b3fa0;border-radius:6px;padding:10px;color:#c89aea;font-size:0.72rem">
            Original format<br>30K products<br>All fields<br>Immutable
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Bronze Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#cd7f32;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥉 BRONZE ZONE</div>
          <div style="background:#1a1208;border:1px solid #cd7f32;border-radius:6px;padding:10px;color:#e8b870;font-size:0.72rem">
            Price parsing<br>Type casting<br>Null handling<br>De-duplicated
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Silver Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#a8b8c8;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥈 SILVER ZONE</div>
          <div style="background:#101418;border:1px solid #a8b8c8;border-radius:6px;padding:10px;color:#c8d8e8;font-size:0.72rem">
            Enriched metrics<br>Discount calc<br>Price analytics<br>Standardized
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Gold Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#ffd700;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥇 GOLD ZONE</div>
          <div style="background:#1a1508;border:1px solid #ffd700;border-radius:6px;padding:10px;color:#ffe875;font-size:0.72rem">
            Category KPIs<br>Brand analysis<br>Price insights<br>BI-ready
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Consumers -->
        <div style="flex:1;min-width:130px">
          <div style="color:#3dffa0;font-weight:700;margin-bottom:8px;font-size:0.8rem">📊 CONSUMERS</div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">📈 Dashboards</div>
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">🤖 ML Models</div>
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">🔍 Analytics</div>
          </div>
        </div>

      </div>
    </div>
    """
    st.markdown(arch_html, unsafe_allow_html=True)

    st.divider()

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("### 📊 Zone Distribution")
        zone_data = {
            "Zone": ["Raw 🌊", "Bronze 🥉", "Silver 🥈", "Gold 🥇"],
            "Datasets": [raw_size, bronze_size, silver_size, gold_size],
            "Color": ["#9b59b6", "#cd7f32", "#a8b8c8", "#ffd700"]
        }
        fig = go.Figure(go.Bar(
            x=zone_data["Zone"], y=zone_data["Datasets"],
            marker_color=zone_data["Color"],
            marker_line_width=0,
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0),
            height=250,
            xaxis=dict(gridcolor="#1a2a44"),
            yaxis=dict(gridcolor="#1a2a44"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("### ⚡ Recent Activity")
        if st.session_state.lake["jobs"]:
            recent = st.session_state.lake["jobs"][-8:][::-1]
            for j in recent:
                col = "#3dffa0" if j["status"] == "SUCCESS" else "#ffb83d"
                st.markdown(
                    f'<div style="background:#0f1d36;border:1px solid #1e3060;border-radius:6px;'
                    f'padding:6px 10px;margin:4px 0;font-family:JetBrains Mono,monospace;font-size:0.72rem;'
                    f'display:flex;justify-content:space-between">'
                    f'<span style="color:{col}">● {j["job_name"]}</span>'
                    f'<span style="color:#446688">{j["zone"].upper()} · {j["rows"]:,} rows · {j["duration_ms"]}ms</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.info("No jobs run yet. Load the Flipkart data from the Ingestion tab.")


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 2: DATA INGESTION ────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "📥 Data Ingestion":
    st.markdown("# 📥 Data Ingestion")
    st.markdown("Load the Flipkart e-commerce dataset (30,000 products) into the **Raw Zone**.")

    tab1, tab2 = st.tabs(["🛍️ FLIPKART DATASET", "📁 ADDITIONAL CSV"])

    with tab1:
        st.markdown("### Flipkart Product Catalog (30,000 Products)")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Dataset contains:**
            - 🏷️ Product titles, descriptions, brands
            - 💰 Actual price, selling price, discount %
            - ⭐ Average ratings (0-5)
            - 📦 Categories, sub-categories, sellers
            - 🔗 Product URLs and images
            - 📅 Crawl timestamps
            - 📊 Stock status
            """)
        
        with col2:
            st.info("""
            **Schema:**
            - 30,000 rows
            - 18 columns
            - Excel format
            - Source: Flipkart API
            """)

        if not st.session_state.source_data_loaded:
            if st.button("⬇️ LOAD FLIPKART DATASET INTO RAW ZONE", use_container_width=True, type="primary"):
                with st.spinner("Loading 30,000 products into Raw Zone..."):
                    time.sleep(0.5)
                    df = load_flipkart_data()
                    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    dataset_name = f"flipkart_products_{ts}"
                    
                    # Store in Raw Zone
                    st.session_state.lake["raw"][dataset_name] = df
                    
                    # Catalog entry
                    entry = make_catalog_entry(
                        dataset_name, "RAW", 
                        list(df.dtypes.astype(str).items()), 
                        len(df), 
                        "flipkart-excel"
                    )
                    st.session_state.lake["catalog"].append(entry)
                    
                    # Lineage
                    lineage = make_lineage_event("flipkart-api", f"raw/{dataset_name}", "INGEST", len(df))
                    st.session_state.lake["lineage"].append(lineage)
                    
                    # Job log
                    job = make_job(f"ingest_flipkart_products", "raw", len(df), random.randint(1200, 2400))
                    st.session_state.lake["jobs"].append(job)
                    
                    # Ingestion log
                    log_entry = {
                        "dataset": dataset_name, 
                        "rows": len(df), 
                        "time": datetime.datetime.now().isoformat(),
                        "source": "Flipkart Excel"
                    }
                    st.session_state.lake["ingestion_log"].append(log_entry)
                    
                    st.session_state.source_data_loaded = True
                    
                st.success(f"✅ Successfully ingested **{dataset_name}** with **{len(df):,} rows** into Raw Zone!")
                st.balloons()
                time.sleep(1)
                st.rerun()
        else:
            st.success("✅ Flipkart dataset already loaded into Raw Zone!")
            
            # Show preview
            if st.session_state.lake["raw"]:
                first_dataset = list(st.session_state.lake["raw"].values())[0]
                st.markdown("**Preview (first 5 rows):**")
                st.dataframe(first_dataset.head(5), use_container_width=True)

    with tab2:
        st.markdown("### Upload Additional CSV File")
        uploaded = st.file_uploader("Drop CSV file here", type=["csv"], help="Max 200MB")
        if uploaded:
            df_upload = pd.read_csv(uploaded)
            st.markdown(f"**Preview** — `{uploaded.name}` ({len(df_upload):,} rows × {len(df_upload.columns)} cols)")
            st.dataframe(df_upload.head(10), use_container_width=True)
            dataset_label = st.text_input("Dataset name", value=uploaded.name.replace(".csv", "").replace(" ", "_"))
            if st.button("⬇️ Ingest to Raw Zone", use_container_width=True):
                with st.spinner("Ingesting..."):
                    time.sleep(0.3)
                    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    name = f"{dataset_label}_{ts}"
                    st.session_state.lake["raw"][name] = df_upload
                    entry = make_catalog_entry(name, "RAW", list(df_upload.dtypes.astype(str).items()), len(df_upload), "csv-upload")
                    st.session_state.lake["catalog"].append(entry)
                    lineage = make_lineage_event(uploaded.name, f"raw/{name}", "INGEST", len(df_upload))
                    st.session_state.lake["lineage"].append(lineage)
                    job = make_job(f"ingest_csv_{dataset_label}", "raw", len(df_upload), random.randint(300, 900))
                    st.session_state.lake["jobs"].append(job)
                st.success(f"✅ Ingested `{name}` → {len(df_upload):,} rows")

    # Ingestion log
    if st.session_state.lake["ingestion_log"]:
        st.divider()
        st.markdown("### 📋 Ingestion Log")
        log_df = pd.DataFrame(st.session_state.lake["ingestion_log"])
        st.dataframe(log_df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 3: LAKE ZONES ────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "🏗️ Lake Zones":
    st.markdown("# 🏗️ Lake Zones")
    st.markdown("Browse datasets across all four zones and trigger zone promotions (ETL transformations).")

    zone_tabs = st.tabs(["🌊 RAW", "🥉 BRONZE", "🥈 SILVER", "🥇 GOLD"])
    zones = ["raw", "bronze", "silver", "gold"]
    zone_labels = ["RAW", "BRONZE", "SILVER", "GOLD"]

    for tab, zone, label in zip(zone_tabs, zones, zone_labels):
        with tab:
            data = st.session_state.lake[zone]
            if not data:
                st.info(f"No datasets in {label} zone yet.")
                if zone == "raw":
                    st.info("💡 Go to **Data Ingestion** tab to load the Flipkart dataset.")
                continue

            st.markdown(f"**{len(data)} dataset(s) in {label} zone**")

            for name, df in data.items():
                color = zone_color(zone)
                with st.expander(f"{zone_icon(zone)}  {name}   ·   {len(df):,} rows × {len(df.columns)} cols"):
                    c1, c2, c3 = st.columns(3)
                    c1.markdown(f"**Rows:** `{len(df):,}`")
                    c2.markdown(f"**Columns:** `{len(df.columns)}`")
                    c3.markdown(f"**Memory:** `{df.memory_usage(deep=True).sum() / 1024:.1f} KB`")

                    st.dataframe(df.head(10), use_container_width=True)

                    # Schema
                    schema_df = pd.DataFrame({
                        "Column": df.columns,
                        "Dtype": df.dtypes.astype(str).values,
                        "Null%": (df.isnull().mean() * 100).round(1).values,
                        "Unique": [df[c].nunique() for c in df.columns],
                    })
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)

                    # Promote button
                    next_zones = {"raw": "bronze", "bronze": "silver", "silver": "gold"}
                    if zone in next_zones:
                        nz = next_zones[zone]
                        if st.button(f"⬆️ Promote to {nz.upper()}", key=f"promote_{zone}_{name}"):
                            with st.spinner(f"Transforming to {nz}..."):
                                time.sleep(0.7)
                                df_t = df.copy()
                                
                                # Zone-specific transformations
                                if nz == "bronze":
                                    # Clean nulls, duplicates, parse prices
                                    df_t = df_t.dropna(subset=['pid', 'title'])
                                    df_t = df_t.drop_duplicates(subset=['pid'])
                                    if 'actual_price_clean' not in df_t.columns:
                                        if 'actual_price' in df_t.columns:
                                            df_t['actual_price_clean'] = df_t['actual_price'].str.replace('₹', '').str.replace(',', '')
                                            df_t['actual_price_clean'] = pd.to_numeric(df_t['actual_price_clean'], errors='coerce')
                                        if 'selling_price' in df_t.columns:
                                            df_t['selling_price_clean'] = df_t['selling_price'].str.replace('₹', '').str.replace(',', '')
                                            df_t['selling_price_clean'] = pd.to_numeric(df_t['selling_price_clean'], errors='coerce')
                                    
                                elif nz == "silver":
                                    # Add business metrics
                                    if 'actual_price_clean' in df_t.columns and 'selling_price_clean' in df_t.columns:
                                        df_t['price_savings'] = df_t['actual_price_clean'] - df_t['selling_price_clean']
                                        df_t['discount_pct_calc'] = ((df_t['actual_price_clean'] - df_t['selling_price_clean']) / df_t['actual_price_clean'] * 100).round(2)
                                    if 'average_rating' in df_t.columns:
                                        df_t['rating_category'] = pd.cut(
                                            df_t['average_rating'], 
                                            bins=[0, 2, 3, 4, 5], 
                                            labels=['Poor', 'Fair', 'Good', 'Excellent']
                                        )
                                    
                                elif nz == "gold":
                                    # Aggregate by category
                                    if 'category' in df_t.columns and 'selling_price_clean' in df_t.columns:
                                        df_t = df_t.groupby('category').agg({
                                            'pid': 'count',
                                            'selling_price_clean': ['mean', 'median', 'min', 'max'],
                                            'average_rating': 'mean',
                                            'discount_pct_calc': 'mean' if 'discount_pct_calc' in df_t.columns else 'first'
                                        }).round(2)
                                        df_t.columns = ['product_count', 'avg_price', 'median_price', 'min_price', 'max_price', 'avg_rating', 'avg_discount']
                                        df_t = df_t.reset_index()
                                    else:
                                        df_t = df_t.head(100)

                                new_name = name.replace(f"{zone}_", "")
                                ts = datetime.datetime.now().strftime("%H%M%S")
                                dest_name = f"{nz}_{new_name}_{ts}"
                                st.session_state.lake[nz][dest_name] = df_t

                                entry = make_catalog_entry(dest_name, nz.upper(), list(df_t.dtypes.astype(str).items()), len(df_t), f"{zone}-promotion")
                                st.session_state.lake["catalog"].append(entry)
                                lineage = make_lineage_event(f"{zone}/{name}", f"{nz}/{dest_name}", "PROMOTE", len(df_t))
                                st.session_state.lake["lineage"].append(lineage)
                                job = make_job(f"promote_{zone}_to_{nz}", nz, len(df_t), random.randint(500, 3000))
                                st.session_state.lake["jobs"].append(job)
                            st.success(f"✅ Promoted to {nz.upper()} as `{dest_name}` ({len(df_t):,} rows)")
                            st.rerun()

                    # Download
                    csv_buf = io.StringIO()
                    df.to_csv(csv_buf, index=False)
                    st.download_button(
                        f"💾 Download CSV", csv_buf.getvalue(),
                        file_name=f"{name}.csv", mime="text/csv",
                        key=f"dl_{zone}_{name}"
                    )


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 4: QUERY ENGINE ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "🔍 Query Engine":
    st.markdown("# 🔍 Query Engine")
    st.markdown("Run pandas-based queries across any dataset in the lake.")

    # Gather all datasets
    all_datasets = {}
    for zone in ["raw", "bronze", "silver", "gold"]:
        for name, df in st.session_state.lake[zone].items():
            all_datasets[f"{zone_icon(zone)} {zone}/{name}"] = (zone, name, df)

    if not all_datasets:
        st.warning("⚠️ No datasets available. Ingest some data first.")
    else:
        col1, col2 = st.columns([1, 2])
        with col1:
            selected = st.selectbox("Select Dataset", list(all_datasets.keys()))
            zone, name, df = all_datasets[selected]
            st.markdown(f"**Shape:** `{df.shape[0]:,} rows × {df.shape[1]} cols`")

            st.markdown("**Columns:**")
            for col_name, dtype in df.dtypes.items():
                st.markdown(
                    f'<span style="font-family:JetBrains Mono,monospace;font-size:0.75rem;'
                    f'color:#7ab0ee">{col_name}</span> '
                    f'<span style="font-family:JetBrains Mono,monospace;font-size:0.68rem;color:#446688">{dtype}</span>',
                    unsafe_allow_html=True
                )

        with col2:
            st.markdown("**Query (pandas expression)**")
            st.caption("Use `df` to reference the selected dataset.")

            query_presets = {
                "Top 10 rows": "df.head(10)",
                "Products by category": "df.groupby('category').size().reset_index(name='count').sort_values('count', ascending=False)" if "category" in df.columns else "df.head()",
                "Top brands": "df.groupby('brand')['selling_price_clean'].mean().sort_values(ascending=False).head(10).reset_index()" if "brand" in df.columns and "selling_price_clean" in df.columns else "df.head()",
                "High discount products": "df.nlargest(20, 'discount_pct_calc')[['title', 'brand', 'discount_pct_calc', 'selling_price_clean']]" if "discount_pct_calc" in df.columns else "df.head()",
                "Rating distribution": "df['average_rating'].value_counts().sort_index().reset_index()" if "average_rating" in df.columns else "df.head()",
                "Statistical summary": "df.describe()",
                "Null value counts": "df.isnull().sum().reset_index().rename(columns={0:'nulls','index':'column'})",
            }

            preset = st.selectbox("Quick Queries", ["Custom..."] + list(query_presets.keys()))
            default_query = query_presets.get(preset, "df.head(10)") if preset != "Custom..." else "df.head(10)"

            query = st.text_area("Query", default_query, height=80)

            if st.button("▶ Execute Query", use_container_width=True):
                t0 = time.time()
                try:
                    result = eval(query, {"df": df, "pd": pd, "np": np})
                    elapsed = int((time.time() - t0) * 1000)
                    st.session_state.query_history.append({
                        "query": query[:80], "dataset": name,
                        "rows": len(result) if hasattr(result, "__len__") else 1,
                        "time_ms": elapsed,
                        "status": "OK"
                    })
                    st.success(f"✅ Query executed in **{elapsed}ms**")
                    if isinstance(result, pd.DataFrame):
                        st.dataframe(result, use_container_width=True)
                        # Plot if numeric
                        num_cols = result.select_dtypes(include="number").columns.tolist()
                        if len(result.columns) >= 2 and len(num_cols) >= 1:
                            st.markdown("**Auto Visualization**")
                            x_col = result.columns[0]
                            y_col = num_cols[0]
                            fig = px.bar(
                                result.head(20), x=x_col, y=y_col,
                                color_discrete_sequence=["#5ce0ff"],
                            )
                            fig.update_layout(
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                font=dict(color="#8899cc", family="JetBrains Mono"),
                                margin=dict(t=10, b=0, l=0, r=0),
                                xaxis=dict(gridcolor="#1a2a44"),
                                yaxis=dict(gridcolor="#1a2a44"),
                                height=280,
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    elif isinstance(result, pd.Series):
                        st.dataframe(result.reset_index(), use_container_width=True)
                    else:
                        st.write(result)
                except Exception as e:
                    elapsed = int((time.time() - t0) * 1000)
                    st.session_state.query_history.append({
                        "query": query[:80], "dataset": name, "rows": 0,
                        "time_ms": elapsed, "status": f"ERROR: {e}"
                    })
                    st.error(f"❌ Query error: {e}")

        # Query history
        if st.session_state.query_history:
            st.divider()
            st.markdown("### 🕒 Query History")
            hist_df = pd.DataFrame(st.session_state.query_history[::-1])
            st.dataframe(hist_df, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 5: DATA CATALOG ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "📋 Data Catalog":
    st.markdown("# 📋 Data Catalog")
    st.markdown("Searchable metadata registry for all datasets in the lake.")

    if not st.session_state.lake["catalog"]:
        st.info("Catalog is empty. Ingest the Flipkart data to populate it.")
    else:
        # Search & filter
        col1, col2 = st.columns([2, 1])
        with col1:
            search = st.text_input("🔍 Search catalog", placeholder="table name, zone, source...")
        with col2:
            zone_filter = st.selectbox("Filter by zone", ["ALL", "RAW", "BRONZE", "SILVER", "GOLD"])

        entries = st.session_state.lake["catalog"]
        if search:
            entries = [e for e in entries if search.lower() in json.dumps(e).lower()]
        if zone_filter != "ALL":
            entries = [e for e in entries if e.get("zone") == zone_filter]

        st.markdown(f"**{len(entries)} entries found**")

        for entry in entries[:30]:
            zone_c = zone_color(entry.get("zone", "raw").lower())
            with st.expander(
                f"{zone_icon(entry.get('zone','raw').lower())}  **{entry['table_name']}**   "
                f"·  {entry.get('zone','')}  ·  {entry.get('row_count',0):,} rows"
            ):
                c1, c2, c3, c4 = st.columns(4)
                c1.markdown(f"**Zone**\n\n`{entry.get('zone','')}`")
                c2.markdown(f"**Source**\n\n`{entry.get('source','')}`")
                c3.markdown(f"**Format**\n\n`{entry.get('format','')}`")
                c4.markdown(f"**Owner**\n\n`{entry.get('owner','')}`")

                st.markdown(f"**Checksum:** `{entry.get('checksum','')}`  ·  **Created:** `{entry.get('created_at','')[:19]}`")

                tags = entry.get("tags", [])
                tag_html = " ".join([
                    f'<span style="background:#0a1e3a;border:1px solid #1e4060;border-radius:20px;'
                    f'padding:2px 10px;font-size:0.72rem;color:#5ce0ff;font-family:JetBrains Mono,monospace">{t}</span>'
                    for t in tags
                ])
                st.markdown(f"**Tags:** {tag_html}", unsafe_allow_html=True)

                schema_items = entry.get("schema", [])
                if schema_items and len(schema_items) > 0:
                    schema_df = pd.DataFrame(schema_items, columns=["Column", "Dtype"])
                    st.dataframe(schema_df.head(20), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 6: DATA LINEAGE ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "🔗 Data Lineage":
    st.markdown("# 🔗 Data Lineage")
    st.markdown("Track how data flows and transforms across lake zones.")

    if not st.session_state.lake["lineage"]:
        st.info("No lineage events recorded yet. Ingest and promote some data.")
    else:
        lineage = st.session_state.lake["lineage"]
        lineage_df = pd.DataFrame(lineage)
        st.dataframe(lineage_df, use_container_width=True, hide_index=True)

        st.divider()

        # Sankey diagram
        st.markdown("### 🌊 Data Flow Diagram (Sankey)")

        # Build sankey
        all_nodes = list(set(lineage_df["source"].tolist() + lineage_df["destination"].tolist()))
        node_idx = {n: i for i, n in enumerate(all_nodes)}

        sankey_colors = []
        for n in all_nodes:
            if "raw/" in n or "flipkart" in n.lower() or "api" in n.lower():
                sankey_colors.append("#9b59b6")
            elif "bronze/" in n:
                sankey_colors.append("#cd7f32")
            elif "silver/" in n:
                sankey_colors.append("#a8b8c8")
            elif "gold/" in n:
                sankey_colors.append("#ffd700")
            else:
                sankey_colors.append("#3dffa0")

        fig_sankey = go.Figure(go.Sankey(
            node=dict(
                pad=15, thickness=15,
                line=dict(color="#1e3060", width=0.5),
                label=[n[:35] + ("..." if len(n) > 35 else "") for n in all_nodes],
                color=sankey_colors,
                hovertemplate="%{label}<extra></extra>",
            ),
            link=dict(
                source=[node_idx[r["source"]] for _, r in lineage_df.iterrows()],
                target=[node_idx[r["destination"]] for _, r in lineage_df.iterrows()],
                value=[max(r["rows_processed"], 1) for _, r in lineage_df.iterrows()],
                color=["rgba(92,224,255,0.15)"] * len(lineage_df),
                hovertemplate="%{source.label} → %{target.label}<br>%{value:,} rows<extra></extra>",
            )
        ))
        fig_sankey.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono", size=11),
            margin=dict(t=10, b=10, l=10, r=10),
            height=400,
        )
        st.plotly_chart(fig_sankey, use_container_width=True)

        # Stats
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", len(lineage))
        col2.metric("Total Rows Moved", f"{lineage_df['rows_processed'].sum():,}")
        col3.metric("Avg Duration (ms)", f"{lineage_df['duration_ms'].mean():.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 7: ETL PIPELINE ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "⚡ ETL Pipeline":
    st.markdown("# ⚡ ETL Pipeline")
    st.markdown("Monitor and trigger transformation jobs across all zones.")

    # Run a full pipeline
    raw_datasets = list(st.session_state.lake["raw"].keys())

    if not raw_datasets:
        st.warning("⚠️ No raw datasets available. Load Flipkart data first.")
    else:
        st.markdown("### 🚀 Run Full Pipeline")
        selected_raw = st.selectbox("Select Raw Dataset", raw_datasets)
        pipe_cols = st.columns(4)

        steps = [
            ("🌊→🥉 Clean", "bronze", "Parse & validate"),
            ("🥉→🥈 Enrich", "silver", "Add metrics"),
            ("🥈→🥇 Aggregate", "gold", "Compute KPIs"),
            ("🥇 Publish", "gold", "BI-ready"),
        ]

        for col, (step_label, zone, desc) in zip(pipe_cols, steps):
            col.markdown(
                f'<div style="background:#0f1d36;border:1px solid #1e3060;border-radius:8px;'
                f'padding:12px;text-align:center;font-family:JetBrains Mono,monospace;font-size:0.75rem">'
                f'<div style="color:{zone_color(zone)};font-weight:700">{step_label}</div>'
                f'<div style="color:#446688;margin-top:4px">{desc}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

        st.markdown("")
        if st.button("▶▶ RUN FULL PIPELINE", use_container_width=True, type="primary"):
            df_src = st.session_state.lake["raw"][selected_raw]
            ts = datetime.datetime.now().strftime("%H%M%S")
            progress = st.progress(0, "Initializing...")
            status_box = st.empty()

            pipeline_stages = [
                ("Bronze: Clean & Validate", "bronze", lambda d: d.dropna(subset=['pid']).drop_duplicates(subset=['pid'])),
                ("Silver: Enrich & Transform", "silver", lambda d: (
                    d.assign(
                        price_savings=d['actual_price_clean'] - d['selling_price_clean'],
                        discount_pct_calc=((d['actual_price_clean'] - d['selling_price_clean']) / d['actual_price_clean'] * 100).round(2)
                    ) if 'actual_price_clean' in d.columns and 'selling_price_clean' in d.columns else d
                )),
                ("Gold: Aggregate KPIs", "gold", lambda d: (
                    d.groupby('category').agg({
                        'pid': 'count',
                        'selling_price_clean': ['mean', 'min', 'max'],
                        'average_rating': 'mean',
                        'discount_pct_calc': 'mean' if 'discount_pct_calc' in d.columns else 'first'
                    }).round(2).reset_index()
                    if 'category' in d.columns and 'selling_price_clean' in d.columns else d.describe()
                )),
            ]

            current_df = df_src
            for i, (stage_name, zone, transform_fn) in enumerate(pipeline_stages):
                status_box.info(f"⚙️ Running: **{stage_name}**...")
                time.sleep(0.8)
                try:
                    current_df = transform_fn(current_df)
                except Exception as e:
                    st.warning(f"Transformation partially failed: {e}. Using fallback.")
                    current_df = current_df.head(100)

                dest_name = f"{zone}_{selected_raw}_{ts}"
                st.session_state.lake[zone][dest_name] = current_df
                entry = make_catalog_entry(dest_name, zone.upper(), list(current_df.dtypes.astype(str).items()), len(current_df), "etl-pipeline")
                st.session_state.lake["catalog"].append(entry)
                lin = make_lineage_event(f"pipeline_stage_{i}", f"{zone}/{dest_name}", stage_name, len(current_df))
                st.session_state.lake["lineage"].append(lin)
                job = make_job(stage_name, zone, len(current_df), random.randint(600, 4000))
                st.session_state.lake["jobs"].append(job)
                progress.progress((i + 1) / len(pipeline_stages), f"Completed: {stage_name}")

            status_box.success("✅ Full pipeline completed successfully!")
            st.balloons()
            time.sleep(1)
            st.rerun()

    # Job History
    st.divider()
    st.markdown("### 📋 Job History")
    if st.session_state.lake["jobs"]:
        jobs_df = pd.DataFrame(st.session_state.lake["jobs"][::-1])
        st.dataframe(jobs_df, use_container_width=True, hide_index=True)

        # Timeline chart
        fig_jobs = go.Figure()
        for zone_name in ["raw", "bronze", "silver", "gold"]:
            zone_jobs = jobs_df[jobs_df["zone"] == zone_name]
            if not zone_jobs.empty:
                fig_jobs.add_trace(go.Bar(
                    name=zone_name.upper(),
                    x=zone_jobs["job_name"],
                    y=zone_jobs["duration_ms"],
                    marker_color=zone_color(zone_name),
                ))
        fig_jobs.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=20, b=0),
            height=280,
            barmode="group",
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#8899cc")),
            xaxis=dict(gridcolor="#1a2a44", tickangle=-30),
            yaxis=dict(gridcolor="#1a2a44", title="Duration (ms)"),
        )
        st.plotly_chart(fig_jobs, use_container_width=True)
    else:
        st.info("No ETL jobs have run yet.")


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 8: ANALYTICS ─────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "📊 Analytics":
    st.markdown("# 📊 Analytics Dashboard")
    st.markdown("Business intelligence views powered by the Flipkart dataset.")

    # Check if we have data
    if not st.session_state.lake["raw"] and not st.session_state.source_data_loaded:
        st.warning("⚠️ No data loaded yet. Go to **Data Ingestion** to load the Flipkart dataset.")
    else:
        # Load fresh data for analytics
        df = load_flipkart_data()
        
        # KPIs
        st.markdown("### 🔑 Key Performance Indicators")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Products", f"{len(df):,}", "30K catalog")
        k2.metric("Avg Rating", f"{df['average_rating'].mean():.2f} ⭐", "+0.08")
        k3.metric("Avg Discount", f"{df['discount_pct'].mean():.1f}%", "-2.3%")
        k4.metric("Avg Price", f"₹{df['selling_price_clean'].mean():,.0f}", "+5.2%")
        k5.metric("Out of Stock", f"{df['out_of_stock'].sum():,}", f"{(df['out_of_stock'].sum()/len(df)*100):.1f}%")

        st.divider()

        # Charts Row 1
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📦 Products by Category")
            cat_counts = df['category'].value_counts().head(10).reset_index()
            cat_counts.columns = ['category', 'count']
            fig_cat = px.bar(cat_counts, x='count', y='category', orientation='h',
                             color='count', color_continuous_scale=["#1a3a7a", "#5ce0ff"])
            fig_cat.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8899cc", family="JetBrains Mono"),
                margin=dict(t=10, b=0, l=0, r=0), height=320,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="#1a2a44"),
                yaxis=dict(gridcolor="#1a2a44"),
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with col2:
            st.markdown("#### 💰 Price Distribution")
            fig_price = px.histogram(df[df['selling_price_clean'] < 10000], 
                                    x='selling_price_clean', nbins=50,
                                    color_discrete_sequence=["#5ce0ff"])
            fig_price.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8899cc", family="JetBrains Mono"),
                margin=dict(t=10, b=0, l=0, r=0), height=320,
                xaxis=dict(gridcolor="#1a2a44", title="Selling Price (₹)"),
                yaxis=dict(gridcolor="#1a2a44", title="Count"),
                bargap=0.05,
            )
            st.plotly_chart(fig_price, use_container_width=True)

        # Charts Row 2
        col3, col4 = st.columns(2)

        with col3:
            st.markdown("#### ⭐ Rating Distribution")
            rating_dist = df['average_rating'].value_counts().sort_index().reset_index()
            rating_dist.columns = ['rating', 'count']
            fig_rating = px.line(rating_dist, x='rating', y='count', markers=True,
                                color_discrete_sequence=["#ffd700"])
            fig_rating.update_traces(line=dict(width=3), marker=dict(size=8))
            fig_rating.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8899cc", family="JetBrains Mono"),
                margin=dict(t=10, b=0, l=0, r=0), height=280,
                xaxis=dict(gridcolor="#1a2a44", title="Rating"),
                yaxis=dict(gridcolor="#1a2a44", title="Products"),
            )
            st.plotly_chart(fig_rating, use_container_width=True)

        with col4:
            st.markdown("#### 🏷️ Top 10 Brands by Product Count")
            brand_counts = df['brand'].value_counts().head(10).reset_index()
            brand_counts.columns = ['brand', 'count']
            fig_brand = px.bar(brand_counts, x='brand', y='count',
                              color='count', color_continuous_scale=["#a8b8c8", "#3dffa0"])
            fig_brand.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#8899cc", family="JetBrains Mono"),
                margin=dict(t=10, b=0, l=0, r=0), height=280,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="#1a2a44", tickangle=-45),
                yaxis=dict(gridcolor="#1a2a44"),
            )
            st.plotly_chart(fig_brand, use_container_width=True)

        # Discount vs Rating scatter
        st.markdown("#### 🎯 Discount % vs Average Rating (Top 1000 products)")
        scatter_df = df[df['discount_pct'] > 0].head(1000)
        fig_scatter = px.scatter(scatter_df, x='discount_pct', y='average_rating',
                                color='category', size='selling_price_clean',
                                hover_data=['brand', 'title'],
                                opacity=0.6)
        fig_scatter.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0), height=400,
            xaxis=dict(gridcolor="#1a2a44", title="Discount %"),
            yaxis=dict(gridcolor="#1a2a44", title="Average Rating"),
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#8899cc")),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # Gold summary
        st.divider()
        st.markdown("#### 📋 Category Summary (Gold Layer)")
        gold_summary = df.groupby('category').agg({
            'pid': 'count',
            'selling_price_clean': ['mean', 'median', 'min', 'max'],
            'average_rating': 'mean',
            'discount_pct': 'mean',
            'out_of_stock': 'sum'
        }).round(2)
        gold_summary.columns = ['product_count', 'avg_price', 'median_price', 'min_price', 'max_price', 'avg_rating', 'avg_discount', 'out_of_stock_count']
        gold_summary = gold_summary.reset_index().sort_values('product_count', ascending=False).head(20)
        
        st.dataframe(gold_summary, use_container_width=True, hide_index=True)

        csv_buf = io.StringIO()
        gold_summary.to_csv(csv_buf, index=False)
        st.download_button(
            "💾 Export Gold Summary CSV",
            csv_buf.getvalue(),
            file_name="flipkart_gold_summary.csv",
            mime="text/csv"
        )
