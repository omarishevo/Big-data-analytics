"""
E-Commerce Data Lake System — Streamlit Dashboard
==================================================
A full-featured data lake simulation with ingestion, cataloging,
querying, lineage tracking, and analytics for an e-commerce platform.
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
    page_title="E-Commerce Data Lake",
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
    background: linear-gradient(135deg, #1e4a9a, #1230 6e);
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

/* ── Layer Zone Cards ── */
.zone-card {
    border-radius: 12px;
    padding: 1rem 1.2rem;
    margin: 0.5rem 0;
    border: 1px solid;
}
.zone-raw {
    background: linear-gradient(135deg, #1a1020, #0e0a1a);
    border-color: #6b3fa0;
}
.zone-bronze {
    background: linear-gradient(135deg, #1a1208, #12100a);
    border-color: #cd7f32;
}
.zone-silver {
    background: linear-gradient(135deg, #101418, #0c1214);
    border-color: #a8b8c8;
}
.zone-gold {
    background: linear-gradient(135deg, #1a1508, #141208);
    border-color: #ffd700;
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


# ─────────────────────────────────────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty", "Toys", "Automotive"]
REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
STATUS_OPTIONS = ["completed", "shipped", "processing", "cancelled", "returned"]
PAYMENT_METHODS = ["Credit Card", "PayPal", "Crypto", "Bank Transfer", "Gift Card"]

@st.cache_data(ttl=300)
def generate_orders(n=500):
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", "2024-12-31", periods=n)
    return pd.DataFrame({
        "order_id": [f"ORD-{i:06d}" for i in range(1, n+1)],
        "customer_id": [f"CUST-{random.randint(1000, 9999)}" for _ in range(n)],
        "product_id": [f"PROD-{random.randint(100, 999)}" for _ in range(n)],
        "category": np.random.choice(CATEGORIES, n),
        "region": np.random.choice(REGIONS, n),
        "quantity": np.random.randint(1, 10, n),
        "unit_price": np.round(np.random.exponential(50, n) + 5, 2),
        "discount_pct": np.round(np.random.choice([0, 5, 10, 15, 20, 25], n, p=[0.4, 0.2, 0.15, 0.12, 0.08, 0.05]), 1),
        "payment_method": np.random.choice(PAYMENT_METHODS, n),
        "status": np.random.choice(STATUS_OPTIONS, n, p=[0.55, 0.2, 0.1, 0.08, 0.07]),
        "order_date": dates,
        "delivery_days": np.random.randint(1, 14, n),
        "customer_rating": np.round(np.random.uniform(2.5, 5.0, n), 1),
        "return_flag": np.random.choice([True, False], n, p=[0.07, 0.93]),
    })

@st.cache_data(ttl=300)
def generate_customers(n=200):
    np.random.seed(99)
    return pd.DataFrame({
        "customer_id": [f"CUST-{i}" for i in range(1000, 1000+n)],
        "name": [f"Customer_{i}" for i in range(n)],
        "email": [f"user{i}@email.com" for i in range(n)],
        "region": np.random.choice(REGIONS, n),
        "signup_date": pd.date_range("2020-01-01", "2023-12-31", periods=n),
        "lifetime_value": np.round(np.random.exponential(500, n) + 50, 2),
        "loyalty_tier": np.random.choice(["Bronze", "Silver", "Gold", "Platinum"], n, p=[0.4, 0.3, 0.2, 0.1]),
        "is_active": np.random.choice([True, False], n, p=[0.85, 0.15]),
        "age_group": np.random.choice(["18-24", "25-34", "35-44", "45-54", "55+"], n),
    })

@st.cache_data(ttl=300)
def generate_products(n=150):
    np.random.seed(77)
    return pd.DataFrame({
        "product_id": [f"PROD-{i}" for i in range(100, 100+n)],
        "product_name": [f"Product_{i}" for i in range(n)],
        "category": np.random.choice(CATEGORIES, n),
        "supplier_id": [f"SUP-{random.randint(10, 99)}" for _ in range(n)],
        "cost_price": np.round(np.random.exponential(20, n) + 3, 2),
        "list_price": np.round(np.random.exponential(50, n) + 8, 2),
        "stock_quantity": np.random.randint(0, 1000, n),
        "weight_kg": np.round(np.random.exponential(1.5, n) + 0.1, 2),
        "is_active": np.random.choice([True, False], n, p=[0.88, 0.12]),
        "launch_date": pd.date_range("2019-01-01", "2023-12-31", periods=n),
    })

@st.cache_data(ttl=300)
def generate_clickstream(n=1000):
    np.random.seed(55)
    return pd.DataFrame({
        "session_id": [f"SESS-{random.randint(10000, 99999)}" for _ in range(n)],
        "user_id": [f"CUST-{random.randint(1000, 1199)}" for _ in range(n)],
        "page": np.random.choice(["home", "product", "cart", "checkout", "search", "account"], n),
        "device": np.random.choice(["mobile", "desktop", "tablet"], n, p=[0.55, 0.35, 0.10]),
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min"),
        "session_duration_sec": np.random.randint(5, 600, n),
        "bounce": np.random.choice([True, False], n, p=[0.35, 0.65]),
        "conversion": np.random.choice([True, False], n, p=[0.12, 0.88]),
    })


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
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
        "tags": [zone.lower(), source.lower(), "ecommerce"],
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
    st.caption("E-Commerce Platform · v2.4.1")

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
    st.markdown("# 🏔️ E-Commerce Data Lake")
    st.markdown("**Enterprise-grade multi-zone data lake with real-time ingestion, cataloging & lineage tracking**")

    # KPI Row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Datasets", total_tables, "+3 today")
    c2.metric("ETL Jobs Run", len(st.session_state.lake["jobs"]), f"+{len(st.session_state.lake['jobs'])} session")
    c3.metric("Catalog Entries", len(st.session_state.lake["catalog"]), "")
    c4.metric("Lineage Events", len(st.session_state.lake["lineage"]), "")
    c5.metric("Lake Health", "98.2%", "+0.4%")

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
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">🛒 Order System</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">👥 CRM / Customers</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">📦 Product Catalog</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">🖱️ Clickstream</div>
            <div style="background:#0a1e3a;border:1px solid #1e4060;border-radius:6px;padding:6px 10px;color:#7ab0ee">📤 CSV / APIs</div>
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Raw Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#9b59b6;font-weight:700;margin-bottom:8px;font-size:0.8rem">🌊 RAW ZONE</div>
          <div style="background:#1a1020;border:1px solid #6b3fa0;border-radius:6px;padding:10px;color:#c89aea;font-size:0.72rem">
            Unprocessed<br>Original format<br>Immutable<br>Full history
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Bronze Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#cd7f32;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥉 BRONZE ZONE</div>
          <div style="background:#1a1208;border:1px solid #cd7f32;border-radius:6px;padding:10px;color:#e8b870;font-size:0.72rem">
            Schema-validated<br>Null removed<br>Type-cast<br>De-duplicated
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Silver Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#a8b8c8;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥈 SILVER ZONE</div>
          <div style="background:#101418;border:1px solid #a8b8c8;border-radius:6px;padding:10px;color:#c8d8e8;font-size:0.72rem">
            Business rules<br>Joined datasets<br>Enriched<br>Standardized
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Gold Zone -->
        <div style="flex:1;min-width:130px">
          <div style="color:#ffd700;font-weight:700;margin-bottom:8px;font-size:0.8rem">🥇 GOLD ZONE</div>
          <div style="background:#1a1508;border:1px solid #ffd700;border-radius:6px;padding:10px;color:#ffe875;font-size:0.72rem">
            Aggregated<br>KPI dashboards<br>ML-ready<br>BI-optimized
          </div>
        </div>

        <!-- Arrow -->
        <div style="display:flex;align-items:center;color:#3a5a9a;font-size:1.2rem">→</div>

        <!-- Consumers -->
        <div style="flex:1;min-width:130px">
          <div style="color:#3dffa0;font-weight:700;margin-bottom:8px;font-size:0.8rem">📊 CONSUMERS</div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">📈 BI Reports</div>
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">🤖 ML Models</div>
            <div style="background:#0a2a1a;border:1px solid #1e6040;border-radius:6px;padding:6px 10px;color:#7af0b0;font-size:0.72rem">🔍 Ad-hoc SQL</div>
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
            st.info("No jobs run yet. Start by ingesting data.")


# ─────────────────────────────────────────────────────────────────────────────
# ── SECTION 2: DATA INGESTION ────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
elif section == "📥 Data Ingestion":
    st.markdown("# 📥 Data Ingestion")
    st.markdown("Load data from built-in generators or upload your own CSV files into the **Raw Zone**.")

    tab1, tab2 = st.tabs(["🎲 SYNTHETIC GENERATORS", "📁 CSV UPLOAD"])

    with tab1:
        st.markdown("### Generate E-Commerce Datasets")
        col1, col2 = st.columns(2)

        datasets = {
            "🛒 Orders": ("orders", generate_orders, "Order transactions with product, pricing & status"),
            "👥 Customers": ("customers", generate_customers, "Customer profiles with LTV & loyalty data"),
            "📦 Products": ("products", generate_products, "Product catalog with cost, pricing & stock"),
            "🖱️ Clickstream": ("clickstream", generate_clickstream, "Web & mobile user behavior events"),
        }

        for i, (label, (key, gen_fn, desc)) in enumerate(datasets.items()):
            col = col1 if i % 2 == 0 else col2
            with col:
                with st.container():
                    st.markdown(f"**{label}**")
                    st.caption(desc)
                    n_rows = st.slider(f"Rows", 100, 2000, 500, 100, key=f"rows_{key}")
                    if st.button(f"⬇️ Ingest {label.split()[1]}", key=f"btn_{key}", use_container_width=True):
                        with st.spinner(f"Ingesting {label}..."):
                            time.sleep(0.4)
                            df = gen_fn(n_rows)
                            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                            dataset_name = f"{key}_{ts}"
                            st.session_state.lake["raw"][dataset_name] = df
                            entry = make_catalog_entry(dataset_name, "RAW", list(df.dtypes.astype(str).items()), len(df), "synthetic-generator")
                            st.session_state.lake["catalog"].append(entry)
                            lineage = make_lineage_event("synthetic-generator", f"raw/{dataset_name}", "INGEST", len(df))
                            st.session_state.lake["lineage"].append(lineage)
                            job = make_job(f"ingest_{key}", "raw", len(df), random.randint(200, 1200))
                            st.session_state.lake["jobs"].append(job)
                            log_entry = {"dataset": dataset_name, "rows": len(df), "time": datetime.datetime.now().isoformat()}
                            st.session_state.lake["ingestion_log"].append(log_entry)
                        st.success(f"✅ Ingested `{dataset_name}` → **{len(df):,} rows** into Raw Zone")

    with tab2:
        st.markdown("### Upload CSV File")
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
                    job = make_job(f"ingest_upload_{dataset_label}", "raw", len(df_upload), random.randint(300, 900))
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
                                time.sleep(0.5)
                                # Simple transformation logic
                                df_t = df.copy()
                                if nz == "bronze":
                                    df_t = df_t.dropna()
                                    df_t = df_t.drop_duplicates()
                                elif nz == "silver":
                                    df_t = df_t.dropna()
                                    if "unit_price" in df_t.columns and "quantity" in df_t.columns:
                                        df_t["revenue"] = df_t["unit_price"] * df_t["quantity"]
                                    if "discount_pct" in df_t.columns and "unit_price" in df_t.columns:
                                        df_t["discounted_price"] = df_t["unit_price"] * (1 - df_t["discount_pct"] / 100)
                                elif nz == "gold":
                                    if "category" in df_t.columns and "revenue" in df_t.columns:
                                        df_t = df_t.groupby("category").agg({"revenue": "sum", "quantity": "sum"}).reset_index()
                                    else:
                                        df_t = df_t.head(50)  # sample

                                new_name = name.replace(f"{zone}_", "") if zone in name else name
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
# ── SECTION 4: QUERY ENGINE ─────────────────────────────────────────────────
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
            st.caption("Use `df` to reference the selected dataset. Example: `df[df['status'] == 'completed'].groupby('category')['unit_price'].mean()`")

            query_presets = {
                "Row count by category": "df.groupby('category').size().reset_index(name='count').sort_values('count', ascending=False)" if "category" in df.columns else "df.head()",
                "Top 10 rows": "df.head(10)",
                "Statistical summary": "df.describe()",
                "Null value counts": "df.isnull().sum().reset_index().rename(columns={0:'nulls','index':'column'})",
                "Value counts (first col)": f"df['{df.columns[0]}'].value_counts().head(20).reset_index()",
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
        st.info("Catalog is empty. Ingest some data to populate it.")
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
                if schema_items:
                    schema_df = pd.DataFrame(schema_items, columns=["Column", "Dtype"])
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)


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
        operations = lineage_df["operation"].value_counts()

        # Build sankey
        all_nodes = list(set(lineage_df["source"].tolist() + lineage_df["destination"].tolist()))
        node_idx = {n: i for i, n in enumerate(all_nodes)}

        sankey_colors = []
        for n in all_nodes:
            if "raw/" in n or n == "synthetic-generator" or "csv" in n.lower():
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
                label=[n[:30] + ("..." if len(n) > 30 else "") for n in all_nodes],
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
        st.warning("⚠️ No raw datasets available. Ingest data first.")
    else:
        st.markdown("### 🚀 Run Full Pipeline")
        selected_raw = st.selectbox("Select Raw Dataset", raw_datasets)
        pipe_cols = st.columns(4)

        steps = [
            ("🌊→🥉 Ingest", "bronze", "Clean & validate"),
            ("🥉→🥈 Transform", "silver", "Enrich & join"),
            ("🥈→🥇 Aggregate", "gold", "Compute KPIs"),
            ("🥇 Publish", "gold", "Expose to BI"),
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
        if st.button("▶▶ RUN FULL PIPELINE", use_container_width=True):
            df_src = st.session_state.lake["raw"][selected_raw]
            ts = datetime.datetime.now().strftime("%H%M%S")
            progress = st.progress(0, "Initializing...")
            status_box = st.empty()

            pipeline_stages = [
                ("Bronze: Clean & Validate", "bronze", lambda d: d.dropna().drop_duplicates()),
                ("Silver: Enrich & Transform", "silver", lambda d: (
                    d.assign(revenue=d["unit_price"] * d["quantity"]) if "unit_price" in d.columns and "quantity" in d.columns else d
                )),
                ("Gold: Aggregate KPIs", "gold", lambda d: (
                    d.groupby("category").agg({"revenue": "sum", "quantity": "sum"}).reset_index()
                    if "category" in d.columns and "revenue" in d.columns else d.describe()
                )),
            ]

            current_df = df_src
            for i, (stage_name, zone, transform_fn) in enumerate(pipeline_stages):
                status_box.info(f"⚙️ Running: **{stage_name}**...")
                time.sleep(0.6)
                try:
                    current_df = transform_fn(current_df)
                except Exception:
                    current_df = current_df.head(100)

                dest_name = f"{zone}_{selected_raw}_{ts}"
                st.session_state.lake[zone][dest_name] = current_df
                entry = make_catalog_entry(dest_name, zone.upper(), list(current_df.dtypes.astype(str).items()), len(current_df), "etl-pipeline")
                st.session_state.lake["catalog"].append(entry)
                lin = make_lineage_event(f"pipeline_input", f"{zone}/{dest_name}", stage_name, len(current_df))
                st.session_state.lake["lineage"].append(lin)
                job = make_job(stage_name, zone, len(current_df), random.randint(400, 3000))
                st.session_state.lake["jobs"].append(job)
                progress.progress((i + 1) / len(pipeline_stages), f"Completed: {stage_name}")

            status_box.success("✅ Full pipeline completed successfully!")
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
    st.markdown("Business intelligence views powered by the Gold Zone.")

    # Auto-load sample data for demo
    if not any(st.session_state.lake["raw"].values()):
        with st.spinner("Loading demo data for analytics..."):
            orders_df = generate_orders(1000)
            customers_df = generate_customers(200)
            products_df = generate_products(150)
            st.session_state.lake["raw"]["demo_orders"] = orders_df
            st.session_state.lake["raw"]["demo_customers"] = customers_df
            st.session_state.lake["raw"]["demo_products"] = products_df

    # Use orders for analytics
    orders_df = generate_orders(1000)
    orders_df["revenue"] = orders_df["unit_price"] * orders_df["quantity"] * (1 - orders_df["discount_pct"] / 100)
    orders_df["order_month"] = pd.to_datetime(orders_df["order_date"]).dt.to_period("M").astype(str)

    # KPIs
    st.markdown("### 🔑 Key Performance Indicators")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Revenue", f"${orders_df['revenue'].sum():,.0f}", "+12.4%")
    k2.metric("Total Orders", f"{len(orders_df):,}", "+8.1%")
    k3.metric("Avg Order Value", f"${orders_df['revenue'].mean():.2f}", "+3.6%")
    k4.metric("Return Rate", f"{orders_df['return_flag'].mean()*100:.1f}%", "-0.5%")
    k5.metric("Avg Rating", f"{orders_df['customer_rating'].mean():.2f} ⭐", "+0.12")

    st.divider()

    # Charts Row 1
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📈 Monthly Revenue Trend")
        monthly = orders_df.groupby("order_month")["revenue"].sum().reset_index()
        fig_rev = px.area(monthly, x="order_month", y="revenue",
                          color_discrete_sequence=["#5ce0ff"])
        fig_rev.update_traces(fill="tozeroy", fillcolor="rgba(92,224,255,0.1)")
        fig_rev.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0), height=270,
            xaxis=dict(gridcolor="#1a2a44", tickangle=-45),
            yaxis=dict(gridcolor="#1a2a44", tickprefix="$"),
        )
        st.plotly_chart(fig_rev, use_container_width=True)

    with col2:
        st.markdown("#### 🏷️ Revenue by Category")
        cat_rev = orders_df.groupby("category")["revenue"].sum().sort_values(ascending=True).reset_index()
        fig_cat = px.bar(cat_rev, x="revenue", y="category", orientation="h",
                         color="revenue", color_continuous_scale=["#1a3a7a", "#5ce0ff"])
        fig_cat.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0), height=270,
            coloraxis_showscale=False,
            xaxis=dict(gridcolor="#1a2a44", tickprefix="$"),
            yaxis=dict(gridcolor="#1a2a44"),
        )
        st.plotly_chart(fig_cat, use_container_width=True)

    # Charts Row 2
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("#### 🌍 Orders by Region")
        region_df = orders_df.groupby("region").size().reset_index(name="orders")
        fig_pie = px.pie(region_df, names="region", values="orders",
                         hole=0.5, color_discrete_sequence=["#5ce0ff", "#a8b8c8", "#ffd700", "#cd7f32", "#3dffa0"])
        fig_pie.update_traces(textfont_color="#d4daf0", textfont_size=11)
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0), height=280,
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#8899cc")),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col4:
        st.markdown("#### 💳 Payment Method Mix")
        pay_df = orders_df.groupby("payment_method")["revenue"].sum().reset_index()
        fig_pay = px.bar(pay_df, x="payment_method", y="revenue",
                         color="payment_method",
                         color_discrete_sequence=["#5ce0ff", "#3dffa0", "#ffd700", "#ff7eb3", "#a8b8c8"])
        fig_pay.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#8899cc", family="JetBrains Mono"),
            margin=dict(t=10, b=0, l=0, r=0), height=280,
            showlegend=False,
            xaxis=dict(gridcolor="#1a2a44"),
            yaxis=dict(gridcolor="#1a2a44", tickprefix="$"),
        )
        st.plotly_chart(fig_pay, use_container_width=True)

    # Heatmap
    st.markdown("#### 🔥 Category × Region Revenue Heatmap")
    heat_df = orders_df.groupby(["category", "region"])["revenue"].sum().unstack(fill_value=0)
    fig_heat = px.imshow(
        heat_df,
        color_continuous_scale=["#0a0e1a", "#1a3a7a", "#5ce0ff"],
        text_auto=".0f",
        aspect="auto",
    )
    fig_heat.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#8899cc", family="JetBrains Mono"),
        margin=dict(t=10, b=0, l=0, r=0), height=340,
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # Order Status Funnel
    st.markdown("#### 🔽 Order Status Distribution")
    status_df = orders_df["status"].value_counts().reset_index()
    status_df.columns = ["status", "count"]
    fig_funnel = go.Figure(go.Funnel(
        y=status_df["status"],
        x=status_df["count"],
        marker_color=["#5ce0ff", "#3dffa0", "#ffd700", "#ff7eb3", "#ffb83d"],
        textinfo="value+percent total",
    ))
    fig_funnel.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#8899cc", family="JetBrains Mono"),
        margin=dict(t=10, b=0, l=0, r=0), height=280,
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

    # Downloadable summary
    st.divider()
    summary = orders_df.groupby("category").agg(
        revenue=("revenue", "sum"),
        orders=("order_id", "count"),
        avg_rating=("customer_rating", "mean"),
        return_rate=("return_flag", "mean"),
    ).round(2).reset_index()

    st.markdown("#### 📋 Category Summary (Gold Layer)")
    st.dataframe(summary, use_container_width=True, hide_index=True)

    csv_buf = io.StringIO()
    summary.to_csv(csv_buf, index=False)
    st.download_button(
        "💾 Export Gold Summary CSV",
        csv_buf.getvalue(),
        file_name="ecommerce_gold_summary.csv",
        mime="text/csv"
    )
