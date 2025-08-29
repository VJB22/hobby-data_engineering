# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 19:36:09 2025

@author: baroc
"""

from pathlib import Path
import duckdb
import streamlit as st

# Use the DuckDB file inside your repo: data/lakehouse.duckdb
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "lakehouse.duckdb"

st.set_page_config(page_title="Sales KPIs", layout="wide")
st.title("KPIs")

# Guard: DB must exist (run the pipeline first)
if not DB_PATH.exists():
    st.warning(f"Database not found at: {DB_PATH}\nRun your pipeline first: `python src/pipeline.py`.")
    st.stop()

con = duckdb.connect(str(DB_PATH), read_only=True)

# ---- Helpers ----
def table_exists(schema_table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {schema_table} LIMIT 1")
        return True
    except duckdb.CatalogException:
        return False

# Ensure gold table exists
if not table_exists("gold.sales_kpi"):
    st.warning("Table `gold.sales_kpi` not found. Did you run the step that builds it?")
    st.stop()

# ---- Filters ----
regions = con.execute(
    "SELECT DISTINCT region FROM gold.sales_kpi ORDER BY 1"
).fetchdf()["region"].tolist()

if not regions:
    st.warning("No regions found in `gold.sales_kpi`.")
    st.stop()

sel_regions = st.multiselect("Region filter", regions, default=regions)

# KPI cards (use UNNEST to pass list safely)
kpi = con.execute(
    """
    SELECT 
      SUM(total_sales) AS total_sales,
      SUM(order_count) AS orders,
      AVG(total_sales) AS avg_sales_per_region
    FROM gold.sales_kpi
    WHERE region IN (SELECT * FROM UNNEST(?))
    """,
    [sel_regions],
).fetchdf().iloc[0]

c1, c2, c3 = st.columns(3)
c1.metric("Total Sales", f"{(kpi['total_sales'] or 0):.2f}")
c2.metric("Orders", int(kpi["orders"] or 0))
c3.metric("Avg Sales / Region", f"{(kpi['avg_sales_per_region'] or 0):.2f}")

# Bar chart by region
df_bar = con.execute(
    """
    SELECT region, total_sales, order_count
    FROM gold.sales_kpi
    WHERE region IN (SELECT * FROM UNNEST(?))
    ORDER BY region
    """,
    [sel_regions],
).fetchdf()

st.subheader("Sales by Region")
if not df_bar.empty:
    st.bar_chart(df_bar.set_index("region")["total_sales"])
else:
    st.info("No rows match the selected regions.")

# Optional trend from silver if available
if table_exists("silver.sales_clean"):
    cols = [r[1] for r in con.execute("PRAGMA table_info('silver.sales_clean')").fetchall()]
    if "order_ts" in cols:
        st.subheader("Sales Trend (if timestamps available)")
        trend = con.execute(
            """
            WITH s AS (
              SELECT CAST(order_ts AS DATE) AS d, TRY_CAST(priceeach AS DOUBLE) AS amt
              FROM silver.sales_clean
            )
            SELECT d, SUM(amt) AS daily_sales
            FROM s
            WHERE d IS NOT NULL AND amt IS NOT NULL
            GROUP BY d
            ORDER BY d
            """
        ).fetchdf()
        if not trend.empty:
            st.line_chart(trend.set_index("d")["daily_sales"])
        else:
            st.info("No time-series data available yet.")

con.close()
