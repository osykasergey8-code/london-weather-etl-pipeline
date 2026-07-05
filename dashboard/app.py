import os
import sqlite3

import pandas as pd
import streamlit as st

st.set_page_config(page_title="London Air Quality", page_icon="🌍", layout="wide")
st.title("🌍 London Weather & Air Quality Dashboard")

# Same env var used by src/load.py -- see docker-compose.yml for how the
# two containers are pointed at the same underlying data/weather.db file.
DB_PATH = os.environ.get("DB_PATH", "data/weather.db")

if not os.path.exists(DB_PATH):
    st.error(
        f"Database not found at '{DB_PATH}'.\n\n"
        "Run the ETL pipeline first: `python scripts/run_pipeline_locally.py` "
        "or trigger the Airflow DAG."
    )
    st.stop()

conn = sqlite3.connect(DB_PATH)
try:
    df = pd.read_sql_query(
        "SELECT * FROM air_quality ORDER BY timestamp DESC LIMIT 100", conn
    )
finally:
    conn.close()

if df.empty:
    st.warning("The database exists but has no rows yet. Run the pipeline at least once.")
    st.stop()

df["timestamp"] = pd.to_datetime(df["timestamp"])
df_sorted = df.sort_values("timestamp")

col1, col2 = st.columns(2)
col1.metric("Current Temperature", f"{df_sorted['current_temp'].iloc[-1]:.1f} °C")
col2.metric("PM2.5", f"{df_sorted['pm2_5'].iloc[-1]:.2f} µg/m³")

st.subheader("PM2.5 over time")
st.line_chart(df_sorted.set_index("timestamp")["pm2_5"])

st.subheader("Raw data")
st.dataframe(df, use_container_width=True)
