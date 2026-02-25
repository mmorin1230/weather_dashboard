import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Weather Dashboard", layout="wide")

@st.cache_resource
def get_engine():
    db_url = os.environ["DATABASE_URL"]
    return create_engine(db_url, future=True)

engine = get_engine()

st.title("Weather Dashboard")

# --- Controls
with st.sidebar:
    st.header("Filters")

    cities = pd.read_sql("SELECT DISTINCT city FROM gold_daily_weather ORDER BY city", engine)["city"].tolist()
    city = st.selectbox("City", cities)

    date_bounds = pd.read_sql(
        text("""
            SELECT MIN(date) AS min_date, MAX(date) AS max_date
            FROM gold_daily_weather
            WHERE city = :city
        """),
        engine,
        params={"city": city},
    ).iloc[0]

    min_date = date_bounds["min_date"]
    max_date = date_bounds["max_date"]

    start_date, end_date = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

# --- Load data
df = pd.read_sql(
    text("""
        SELECT date, avg_temperature_2m, total_precipitation, max_wind_speed_10m, hours_count
        FROM gold_daily_weather
        WHERE city = :city
          AND date BETWEEN :start_date AND :end_date
        ORDER BY date
    """),
    engine,
    params={"city": city, "start_date": start_date, "end_date": end_date},
)

# --- Layout
c1, c2, c3 = st.columns(3)
c1.metric("Days", len(df))
c2.metric("Avg temp (mean)", f"{df['avg_temperature_2m'].mean():.2f} °C" if len(df) else "—")
c3.metric("Total precip (sum)", f"{df['total_precipitation'].sum():.2f} mm" if len(df) else "—")

st.subheader("Daily average temperature (°C)")
st.line_chart(df.set_index("date")["avg_temperature_2m"])

st.subheader("Daily total precipitation (mm)")
st.bar_chart(df.set_index("date")["total_precipitation"])

st.subheader("Daily max wind speed (units from API)")
st.line_chart(df.set_index("date")["max_wind_speed_10m"])

with st.expander("Raw gold table rows"):
    st.dataframe(df, use_container_width=True)