from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

from src.db import get_engine

load_dotenv()

SCHEMA_PATH = Path("sql/schema.sql")

UPSERT_SQL = """
INSERT INTO silver_hourly_weather (
  city, latitude, longitude, timezone, time,
  temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
)
VALUES (
  :city, :latitude, :longitude, :timezone, :time,
  :temperature_2m, :relative_humidity_2m, :precipitation, :wind_speed_10m
)
ON CONFLICT (city, time) DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  timezone = EXCLUDED.timezone,
  temperature_2m = EXCLUDED.temperature_2m,
  relative_humidity_2m = EXCLUDED.relative_humidity_2m,
  precipitation = EXCLUDED.precipitation,
  wind_speed_10m = EXCLUDED.wind_speed_10m;
"""

def ensure_schema():
    engine = get_engine()
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(ddl))

def upsert_silver(df: pd.DataFrame):
    # Keep only the columns we expect
    cols = [
        "city", "latitude", "longitude", "timezone", "time",
        "temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"
    ]
    df2 = df.copy()
    df2 = df2[[c for c in cols if c in df2.columns]]

    # Convert time to Python datetime objects for psycopg
    df2["time"] = pd.to_datetime(df2["time"]).dt.to_pydatetime()

    records = df2.to_dict(orient="records")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL), records)

if __name__ == "__main__":
    # Example wiring: load the latest bronze, transform to df, then upsert
    from src.transform import load_latest_bronze_snapshot, hourly_to_dataframe

    city = "Sunnyvale"
    bronze = load_latest_bronze_snapshot(city)
    df = hourly_to_dataframe(bronze)

    ensure_schema()
    upsert_silver(df)
    print(f"Upserted {len(df)} rows into silver_hourly_weather")
