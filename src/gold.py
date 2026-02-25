from sqlalchemy import text
from src.db import get_engine


def ensure_gold_schema():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold_daily_weather (
          city TEXT NOT NULL,
          date DATE NOT NULL,
          avg_temperature_2m DOUBLE PRECISION,
          total_precipitation DOUBLE PRECISION,
          max_wind_speed_10m DOUBLE PRECISION,
          hours_count INTEGER NOT NULL,
          computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (city, date)
        );
        """))


def upsert_gold():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO gold_daily_weather (
          city, date,
          avg_temperature_2m,
          total_precipitation,
          max_wind_speed_10m,
          hours_count
        )
        SELECT
          city,
          (time AT TIME ZONE timezone)::date AS date,
          AVG(temperature_2m),
          SUM(precipitation),
          MAX(wind_speed_10m),
          COUNT(*)
        FROM silver_hourly_weather
        GROUP BY city, (time AT TIME ZONE timezone)::date
        ON CONFLICT (city, date) DO UPDATE SET
          avg_temperature_2m = EXCLUDED.avg_temperature_2m,
          total_precipitation = EXCLUDED.total_precipitation,
          max_wind_speed_10m = EXCLUDED.max_wind_speed_10m,
          hours_count = EXCLUDED.hours_count,
          computed_at = NOW();
        """))