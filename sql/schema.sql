CREATE TABLE IF NOT EXISTS silver_hourly_weather (
  city TEXT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT,
  time TIMESTAMPTZ NOT NULL,

  temperature_2m DOUBLE PRECISION,
  relative_humidity_2m DOUBLE PRECISION,
  precipitation DOUBLE PRECISION,
  wind_speed_10m DOUBLE PRECISION,

  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT silver_hourly_weather_pk PRIMARY KEY (city, time)
);

CREATE INDEX IF NOT EXISTS idx_silver_hourly_weather_time
  ON silver_hourly_weather (time);
