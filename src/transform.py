from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")  # optional: useful for debugging / local artifacts
SILVER_DIR.mkdir(parents=True, exist_ok=True)


def load_latest_bronze_snapshot(city: str) -> dict:
    """
    Load the most recent bronze snapshot JSON for a given city (based on filename timestamp).
    """
    safe_city = city.lower().replace(" ", "_")
    files = sorted(BRONZE_DIR.glob(f"{safe_city}_*.json"))
    if not files:
        raise FileNotFoundError(
            f"No bronze snapshots found for city='{city}'. Expected files like {safe_city}_*.json in {BRONZE_DIR}"
        )

    latest = files[-1]
    payload = json.loads(latest.read_text(encoding="utf-8"))
    payload["_bronze_path"] = str(latest)
    return payload


def hourly_to_dataframe(bronze_payload: dict) -> pd.DataFrame:
    """
    Convert a bronze snapshot payload into a clean hourly DataFrame.

    Output columns (example):
      - city, latitude, longitude, timezone
      - time (datetime64[ns])
      - temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
    """
    resolved = bronze_payload["resolved_location"]
    data = bronze_payload["data"]

    hourly = data.get("hourly")
    if not hourly:
        raise ValueError("Bronze payload is missing 'data.hourly'")

    # Build a table from the "column arrays" structure Open-Meteo uses.
    df = pd.DataFrame(hourly)

    # Standardize / enrich
    df.insert(0, "city", resolved.get("name"))
    df.insert(1, "latitude", float(resolved["latitude"]))
    df.insert(2, "longitude", float(resolved["longitude"]))
    df.insert(3, "timezone", data.get("timezone"))

    # Parse time column to pandas datetime
    if "time" not in df.columns:
        raise ValueError("Hourly data missing 'time' column")

    df["time"] = pd.to_datetime(df["time"])

    # Optional: enforce numeric types for expected measures (safe coercion)
    for col in ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop duplicate timestamps for same city (defensive)
    df = df.drop_duplicates(subset=["city", "time"]).reset_index(drop=True)

    return df


def save_silver_parquet(df: pd.DataFrame, city: str) -> Path:
    """
    Optional: save a local silver artifact to inspect results.
    """
    safe_city = city.lower().replace(" ", "_")
    out = SILVER_DIR / f"{safe_city}_hourly.parquet"
    df.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    city = "Sunnyvale"
    bronze = load_latest_bronze_snapshot(city)
    df = hourly_to_dataframe(bronze)

    print(df.head(5))
    print(f"\nRows: {len(df)}  Columns: {list(df.columns)}")
    out = save_silver_parquet(df, city)
    print(f"\nSaved silver parquet: {out}")
