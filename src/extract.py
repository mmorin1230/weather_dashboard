from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

BRONZE_DIR = Path("data/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

@dataclass(frozen=True)
class Location:
    name: str
    country: str | None
    latitude: float
    longitude: float
    timezone: str | None

def geocode_city(city: str, count: int = 1) -> Location:
    url = "https://geocoding-api.open-meteo.com/v1/search"
    r = requests.get(url, params={"name": city, "count": count, "language": "en", "format": "json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    if not results:
        raise ValueError(f"No geocoding results for city={city!r}")

    top = results[0]
    return Location(
        name=top.get("name", city),
        country=top.get("country"),
        latitude=float(top["latitude"]),
        longitude=float(top["longitude"]),
        timezone=top.get("timezone"),
    )

def fetch_forecast(loc: Location) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": loc.latitude,
        "longitude": loc.longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "auto",
        "forecast_days": 7,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def save_bronze_snapshot(city: str) -> Path:
    loc = geocode_city(city)
    payload = {
        "requested_city": city,
        "resolved_location": loc.__dict__,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "data": fetch_forecast(loc),
    }

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_city = city.lower().replace(" ", "_")
    out = BRONZE_DIR / f"{safe_city}_{ts}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out

if __name__ == "__main__":
    path = save_bronze_snapshot("Sunnyvale")
    print(f"Saved bronze snapshot: {path}")
