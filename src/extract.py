"""
extract.py
----------
Step 1 of the ETL pipeline: pull raw weather + air quality data
for London from the free Open-Meteo API (no API key required).

Two separate Open-Meteo services are used because weather and air
quality live on different subdomains:
  - https://api.open-meteo.com/v1/forecast          -> temperature
  - https://air-quality-api.open-meteo.com/v1/air-quality -> pm2.5 / pm10
"""

import os
import requests

# London coordinates, overridable via environment variables so the same
# code can be reused for any other city without touching a single line.
LATITUDE = float(os.environ.get("CITY_LAT", 51.5074))
LONGITUDE = float(os.environ.get("CITY_LON", -0.1278))

WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

REQUEST_TIMEOUT_SECONDS = 15


def extract_weather_data() -> dict:
    """
    Calls both Open-Meteo endpoints and returns a single merged dict:

        {
          "current": {...},
          "hourly": {
              "time": [...],
              "temperature_2m": [...],
              "pm2_5": [...],
              "pm10": [...],
          }
        }

    Raises requests.exceptions.RequestException if either HTTP call fails
    (bad network, bad params, Open-Meteo downtime, etc). We deliberately do
    NOT swallow the exception here -- the caller (the Airflow task) decides
    what to do with a failed extraction, e.g. retry.
    """
    weather_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current": "temperature_2m",
        "hourly": "temperature_2m",
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    aqi_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "pm10,pm2_5",
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "UTC",
    }

    weather_response = requests.get(
        WEATHER_API_URL, params=weather_params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    weather_response.raise_for_status()
    weather_json = weather_response.json()

    aqi_response = requests.get(
        AIR_QUALITY_API_URL, params=aqi_params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    aqi_response.raise_for_status()
    aqi_json = aqi_response.json()

    return {
        "current": weather_json["current"],
        "hourly": {
            "time": weather_json["hourly"]["time"],
            "temperature_2m": weather_json["hourly"]["temperature_2m"],
            "pm2_5": aqi_json["hourly"]["pm2_5"],
            "pm10": aqi_json["hourly"]["pm10"],
        },
    }


if __name__ == "__main__":
    # Lets you sanity-check the extractor on its own:
    #   python -m src.extract
    print("Running extract.py as a standalone script...")
    try:
        payload = extract_weather_data()
        print("Success. Top-level keys:", list(payload.keys()))
        print("Hourly rows fetched:", len(payload["hourly"]["time"]))
    except Exception as error:
        print(f"Extraction failed: {error}")
