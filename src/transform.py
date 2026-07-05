"""
transform.py
------------
Step 2 of the ETL pipeline: turn the raw dict from extract.py into a
clean, typed Pandas DataFrame ready to be written to SQLite.
"""

from datetime import datetime, timezone
import pandas as pd


def transform_data(raw_data: dict) -> pd.DataFrame:
    """
    raw_data must look like:
        {"hourly": {"time": [...], "temperature_2m": [...], "pm2_5": [...], "pm10": [...]}}

    Returns a DataFrame with columns:
        timestamp, pm2_5, pm10, current_temp, extracted_at
    Rows with any missing value are dropped (dropna) so bad API rows
    never reach the database.
    """
    hourly = raw_data["hourly"]

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hourly["time"]),
            "pm2_5": pd.to_numeric(hourly["pm2_5"], errors="coerce"),
            "pm10": pd.to_numeric(hourly["pm10"], errors="coerce"),
            "current_temp": pd.to_numeric(hourly["temperature_2m"], errors="coerce"),
        }
    )

    df["extracted_at"] = datetime.now(timezone.utc).isoformat()
    df = df.dropna().reset_index(drop=True)
    return df


if __name__ == "__main__":
    print("transform.py is a library module.")
    print("Import transform_data(raw_data) from it instead of running this file directly.")
