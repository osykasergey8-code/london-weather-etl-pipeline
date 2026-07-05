"""
scripts/run_pipeline_locally.py
--------------------------------
Runs extract -> transform -> load exactly once, without needing a full
Airflow install. Use this to sanity-check the pipeline on your machine
before trusting the Airflow container with it.

Usage (from the project root):
    python scripts/run_pipeline_locally.py
"""

import os
import sys

# Make "src" importable when this script is run directly from scripts/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract import extract_weather_data
from src.transform import transform_data
from src.load import load_to_db


def main():
    print("STEP 1/3: extracting data from Open-Meteo...")
    raw_data = extract_weather_data()
    print(f"  -> got {len(raw_data['hourly']['time'])} hourly rows")

    print("STEP 2/3: transforming into a DataFrame...")
    df = transform_data(raw_data)
    print(f"  -> {len(df)} rows survived cleaning")

    print("STEP 3/3: loading into SQLite...")
    rows_written = load_to_db(df)
    print(f"  -> wrote {rows_written} rows")

    print("Done. Run: streamlit run dashboard/app.py")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Pipeline failed: {error}")
        raise
