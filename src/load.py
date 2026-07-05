"""
load.py
-------
Step 3 of the ETL pipeline: persist the clean DataFrame into SQLite.

DB_PATH is read from the DB_PATH environment variable so the exact same
code works in three different contexts without any edits:
  1. Local run from the project root        -> defaults to "data/weather.db"
  2. Inside the Airflow container            -> set to /opt/airflow/data/weather.db
  3. Inside the Streamlit dashboard container -> set to /app/data/weather.db
All three point at the same host folder (./data) once docker-compose
mounts it into both containers -- see docker-compose.yml.
"""

import os
import sqlite3
import pandas as pd

DEFAULT_DB_PATH = os.environ.get("DB_PATH", "data/weather.db")


def load_to_db(df: pd.DataFrame, db_path: str = DEFAULT_DB_PATH) -> int:
    """
    Appends df to the air_quality table, creating the table and any
    missing parent directories automatically. Returns the number of
    rows written so callers can log it.
    """
    parent_dir = os.path.dirname(db_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        df.to_sql("air_quality", connection, if_exists="append", index=False)

    return len(df)


if __name__ == "__main__":
    print("load.py is a library module.")
    print("Import load_to_db(df) from it instead of running this file directly.")
