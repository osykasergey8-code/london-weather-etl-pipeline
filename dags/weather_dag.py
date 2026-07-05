"""
weather_dag.py
--------------
Airflow DAG definition. This file only ORCHESTRATES; the actual work
(extract / transform / load) lives in src/, so it can be unit-tested
without spinning up Airflow at all.

Runs hourly. Each task hands its output to the next one via XCom:
  extract -> raw dict
  transform -> list of clean records
  load -> writes to SQLite, returns row count
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_weather_data
from src.transform import transform_data
from src.load import load_to_db

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _extract_task(**context):
    raw_data = extract_weather_data()
    context["ti"].xcom_push(key="raw_data", value=raw_data)


def _transform_task(**context):
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = transform_data(raw_data)
    context["ti"].xcom_push(key="clean_records", value=df.to_dict(orient="records"))


def _load_task(**context):
    import pandas as pd  # local import keeps Airflow's DAG-parsing step light

    records = context["ti"].xcom_pull(key="clean_records", task_ids="transform")
    df = pd.DataFrame(records)
    rows_written = load_to_db(df)
    print(f"Loaded {rows_written} rows into the database.")


with DAG(
    dag_id="weather_aqi_pipeline",
    description="Hourly ETL: fetch London weather & air quality from Open-Meteo, clean it, store in SQLite.",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "portfolio"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract_task)
    transform = PythonOperator(task_id="transform", python_callable=_transform_task)
    load = PythonOperator(task_id="load", python_callable=_load_task)

    extract >> transform >> load
