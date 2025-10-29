
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import requests
import pandas as pd

def _fetch_weather_data_and_create_csv(date, **kwargs):
    """
    Fetches daily historical weather data for Fortaleza from the Open-Meteo Archive API and saves it to a CSV file.
    """
    latitude = -3.73
    longitude = -38.53
    # Use archive API for historical data with multiple weather variables
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={date}&end_date={date}&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,windspeed_10m_max,winddirection_10m_dominant&timezone=America/Fortaleza"
    
    response = requests.get(url)
    data = response.json()

    if "daily" in data and data["daily"]:
        df = pd.DataFrame(data["daily"])
        df.to_csv(f"/opt/airflow/dags/weather_fortaleza_{date}.csv", index=False)
        print(f"Weather data saved for {date}: {len(df)} records")
    else:
        error_msg = data.get("reason", "Unknown error")
        print(f"No weather data found for {date}. Error: {error_msg}")

with DAG(
    dag_id="weather_fortaleza_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=True,
    schedule="@daily",
    tags=["dataops", "unifor"],
) as dag:
    
    date_str = "{{ ds }}"

    fetch_weather_data_task = PythonOperator(
        task_id="fetch_weather_data_and_create_csv",
        python_callable=_fetch_weather_data_and_create_csv,
        op_kwargs={"date": date_str},
    )
