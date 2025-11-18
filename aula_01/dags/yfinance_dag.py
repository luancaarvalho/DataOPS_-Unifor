
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import yfinance as yf
import pandas as pd

def _fetch_yfinance_data_and_create_csv(ticker, date, **kwargs):
    """
    Fetches daily stock data for a given ticker from Yahoo Finance and saves it to a CSV file.
    """

    date_str = "{{ ds }}"

    print("----------------------------------------------")
    print(date_str)
    print("----------------------------------------------")

    # yfinance downloads data up to the day before the end date
    end_date = pd.to_datetime(date) + pd.DateOffset(days=1)
    data = yf.download(ticker, start=date, end=end_date.strftime('%Y-%m-%d'))

    if not data.empty:
        data.to_csv(f"/opt/airflow/dags/{ticker}_{date}.csv")
    else:
        print(f"No data found for {ticker} on {date}")

with DAG(
    dag_id="yfinance_petrobras_dag",
    start_date=pendulum.datetime(2025, 9, 20, tz="UTC"),
    catchup=True,
    schedule="@daily",
    tags=["dataops", "unifor"],
    params={
        "ticker": "PETR4.SA",
    },
    max_active_runs=1,
) as dag:
    
    date_str = "{{ ds }}"

    print("----------------------------------------------")
    print(date_str)
    print("----------------------------------------------")


    fetch_yfinance_data_task = PythonOperator(
        task_id="fetch_yfinance_data_and_create_csv",
        python_callable=_fetch_yfinance_data_and_create_csv,
        op_kwargs={"ticker": dag.params["ticker"], "date": date_str},
    )
