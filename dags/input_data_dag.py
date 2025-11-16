from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests as req
import boto3
from botocore.client import Config

def input_data(**kwargs):
    last_month_day = {
        "01": 31,
        "02": 28,
        "03": 31,
        "04": 30,
        "05": 31,
        "06": 30,
        "07": 31,
        "08": 31,
        "09": 30,
        "10": 31,
        "11": 30,
        "12": 31,
    }
    execution_date = kwargs['ds']
    month = execution_date[5:7]
    year = execution_date[:4]
    day = last_month_day[month]

    start_date = f'01/{month}/{year}'
    final_date = f'{day}/{month}/{year}'
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=csv&dataInicial={start_date}&dataFinal={final_date}"

    response = req.get(url)
    response.raise_for_status()

    csv_bytes = response.content

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    bucket_name = 'bronze'
    object_name = f'{execution_date}.csv'

    s3.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=csv_bytes,
        ContentType='text/csv'
    )


with DAG(
        dag_id='input_data_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@monthly',
        catchup=True
     ) as dag:
        input_data = PythonOperator(
            task_id='input_data',
            python_callable=input_data
        )