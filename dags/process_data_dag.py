import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from botocore.client import Config

MYSQL_USER = "root"
MYSQL_PASSWORD = "rootpassword"
MYSQL_HOST = "mysql"
MYSQL_DB = "selic_data"
MYSQL_PORT = 3306

def process_data():

    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    response = s3.list_objects_v2(Bucket='bronze')

    if "Contents" not in response:
        print("Nenhum arquivo encontrado no bucket/pasta especificado.")
    else:
        for obj in response["Contents"]:
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            print(f"Lendo arquivo: {key}")

            csv_obj = s3.get_object(Bucket='bronze', Key=key)
            csv_data = csv_obj["Body"].read().decode("utf-8")


            df = pd.read_csv(StringIO(csv_data), sep=";")

            df.to_sql('data', con=engine, if_exists="append", index=False)


            # s3.delete_object(Bucket='bronze', Key=key)

with DAG(
        dag_id='process_data_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@monthly',
        catchup=False
     ) as dag:
        input_data = PythonOperator(
            task_id='process_data',
            python_callable=process_data
        )