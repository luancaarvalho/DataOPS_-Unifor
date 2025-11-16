from datetime import datetime 
import os
from airflow.decorators import task # type: ignore
from airflow.operators.python import get_current_context # type: ignore
import duckdb
import pandas as pd

@task 
def store_data(df, table_name):
    print(f"🚀 Iniciando armazenamento de dados na tabela: {table_name}")
    context = get_current_context()
    logical_date = context["logical_date"]

    pd_df = pd.DataFrame(df)

    path = "/opt/airflow/data/"
    path = os.path.join(path, logical_date.strftime("%Y-%m-%d_%H-%M-%S"),"db.duckdb")

    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    with duckdb.connect(path) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM pd_df")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM pd_df")
        
    print(f"✅ Dados armazenados na tabela: {table_name} em {path}")