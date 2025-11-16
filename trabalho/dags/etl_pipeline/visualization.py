from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.decorators import task # type: ignore
from datetime import datetime
from airflow.operators.python import get_current_context # type: ignore
import requests
import os

SUPERSET_URL = "http://superset:8088"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"
DUCKDB_PATH = "/app/data/db.duckdb"
DATASET_NAME = "sales"

def get_superset_tokens():
    session = requests.Session()

    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    jwt = resp.json()["access_token"]

    csrf_resp = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {jwt}"}
    )
    csrf_resp.raise_for_status()
    csrf_token = csrf_resp.json()["result"]

    return session, jwt, csrf_token

def register_database(session, jwt, csrf_token):
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "X-CSRFToken": csrf_token,
    }

    context = get_current_context()
    logical_date = context["logical_date"]
    path = "/var/data/"
    path = os.path.join(path, logical_date.strftime("%Y-%m-%d_%H-%M-%S"),"db.duckdb")

    payload = {
        "database_name": f"meme_coins_{logical_date}",
        "sqlalchemy_uri": f"duckdb:///{path}",
        "expose_in_sqllab": True,
        "extra":"{\"allows_virtual_table_explore\":true}",
        "engine_information":{"supports_file_upload": True},
        "configuration_method":"sqlalchemy_form"
    }
    
    resp = session.post(f"{SUPERSET_URL}/api/v1/database/", headers=headers, json=payload)
    resp.raise_for_status()

def update_dataset(session, jwt, csrf_token, dataset_name):
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
        "X-CSRFToken": csrf_token,
    }

    context = get_current_context()
    logical_date = context["logical_date"]
    database_name = f"meme_coins_{logical_date}"


    resp = session.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
    resp.raise_for_status()
    databases = resp.json()["result"]
    database = next(
        (db for db in databases if db.get("database_name") == database_name),
        None,
    )

    if not database:
        raise ValueError(f"Database '{database_name}' not found in Superset")

    resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers)
    resp.raise_for_status()
    datasets = resp.json().get("result", [])

    dataset = next(
        (d for d in datasets if d.get("table_name") == dataset_name or d.get("name") == dataset_name),
        None,
    )

    if not dataset:
        raise ValueError(f"Dataset '{dataset_name}' not found in Superset")

    dataset_id = dataset["id"]

    dataset["database_id"] = database["id"]

    payload = {
        "database_id": database["id"]
    }

    resp = session.put(
        f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}",
        headers=headers,
        json=payload
    )  

    resp.raise_for_status()

@task 
def send_data_to_superset(prev_task):
    print("🚀 Enviando dados para o Superset")
    session, jwt, csrf_token = get_superset_tokens()
    register_database(session, jwt, csrf_token)

    update_dataset(session, jwt, csrf_token, "meme_coins")
    print("✅ Dados enviados para o Superset com sucesso")

