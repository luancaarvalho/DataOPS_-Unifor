from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import duckdb as db
import subprocess
import pandas as pd
from pathlib import Path

#configurar dag
default_args = {
    "owner": "maria",
    "depends_on_past": False,

    "start_date": datetime(2025, 11, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="pipeline_clima",
    default_args=default_args,
    schedule="@daily",  # substitui schedule_interval
    catchup=False,
    description="Pipeline de clima completo",
)

#configurar o diretório
BASE_DIR = "/opt/airflow"
DATA_DIR = os.path.join(BASE_DIR, "data")
TESTS_DIR = os.path.join(BASE_DIR, "tests")
MONITORING_DIR = os.path.join(BASE_DIR, "monitoring")

# função para executar os meus scripts
def run_script(script_path):
    try:
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_path}: {e}")
        raise

# salvar as métricas do aiflow em um arquivo diickdb
def salvar_metricas():
    from airflow.models import TaskInstance, DagRun
    from airflow.utils.session import create_session
    from sqlalchemy import and_

    db_path = os.path.join(DATA_DIR, "metricas_airflow.duckdb")
    con = db.connect(db_path)

    with create_session() as session:
        dagruns = session.query(DagRun).filter(DagRun.dag_id == "pipeline_clima").order_by(DagRun.execution_date.desc()).limit(1)
        metrics = []

        for dr in dagruns:
            tis = session.query(TaskInstance).filter(
                TaskInstance.dag_id == "pipeline_clima",
                TaskInstance.execution_date == dr.execution_date
            )
            for ti in tis:
                metrics.append({
                    "dag_id": ti.dag_id,
                    "task_id": ti.task_id,
                    "execution_date": ti.execution_date,
                    "start_date": ti.start_date,
                    "end_date": ti.end_date,
                    "duration": ti.duration,
                    "state": ti.state,
                })

    if metrics:
        df_metrics = pd.DataFrame(metrics)
        con.execute("""
            CREATE TABLE IF NOT EXISTS metricas_airflow (
                dag_id TEXT,
                task_id TEXT,
                execution_date TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                duration DOUBLE,
                state TEXT
            )
        """)
        con.register("df_metrics", df_metrics)
        con.execute("INSERT INTO metricas_airflow SELECT * FROM df_metrics")

    con.close()


# executar os scripts na ordem
scripts = {
    "coleta": [os.path.join(DATA_DIR, "weather_api.py")],
    "bronze": [
        os.path.join(TESTS_DIR, "criar_df.py"),
        os.path.join(TESTS_DIR, "extrair_bronze.py"),
    ],
    "prata": [
        os.path.join(TESTS_DIR, "validacao_prata_gx.py"),
        os.path.join(TESTS_DIR, "extrair_prata.py"),
    ],
    "ouro": [os.path.join(TESTS_DIR, "extrair_ouro.py")],
    "monitoramento": [os.path.join(MONITORING_DIR, "weather_dashboard.py")],
}

# criar tasks
task_groups = {}
previous_task = None

for grupo, lista_scripts in scripts.items():
    with TaskGroup(group_id=f"{grupo}_stage", tooltip=f"Etapa {grupo}", dag=dag) as tg:
        tasks = []
        for script_path in lista_scripts:
            script_name = Path(script_path).stem.replace(" ", "_").lower()
            task = PythonOperator(
                task_id=f"run_{script_name}",
                python_callable=run_script,
                op_args=[script_path],
                dag=dag,
            )
            tasks.append(task)

        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

        task_groups[grupo] = tg

        if previous_task:
            previous_task >> tg
        previous_task = tg

#salvar as métricas
task_metricas = PythonOperator(
    task_id="salvar_metricas",
    python_callable=salvar_metricas,
    dag=dag,
)

#última etapa
previous_task >> task_metricas