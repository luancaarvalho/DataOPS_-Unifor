import sqlite3
import os
import logging
from datetime import datetime, timezone

logging.basicConfig()
logger = logging.getLogger(__name__)


DB_PATH = "/opt/airflow/data/metrics/metrics.db"


def init_db():
    os.makedirs("/opt/airflow/data/metrics", exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS task_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            dag_id TEXT,
            task_id TEXT,
            run_id TEXT,
            status TEXT,
            start_time TEXT,
            end_time TEXT,
            duration REAL
        );
    """)

    conn.commit()
    conn.close()

    logger.info("Tabela task_metrics verificada/criada com sucesso.")


def log_task_metrics(dag_id, task_id, run_id, status, start_time):
    try:
        init_db()
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
            
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO task_metrics (
                timestamp, dag_id, task_id, run_id, status, start_time, end_time, duration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            dag_id,
            task_id,
            run_id,
            status,
            start_time.isoformat(),
            end_time.isoformat(),
            duration
        ))

        conn.commit()
        conn.close()

        logger.info(
            f"[METRIC] DAG={dag_id} | TASK={task_id} | STATUS={status} | "
            f"DURAÇÃO={duration:.2f}s | RUN_ID={run_id}"
        )

    except Exception as e:
        logger.error(f"Erro ao registrar métricas: {e}")
        raise e
