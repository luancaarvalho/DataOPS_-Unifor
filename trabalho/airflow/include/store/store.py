import duckdb
import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)

def store_data(processed_path):
    os.makedirs("/opt/airflow/data/db", exist_ok=True)
    con = duckdb.connect("/opt/airflow/data/db/superset.duckdb")

    con.execute("""
        DROP TABLE IF EXISTS cotacao;
    """)

    con.execute(f"""
        CREATE TABLE cotacao AS
        SELECT *
        FROM '{processed_path}';
    """)

    con.close()