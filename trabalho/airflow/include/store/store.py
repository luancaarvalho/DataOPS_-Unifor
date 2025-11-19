import os
import sqlite3
import logging
import pandas as pd

logging.basicConfig()
logger = logging.getLogger(__name__)

def store_data(processed_path):
    db_path = "/opt/airflow/data/db/finance.db"
    os.makedirs("/opt/airflow/data/db", exist_ok=True)

    logger.info("Lendo parquet processado...")
    df = pd.read_parquet(processed_path)

    logger.info("Conectando ao SQLite...")
    con = sqlite3.connect(db_path)

    logger.info("Removendo tabela antiga (se existir)...")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS cotacao;")
    con.commit()

    logger.info("Gravando dados no SQLite...")
    df.to_sql("cotacao", con, if_exists="replace", index=False)

    con.close()
    logger.info(f"Dados salvos com sucesso em {db_path} na tabela 'cotacao'.")

    return db_path
