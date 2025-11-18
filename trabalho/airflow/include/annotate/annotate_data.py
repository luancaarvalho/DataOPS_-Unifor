import pandas as pd
import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def annotate_financial_data(raw_path):
    df = pd.read_parquet(raw_path)

    logger.info("Removendo multi-índice de colunas")
    df = df.stack(level=0).reset_index()
    
    logger.info("Renomeando colunas")
    df = df.rename(columns={"level_1": "Ticker"})

    logger.info("Inserindo coluna empresa")
    coluna_empresa = {
        'PETR4.SA': 'Petrobras',
        'BBAS3.SA': 'Banco do Brasil',
        'ELET6.SA': 'Eletrobras',
        'SBSP3.SA': 'Empresa de Saneamento de SP',
        'VALE3.SA': 'Vale'
    }
    df['Empresa'] = df['Ticker'].map(coluna_empresa)

    logger.info("Inserindo coluna setor")
    coluna_setor = {
        'PETR4.SA': 'Energia',
        'BBAS3.SA': 'Financeiro',
        'ELET6.SA': 'Energia',
        'SBSP3.SA': 'Saneamento',
        'VALE3.SA': 'Mineração'
    }
    df['Setor'] = df['Ticker'].map(coluna_setor)

    logger.info("Salvando dados anotados")
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    annotated_path = "/opt/airflow/data/processed/br_annotated.parquet"
    df.to_parquet(annotated_path)
    return annotated_path
