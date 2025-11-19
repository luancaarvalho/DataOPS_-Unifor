import pandas as pd
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def validate_data(annotated_path):
    df = pd.read_parquet(annotated_path)

    assert df["Close"].notnull().all(), "Erro: coluna Close contém nulos"
    assert (df["Close"] > 0).all(), "Erro: preços não podem ser negativos"

    logger.info("Verficações finalizadas")
    return True
