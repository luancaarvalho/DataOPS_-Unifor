import yfinance as yf
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

def download_financial_data():
    br_company = ['PETR4.SA', 'BBAS3.SA', 'ELET6.SA', 'SBSP3.SA', 'VALE3.SA']

    end = datetime.now()
    start = end - relativedelta(months=6)
    logger.info("Iniciando download dos dados financeiros brutos")

    df = yf.download(br_company, start=start, end=end, group_by='ticker', threads=False)
    logger.info("Download finalizado")

    os.makedirs("/opt/airflow/data/raw", exist_ok=True)
    path = "/opt/airflow/data/raw/br_raw.parquet"
    df.to_parquet(path)
    return path