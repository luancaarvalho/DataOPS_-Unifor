from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests

from logs.logger import Logger

log = Logger("etl.scrapper.bcb")

BCB_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"

USD_BRL_CODE = 1
SELIC_CODE = 11

def get_data(code: str, start_date: str, end_date: str, format: str = "json") -> dict:
    url = f"{BCB_API_URL}.{code}/dados?formato={format}&dataInicial={start_date}&dataFinal={end_date}"

    log.logger.info(f"📄 Fetching API: {url}")

    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception("Failed to get BCB data")
    
    log.logger.info(f"Got successful response!")

    return resp.json()

def get_date_range(months_back: int) -> tuple[str, str]:
    """
    Returns a string with dataInicial and dataFinal for the last N months
    formatted as required by the BCB API: dd/mm/yyyy
    """
    data_final = datetime.today()
    data_inicial = data_final - relativedelta(months=months_back)
    
    data_inicial_str = data_inicial.strftime("%d/%m/%Y")
    data_final_str = data_final.strftime("%d/%m/%Y")
    
    return data_inicial_str, data_final_str