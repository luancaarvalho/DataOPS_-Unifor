import pandas as pd
from datetime import datetime

from logs.logger import Logger
from src.scrapper.scrapper import Scrapper


class Extractor:
    def __init__(self):
        self.scrapper = Scrapper()

    def collect_news(self) -> pd.DataFrame:
        return self.scrapper.get_news()
    
    def collect_monetary_history(self) -> pd.DataFrame:
        return self.scrapper.get_real2dolar_history()
    
    def collect_selic_history(self) -> pd.DataFrame:
        return self.scrapper.get_selic_history()
    
    def collect_selic(self, date: str) -> pd.DataFrame:
        print(f"Collecting SELIC data for date: {date}")
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")
        return self.scrapper.get_selic(date)