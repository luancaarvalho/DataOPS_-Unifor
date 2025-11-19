import pandas as pd
from datetime import datetime

from src.scrapper.br_investing import SITEMAP_URL as BR_INVESTING_URL
from src.scrapper.br_investing import get_page as br_investing_get_page, extract_news as br_investing_extract_news

from src.scrapper.bcb import USD_BRL_CODE as BCB_USD_BRL_CODE, SELIC_CODE as BCB_SELIC_CODE
from src.scrapper.bcb import get_data as bcb_get_data, get_date_range as bcb_get_date_range

class Scrapper():
    def __init__(self):
        pass

    def get_news(self) -> pd.DataFrame:
        min_news = 10
        news = []
        page = 1

        while len(news) < min_news:
            url = BR_INVESTING_URL
            if page > 1:
                url += f"/{page}"

            print(f"📄 Collecting page: {url}")
            soup = br_investing_get_page(url)
            if not soup:
                raise Exception(f"Failed to get page content")
            
            items = br_investing_extract_news(soup)
            if len(items) == 0:
                print("Breaking loop as no news were extracted")
                break

            news.extend(items)
            page += 1

        df = pd.DataFrame(news)
        df["collected_at"] = datetime.now()

        print(f"✅ Total of news scrapped: {len(df)}")

        return df
    
    def get_real2dolar_history(self) -> pd.DataFrame:
        return self.get_history(BCB_USD_BRL_CODE)
    
    def get_selic_history(self) -> pd.DataFrame:
        return self.get_history(BCB_SELIC_CODE, 120)
    
    def get_selic(self, date: str) -> pd.DataFrame:
        data = bcb_get_data(BCB_SELIC_CODE, date, date)

        return pd.DataFrame(data)
        
    def get_history(self, code: str, months_back: int = 1) -> pd.DataFrame:
        # This BCB API only accept request from at most 10 years (120 months) at a time
        # TODO: improve this function to break greater than 120 months calls into multiple requests
        if months_back > 120:
            raise Exception("Exceeded maximum threshold")

        start, end = bcb_get_date_range(months_back)

        data = bcb_get_data(code, start, end)

        return pd.DataFrame(data)
