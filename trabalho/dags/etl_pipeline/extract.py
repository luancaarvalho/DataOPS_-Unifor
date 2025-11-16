import random
import tempfile
from airflow.decorators import task # type: ignore
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import uuid
import pandas as pd
from datetime import datetime

@task
def extract_coins():
    print("🚀 Iniciando extração de coins...")
    tmp_profile = tempfile.mkdtemp(prefix=f"sb_profile_{uuid.uuid4()}_")

    driver = Driver(uc=True,
                    headless=True,
                    browser="chrome",
                    no_sandbox=True,
                    user_data_dir=tmp_profile,)
    wait = WebDriverWait(driver, 10)
    df_meme_coins = pd.DataFrame()

    driver.get("https://www.coingecko.com/en/categories/meme-token")

    for page in range(1,2):
        print(f"coletando pagina {page}")
        driver.get(f"https://www.coingecko.com/en/categories/meme-token?page={page}")
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        rows = soup.select("tbody tr")
        for row in rows:
            if len(row.select("td")) < 3:
                continue

            name_content = row.select_one("td:nth-child(3)")
            if not name_content:
                continue

            name = " ".join(name_content.text.strip().split(" ")[:-1]).strip()
            symbol = name_content.text.strip().split(" ")[-1]

            price = row.select_one("td:nth-child(5)")
            if not price:
                continue
            price = price.text.strip()

            volume = row.select_one("td:nth-child(10)")
            if not volume:
                continue
            volume = volume.text.strip()

            cap = row.select_one("td:nth-child(11)")
            if not cap:
                continue
            cap = cap.text.strip()

            date = datetime.today().isoformat()

            # anotando se a moeda é válida aleatoriamente. 33% de chance de ser válida.
            is_valid = random.random() < 0.33

            obj = {
                "id": str(uuid.uuid4()),
                "name": name,
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "cap": cap,
                "date": date,
                "is_valid": is_valid
            }

            df_meme_coins = pd.concat([df_meme_coins, pd.DataFrame([obj])], ignore_index=True)


    print("✅ Coins Extraídos:", df_meme_coins.shape[0])
    driver.quit()

    return df_meme_coins