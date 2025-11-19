import traceback

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from logs.logger import Logger

SITEMAP_URL = "https://br.investing.com/news/stock-market-news"
HEADERS = {
    "User-Agent": "FintechDataBot/1.0 - Study purpose only",
    "Accept-Language": "en-US,en;q=0.9"
}

def get_page(url):
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--disable-blink-features=AutomationControlled")
        if "User-Agent" in HEADERS:
            options.add_argument(f"user-agent={HEADERS['User-Agent']}")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(30)
        
        try:
            driver.get(url)
        except Exception as e:
            log.logger.error(f"Error loading {url}: {e}")
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            log.logger.warning(f"Timeout waiting for body on {url}")

        html = driver.page_source
        if not html:
            log.logger.error(f"No page source retrieved for {url}")
            return None

        soup = BeautifulSoup(html, "lxml")
        return soup

    except Exception as e:
        log.logger.error(f"Error fetching {url}: {e}\n{traceback.format_exc()}")
        return None

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def extract_news(soup):
    articles = soup.select('[data-test="article-item"]')
    data = []
    for art in articles:
        title_tag = art.select_one('[data-test="article-title-link"]')
        date_tag = art.select_one('[data-test="article-publish-date"]')

        title = title_tag.text.strip() if title_tag else None
        url = urljoin(SITEMAP_URL, title_tag["href"]) if title_tag else None
        pub_date = date_tag["datetime"] if date_tag else None

        # TODO: fetch lead in parallel
        # lead = extract_lead_paragraph(url)

        item = {"title": title, "lead": "", "url": url, "published_at": pub_date}
        data.append(item)

        print(f"News found: {item}")
    return data

def extract_lead_paragraph(url):
    try:
        soup = get_page(url)
        # The lead paragraph is usually the first <p> inside article content
        lead = None
        article = soup.find("article")
        if article:
            p = article.find("p")
            if p:
                lead = p.get_text(strip=True)

        # fallback: try first paragraph anywhere
        if not lead:
            p = soup.find("p")
            if p:
                lead = p.get_text(strip=True)

        return lead
    except Exception:
        return None
