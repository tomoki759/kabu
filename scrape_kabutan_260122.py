# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9",
    "Referer": "https://kabutan.jp/"
}


# ---------------------------
# 52週高値一覧ページ
# ---------------------------
def scrape_kabutan_52w_page(page: int):
    url = "https://kabutan.jp/warning/"
    params = {
        "mode": "3_3",
        "market": "0",
        "capitalization": "-1",
        "dispmode": "normal",
        "stc": "code",
        "stm": "0",
        "page": page
    }

    r = requests.get(url, params=params, headers=HEADERS, timeout=10)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    results = []

    for row in soup.select("table.stock_table tbody tr"):
        tds = row.find_all("td")
        name_tag = row.select_one("th.tal")
        code_tag = row.select_one("td.tac a")

        if not (tds and name_tag and code_tag):
            continue

        code = code_tag.text.strip()
        name = name_tag.text.strip()

        if not (code.isdigit() and len(code) == 4):
            continue

        try:
            market = tds[1].text.strip()
            per = tds[-3].text.strip()
            pbr = tds[-2].text.strip()
        except IndexError:
            continue

        results.append({
            "code": code,
            "name": name,
            "market": market,
            "PER": per,
            "PBR": pbr
        })

    return results


def scrape_all_kabutan_52w(max_pages=15, sleep_sec=1.5):
    all_records = []

    for page in range(1, max_pages + 1):
        print(f"Scraping list page {page}...")
        records = scrape_kabutan_52w_page(page)

        if not records:
            break

        all_records.extend(records)
        time.sleep(sleep_sec)

    return pd.DataFrame(all_records).drop_duplicates()

def scrape_minkabu_performance_selenium(code: str, driver):
    url = f"https://minkabu.jp/stock/{code}"
    driver.get(url)

    try:
        wait = WebDriverWait(driver, 10)

        # 「業績評価」というテキストを含むdivを起点にする
        label = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[contains(text(),'業績評価')]")
            )
        )

        # その直後にある評価テキスト
        value = label.find_element(
            By.XPATH, "following-sibling::div"
        )

        return value.text.strip()

    except Exception as e:
        print(f"[minkabu selenium error] {code}: {e}")
        return None



# ---------------------------
# main
# ---------------------------

if __name__ == "__main__":

    df = scrape_all_kabutan_52w(max_pages=15)
    print(f"\n52週高値銘柄数: {len(df)}")
    
    # Selenium 起動
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)

    ratings = []
    for i, code in enumerate(df["code"], 1):
        print(f"[{i}/{len(df)}] minkabu selenium scraping: {code}")
        rating = scrape_minkabu_performance_selenium(code, driver)
        ratings.append(rating)
        time.sleep(1.5)  # ★重要：アクセス間隔

    driver.quit()
    
    df["performance_rating"] = ratings


    today = datetime.today().strftime("%Y%m%d")
    filename = f"kabutan_52w_{today}.csv"

    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\nSaved: {filename}")
