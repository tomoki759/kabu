# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

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
def scrape_kabutan_52w_page(page: int, driver):
    url = f"https://kabutan.jp/warning/?mode=3_3&market=0&capitalization=-1&dispmode=normal&stc=code&stm=0&page={page}"

    # requests の代わりに Selenium でページを開く（ブロックを回避）
    driver.get(url)
    # ★ ページ内のテーブル（銘柄一覧）が読み込まれるまで最大10秒待機する処理を追加
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.stock_table")))
    except Exception as e:
        print(f"[WARN] Table not found on page {page}: {e}")
    
    time.sleep(1) # 念のためのマージン
    
    # 開いたページのHTMLを BeautifulSoup に渡す
    soup = BeautifulSoup(driver.page_source, "html.parser")

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


def scrape_all_kabutan_52w(driver, max_pages=15, sleep_sec=1.5):
    all_records = []

    for page in range(1, max_pages + 1):
        print(f"Scraping list page {page}...")
        # driver を渡す
        records = scrape_kabutan_52w_page(page, driver)

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

        # 1. 「業績評価」の文字を起点にして、その「1つ上の階層の親div（2つのdivを包んでいる箱）」を取得
        card_element = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[contains(text(),'業績評価')]/..")
            )
        )

        # 2. 箱の中にあるすべてのテキストを合体して取得
        # （これで「業績評価\n晴れ時々曇り」のような文字列が取れます）
        full_text = card_element.text.strip()

        # 3. 「業績評価」という見出し文字を消して、純粋な評価内容（「晴れ時々曇り」）だけを抽出
        rating_value = full_text.replace("業績評価", "").strip()

        return rating_value

    except Exception as e:
        print(f"[minkabu selenium error] {code}: {e}")
        return None

def upload_to_gdrive(filename, filepath, folder_id):

    creds = Credentials(
        None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": filename,
        "parents": [folder_id]
    }

    media = MediaFileUpload(filepath, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print("Uploaded file ID:", file.get("id"))

# ---------------------------
# main
# ---------------------------

if __name__ == "__main__":
    TEST_MODE = True  # まずはテストモードで！
    TEST_LIMIT = 5
    try:
        # 1. 最初に Selenium を起動する
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--remote-debugging-pipe")

        if "CHROME_BIN" in os.environ:
            options.binary_location = os.environ["CHROME_BIN"]

        print("[INFO] Starting Selenium", flush=True)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        print("[INFO] Started Selenium", flush=True)
        
        # 2. 起動した driver を渡して株探のリストを取得する（ブロック回避）
        df = scrape_all_kabutan_52w(driver, max_pages=15)
        print(f"\n52週高値銘柄数: {len(df)}")
        
        if TEST_MODE:
            df = df.head(TEST_LIMIT).copy()
    
        # 3. そのままみんかぶのスクレイピングに移行する
        ratings = []
        for i, code in enumerate(df["code"].head(TEST_LIMIT if TEST_MODE else len(df)), 1):
            print(f"[{i}/{len(df)}] minkabu selenium scraping: {code}")
            rating = scrape_minkabu_performance_selenium(code, driver)
            ratings.append(rating)
            time.sleep(1.5)

        driver.quit()

        df["performance_rating"] = ratings


        today = datetime.today().strftime("%Y%m%d")
        csv_name = f"kabutan_52w_{today}.csv"

        df.to_csv(csv_name, index=False, encoding="utf-8-sig")

        GDRIVE_FOLDER_ID = "1gfso7YvjiclmQ5OdA8w9v3SpTZjGCe_W"

        upload_to_gdrive(
            filename=csv_name,
            filepath=csv_name,
            folder_id=GDRIVE_FOLDER_ID
        )

        print(f"[OK] Uploaded to Google Drive: {csv_name}")

    except Exception as e:
        import traceback
        print("🔥 FATAL ERROR 🔥", flush=True)
        traceback.print_exc()
        raise
