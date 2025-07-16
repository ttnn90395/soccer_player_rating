import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

# 🔗 Constants
BASE_URL = (
    "https://www.footballtransfers.com/en/values/players/most-valuable-players/{}"
)
MAX_RETRIES = 8
THREADS = 7
MAX_PAGES = 1353
TIMEOUT_SECONDS = 1
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
DATA_FILE = "most_valuable_players_fast.csv"
SCRAPED_PAGES_LOG = "scraped_pages.txt"
FAILED_PAGES_LOG = "failed_pages.txt"


# 📄 Page Logging Helpers
def load_page_log(file_path):
    try:
        with open(file_path, "r") as f:
            return set(int(line.strip()) for line in f if line.strip().isdigit())
    except FileNotFoundError:
        return set()


def log_page(file_path, page_number):
    with open(file_path, "a") as f:
        f.write(f"{page_number}\n")


# 🧭 Setup WebDriver
def create_driver():
    options = Options()
    options.binary_location = CHROMIUM_PATH
    options.add_argument("--headless=chrome")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 2,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.plugins": 2,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
    }
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options)


# 🧪 Parse HTML Page
def parse_html(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    rows = soup.find_all("tr")
    data = []

    for row in rows:
        try:
            name_tag = row.select_one("td.td-player a[title]")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            player_url = "https://www.footballtransfers.com" + name_tag["href"]

            age_tag = row.select_one("td.age")
            age = age_tag.get_text(strip=True) if age_tag else None

            club_tag = row.select_one("td.td-player .sub-text a[title]")
            club = club_tag.get_text(strip=True) if club_tag else None
            club_url = (
                "https://www.footballtransfers.com" + club_tag["href"]
                if club_tag
                else None
            )

            position_tag = row.select_one("td.td-player span.sub-text")
            position = (
                position_tag.get_text(strip=True).split("•")[-1].strip()
                if position_tag
                else None
            )

            nationality_tag = row.select_one("td.td-player figure img")
            nationality = nationality_tag["alt"] if nationality_tag else None

            skill_tag = row.select_one("div.table-skill__skill")
            skill = float(skill_tag.get_text(strip=True)) if skill_tag else None

            potential_tag = row.select_one("div.table-skill__pot")
            potential = (
                float(potential_tag.get_text(strip=True)) if potential_tag else None
            )

            value_tag = row.select_one("span.player-tag")
            value = (
                value_tag.get_text(strip=True).replace("€", "") if value_tag else None
            )

            if value and "M" in value:
                market_value = float(value.replace("M", "")) * 1e6
            elif value and "K" in value:
                market_value = float(value.replace("K", "")) * 1e3
            else:
                market_value = None

            data.append(
                {
                    "Name": name,
                    "Player URL": player_url,
                    "Age": age,
                    "Club": club,
                    "Club URL": club_url,
                    "Position": position,
                    "Nationality": nationality,
                    "Skill": skill,
                    "Potential": potential,
                    "Market Value (€)": market_value,
                }
            )
        except Exception as e:
            print(f"⚠️ Parse error: {e}")
    return data


# 🚜 Scrape Range
def scrape_page_range(start_page, end_page, scraped_pages):
    driver = create_driver()
    wait = WebDriverWait(driver, TIMEOUT_SECONDS)

    for page in tqdm(
        range(start_page, end_page + 1),
        desc=f"Thread {start_page}-{end_page}",
        leave=False,
    ):
        if page in scraped_pages:
            print(f"⏭️ Skipping page {page} (already scraped)")
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                url = BASE_URL.format(page)
                driver.get(url)
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "td.td-player a[title]")
                    )
                )
                data = parse_html(driver.page_source)

                if data:
                    df = pd.DataFrame(data)
                    df.to_csv(
                        DATA_FILE,
                        mode="a",
                        header=not os.path.exists(DATA_FILE),
                        index=False,
                    )
                    log_page(SCRAPED_PAGES_LOG, page)
                    print(f"✅ Saved page {page} with {len(data)} players")
                else:
                    print(f"⚠️ No data found on page {page}")
                break
            except Exception as e:
                print(f"🔁 Retry {attempt} failed on page {page}: {e}")
                time.sleep(2 * attempt)
                if attempt == MAX_RETRIES:
                    log_page(FAILED_PAGES_LOG, page)
                    print(f"❌ Failed page {page} after {MAX_RETRIES} retries")
    driver.quit()


# 🚀 Main Execution
def main():
    chunk_size = math.ceil(MAX_PAGES / THREADS)
    ranges = [
        (i, min(i + chunk_size - 1, MAX_PAGES))
        for i in range(1, MAX_PAGES + 1, chunk_size)
    ]
    scraped_pages = load_page_log(SCRAPED_PAGES_LOG)

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [
            executor.submit(scrape_page_range, start, end, scraped_pages)
            for start, end in ranges
        ]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="🔄 Scraping Progress"
        ):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Thread error: {e}")

    # 📊 Summary Report
    scraped = load_page_log(SCRAPED_PAGES_LOG)
    failed = load_page_log(FAILED_PAGES_LOG)
    all_attempted = scraped | failed
    skipped = set(range(1, MAX_PAGES + 1)) - all_attempted

    print("\n📊 Summary:")
    print(f"✅ Scraped pages: {len(scraped)}")
    print(f"❌ Failed pages: {len(failed)} (see '{FAILED_PAGES_LOG}')")
    print(f"⏭️ Skipped pages: {len(skipped)} (not attempted yet)")


if __name__ == "__main__":
    main()
