import asyncio
import os
from datetime import datetime

import nest_asyncio
import pandas as pd
from playwright.async_api import async_playwright
from tqdm import tqdm

nest_asyncio.apply()

# Settings
CSV_PATH = "all_players_ratings.csv"
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
LOG_PATH = "google_fbref_log.txt"
WAIT_BETWEEN_SEARCHES = 2.0  # Throttle: 1 search every 2 seconds

# Load & validate CSV
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"Missing CSV file: {CSV_PATH}")

df = pd.read_csv(CSV_PATH)
if "Name" not in df.columns or "Team" not in df.columns:
    raise ValueError("CSV must contain 'Name' and 'Team' columns")

if "fbref_url" not in df.columns:
    df["fbref_url"] = None

df = df.sort_values(by="Name").reset_index(drop=True)

# Start log
with open(LOG_PATH, "w", encoding="utf-8") as log:
    log.write(f"Google search started: {datetime.now()}\n\n")


# Google search for FBref profile
async def search_fbref(tab, index, name, team):
    query = f"{name} {team} fbref.com"
    url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
    result = None

    await asyncio.sleep(WAIT_BETWEEN_SEARCHES)

    try:
        await tab.goto(url, wait_until="domcontentloaded", timeout=10000)
        await tab.wait_for_selector('a[href*="fbref.com"]', timeout=5000)

        links = await tab.locator('a[href*="fbref.com"]').all()
        for link in links:
            href = await link.get_attribute("href")
            if href and "/en/players/" in href and len(href.strip("/").split("/")) > 4:
                result = href
                break

        msg = f"{name} ({team}) → {result}" if result else f"Not found: {name} ({team})"
        print(msg)
        with open(LOG_PATH, "a", encoding="utf-8") as log:
            log.write(msg + "\n")

        return result

    except Exception as e:
        error = f"Error searching {name} ({team}): {str(e)}"
        print(error)
        with open(LOG_PATH, "a", encoding="utf-8") as log:
            log.write(error + "\n")
        return None


# Scraper loop
async def run_scraper():
    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=CHROMIUM_PATH, headless=False)
        tab = await browser.new_page()

        for i, row in tqdm(
            df.iterrows(), total=len(df), desc="🔎 Searching FBref via Google"
        ):
            try:
                current_url = (
                    str(row["fbref_url"]) if pd.notnull(row["fbref_url"]) else ""
                )
                if (
                    current_url
                    and "/en/players/" in current_url
                    and len(current_url.strip("/").split("/")) > 4
                ):
                    continue  # Skip if valid URL already exists

                name = str(row["Name"]).strip()
                team = str(row["Team"]).strip()
                new_url = await search_fbref(tab, i, name, team)
                df.at[i, "fbref_url"] = new_url
            except Exception as e:
                msg = f"Error on row {i} ({row['Name']}): {str(e)}"
                print(msg)
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(msg + "\n")

        await browser.close()

    df.to_csv(CSV_PATH, index=False)
    print(f"Done! Updated CSV saved to {CSV_PATH} | Log written to {LOG_PATH}")


# Start
asyncio.run(run_scraper())
