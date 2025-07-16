import asyncio
import os
from datetime import datetime

import nest_asyncio
import pandas as pd
from playwright.async_api import async_playwright
from tqdm import tqdm

nest_asyncio.apply()

# ✏️ Configuration
CSV_PATH = "all_players_ratings.csv"
DUCKDUCKGO_SEARCH = "https://duckduckgo.com/?q=site%3Afbref.com+"
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
LOG_PATH = "scraping_log.txt"
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 4  # ⏳ Backoff: 4s, 16s, etc.
WAIT_BETWEEN_TASKS = 0.7  # 🐢 Delay between players

# 📄 Load & validate CSV
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"Missing CSV file at: {CSV_PATH}")

df = pd.read_csv(CSV_PATH)
if "Name" not in df.columns or "Team" not in df.columns:
    raise ValueError("CSV must contain 'Name' and 'Team' columns")

if "fbref_url" not in df.columns:
    df["fbref_url"] = None

df = df.sample(frac=1).reset_index(drop=True)

# 📊 Show how many players are missing fbref_url
missing_count = df["fbref_url"].isna().sum()
print(f"🔢 Players missing fbref_url: {missing_count}")
with open(LOG_PATH, "w", encoding="utf-8") as log:
    log.write(f"Scraping started at {datetime.now()}\n")
    log.write(f"🔢 Players missing fbref_url: {missing_count}\n\n")


# 🔄 Single player search
async def scrape_player(tab, index, name, club):
    query = f"{name} {club} fbref profile".replace(" ", "+")
    url = DUCKDUCKGO_SEARCH + query + "&ia=web"
    match = None
    await asyncio.sleep(WAIT_BETWEEN_TASKS)

    for attempt in range(RETRY_ATTEMPTS):
        try:
            await tab.goto(url, wait_until="domcontentloaded", timeout=1000)
            await tab.wait_for_timeout(700)
            content = await tab.content()

            links = await tab.locator("a").all()
            if not links:
                raise Exception("No links found on DuckDuckGo page")

            for link in links:
                href = await link.get_attribute("href")
                if href and "fbref.com/en/players/" in href:
                    match = href
                    break

            msg = (
                f"✅ Found: {name} ({club}) → {match}"
                if match
                else f"❌ Not found: {name} ({club})"
            )
            print(msg)
            with open(LOG_PATH, "a", encoding="utf-8") as log:
                log.write(msg + "\n")

            if match:
                df.at[index, "fbref_url"] = match
                df.to_csv(CSV_PATH, index=False)  # 💾 Live update!
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(f"📝 Saved {name} ({club}) to CSV\n")

            else:
                debug_path = f"debug_{index}_{name.replace(' ', '_')}.html"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(content)

            break

        except Exception as e:
            await asyncio.sleep(RETRY_BACKOFF**attempt)
            err_text = str(e)

            if "418" in err_text or "I'm a teapot" in err_text:
                msg = f"🫖 418 Teapot error for {name} ({club}) — sleeping for 1 hour."
                print(msg)
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(msg + "\n")
                await asyncio.sleep(3600)
                break

            if "ERR_TIMED_OUT" in err_text:
                timeout_msg = f"⏱️ Timeout error: {name} ({club}) → {url}"
                print(timeout_msg)
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(timeout_msg + "\n")
            else:
                error_msg = f"⚠️ Error on {name} ({club}): {err_text}"
                print(error_msg)
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(error_msg + "\n")


# 🚀 Scraper loop
async def run_scraper():
    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=CHROMIUM_PATH, headless=False)
        tab = await browser.new_page()

        for i, row in tqdm(
            df.iterrows(), total=len(df), desc="🔎 Scraping FBref links"
        ):
            try:
                if pd.notnull(row["fbref_url"]):
                    skip_msg = f"⏭️ Skipped: {row['Name']} ({row['Team']}) — already has fbref_url"
                    print(skip_msg)
                    with open(LOG_PATH, "a", encoding="utf-8") as log:
                        log.write(skip_msg + "\n")
                    continue

                name = str(row["Name"]).strip()
                club = str(row["Team"]).strip()
                await scrape_player(tab, i, name, club)
            except Exception as e:
                error_msg = (
                    f"💥 Unhandled error for {row['Name']} ({row['Team']}): {str(e)}"
                )
                print(error_msg)
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(error_msg + "\n")

        await browser.close()

    print(f"\n📁 Done! Logs saved to {LOG_PATH}")


# 🧨 Start
asyncio.run(run_scraper())
