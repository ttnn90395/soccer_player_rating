import asyncio
import os

import nest_asyncio
import pandas as pd
from playwright.async_api import async_playwright
from tqdm import tqdm

nest_asyncio.apply()

# ✏️ Configuration
ORIGINAL_CSV = r"C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_original.csv"
REFERENCE_CSV = r"C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings.csv"
DUCKDUCKGO_SEARCH = "https://duckduckgo.com/?q=site%3Afbref.com+"
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
LOG_PATH = "scraping_log.txt"
RETRY_ATTEMPTS = 10
RETRY_BACKOFF = 4
WAIT_BETWEEN_TASKS = 0.2

# 📄 Load CSVs
if not os.path.exists(ORIGINAL_CSV) or not os.path.exists(REFERENCE_CSV):
    raise FileNotFoundError("One or both CSV files are missing.")

df_original = pd.read_csv(ORIGINAL_CSV)
df_reference = pd.read_csv(REFERENCE_CSV)

# Ensure required columns exist
for df in [df_original, df_reference]:
    if "Name" not in df.columns or "Team" not in df.columns:
        raise ValueError("CSV must contain 'Name' and 'Team' columns")
    if "fbref_url" not in df.columns:
        df["fbref_url"] = None

df_original["fbref_url"] = None
df_original = df_original.sort_values(by="Value", ascending=False)


# 🔄 Single player search
async def scrape_player(tab, index, name, club):
    query = f"{name} {club} fbref profile".replace(" ", "+")
    url = DUCKDUCKGO_SEARCH + query + "&ia=web"
    match = None
    await asyncio.sleep(WAIT_BETWEEN_TASKS)

    for attempt in range(RETRY_ATTEMPTS):
        try:
            await tab.goto(url, wait_until="domcontentloaded", timeout=10000)
            await tab.wait_for_timeout(700)
            content = await tab.content()

            links = await tab.locator("a").all()
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
                df_original.at[index, "fbref_url"] = match
                df_original.to_csv(ORIGINAL_CSV, index=False)
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
                print(f"🫖 418 Teapot error for {name} ({club}) — sleeping for 1 hour.")
                await asyncio.sleep(3600)
                break
            print(f"⚠️ Error on {name} ({club}): {err_text}")
            with open(LOG_PATH, "a", encoding="utf-8") as log:
                log.write(f"⚠️ Error on {name} ({club}): {err_text}\n")


# 🚀 Scraper loop
async def run_scraper():
    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=CHROMIUM_PATH, headless=False)
        tab = await browser.new_page()

        for i, row in tqdm(
            df_original.iterrows(),
            total=len(df_original),
            desc="🔎 Scraping FBref links",
        ):
            name = str(row["Name"]).strip()
            club = str(row["Team"]).strip()

            # Check reference CSV for existing URL
            ref_match = df_reference[
                (df_reference["Name"].str.strip() == name)
                & (df_reference["Team"].str.strip() == club)
            ]

            if not ref_match.empty and pd.notnull(ref_match.iloc[0]["fbref_url"]):
                url = ref_match.iloc[0]["fbref_url"]
                df_original.at[i, "fbref_url"] = url
                print(f"📥 Retrieved from reference: {name} ({club}) → {url}")
                with open(LOG_PATH, "a", encoding="utf-8") as log:
                    log.write(f"📥 Retrieved from reference: {name} ({club}) → {url}\n")
                continue

            await scrape_player(tab, i, name, club)

        await browser.close()
    print(f"\n📁 Done! Logs saved to {LOG_PATH}")


# 🧨 Start
asyncio.run(run_scraper())
