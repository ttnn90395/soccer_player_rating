import asyncio
import os

import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

nest_asyncio.apply()

# Settings
BASE_URL = "https://fbref.com"
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
SAVE_FOLDER = "alisson_all_fbref_tables"

os.makedirs(SAVE_FOLDER, exist_ok=True)


async def scrape_all_fbref_tables():
    url = f"{BASE_URL}/en/players/7a2e46a8/all_comps/Alisson-Stats---All-Competitions"

    async with async_playwright() as p:
        browser = await p.chromium.launch(executable_path=CHROMIUM_PATH, headless=True)
        page = await browser.new_page()
        await page.goto(url)

        # Trigger all preset buttons to reveal toggled tables
        await page.evaluate("""
            document.querySelectorAll('a.sr_preset').forEach(el => el.click());
        """)
        await asyncio.sleep(5)  # Let the page render updated sections

        html = await page.content()
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    print(f"🔍 Found {len(tables)} tables on page")

    for table in tables:
        table_id = table.get("id", None)
        if not table_id:
            continue  # skip tables without ID

        print(f"📊 Processing table: {table_id}")

        # Extract headers
        header_row = (
            table.find("thead").find_all("tr")[-1] if table.find("thead") else None
        )
        headers = (
            [th.get_text(strip=True) for th in header_row.find_all("th")]
            if header_row
            else []
        )

        rows = []
        for tr in table.find("tbody").find_all("tr"):
            row = []
            for cell in tr.find_all(["th", "td"]):
                text = cell.get_text(strip=True)
                link = cell.find("a")
                if link and link.get("href"):
                    text += f" ({BASE_URL}{link.get('href')})"
                row.append(text)
            if len(row) == len(headers):
                rows.append(row)

        if rows:
            df = pd.DataFrame(rows, columns=headers)
            output_path = os.path.join(SAVE_FOLDER, f"{table_id}.csv")
            df.to_csv(output_path, index=False)
            print(f"✅ Saved: {output_path}")
        else:
            print(f"⚠️ Skipped empty table: {table_id}")


# Run the function
asyncio.run(scrape_all_fbref_tables())
