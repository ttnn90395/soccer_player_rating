import asyncio
import os

import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tqdm import tqdm

nest_asyncio.apply()

# Settings
BASE_URL = "https://fbref.com"
CHROMIUM_PATH = (
    r"C:/Users/L1160681/playwright-browsers/chromium-win64/chrome-win/chrome.exe"
)
CSV_PATH = r"C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_original_updated.csv"
ROOT_FOLDER = "all_players_fbref_tables"

os.makedirs(ROOT_FOLDER, exist_ok=True)

# Load player data
df = pd.read_csv(CSV_PATH)
df = df.dropna(subset=["fbref_alltimestat", "Name"])
df = df[df["fbref_alltimestat"].str.startswith("https://fbref.com")]

# Count existing folders to resume scraping
total_number = len(
    [
        name
        for name in os.listdir(ROOT_FOLDER)
        if os.path.isdir(os.path.join(ROOT_FOLDER, name))
    ]
)

print(f"Found {total_number} existing player folders. Resuming from there...")


async def scrape_all_fbref_tables(context, url: str, player_name: str):
    try:
        player_id = url.split("/")[5]
        SAVE_FOLDER = f"{player_name}_{player_id}"
        folder_path = os.path.join(ROOT_FOLDER, SAVE_FOLDER)

        # Skip if folder exists and contains .csv files
        if os.path.exists(folder_path) and any(
            fname.endswith(".csv") for fname in os.listdir(folder_path)
        ):
            print(f"Skipping {player_name} ({player_id}) — folder already populated.")
            return

        os.makedirs(folder_path, exist_ok=True)

        tab = await context.new_page()
        await tab.goto(url, timeout=60000)

        await tab.evaluate("""
            document.querySelectorAll('a.sr_preset').forEach(el => el.click());
        """)

        html = await tab.content()
        await tab.close()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")

        print(f"Found {len(tables)} tables on tab for {player_name}")
        table_ids = [table.get("id") for table in tables if table.get("id")]
        print(f"{player_name} ({player_id}) - Table IDs: {table_ids}")

        for table in tables:
            table_id = table.get("id", None)
            if not table_id:
                continue

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
                df_table = pd.DataFrame(rows, columns=headers)
                output_path = os.path.join(folder_path, f"{table_id}.csv")
                df_table.to_csv(output_path, index=False)

    except Exception as e:
        print(f"Error scraping {player_name}: {e}")


async def main():
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            executable_path=CHROMIUM_PATH, headless=True
        )

        # Create a single browser context (i.e. one window)
        context = await browser.new_context()

        rows_to_scrape = df[total_number - 50 :]
        batch_size = len(rows_to_scrape)

        for batch_start in tqdm(
            range(0, len(rows_to_scrape), batch_size),
            desc="Scraping batches of players",
        ):
            batch = rows_to_scrape.iloc[batch_start : batch_start + batch_size]
            tasks = []

            for i, (_, row) in enumerate(batch.iterrows()):
                url = row["fbref_alltimestat"]
                name = row["Name"]

                await asyncio.sleep(6)  # Delay between tab openings
                task = asyncio.create_task(scrape_all_fbref_tables(context, url, name))
                tasks.append(task)

            await asyncio.gather(*tasks)

        await browser.close()


# Run the scraper
asyncio.run(main())
