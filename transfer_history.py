import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter("ignore", InsecureRequestWarning)

# Load player URL list
df = pd.read_csv("all_players_ratings.csv")

# Load previously scraped data
try:
    transfers_df = pd.read_csv("compiled_transfers.csv")
except FileNotFoundError:
    transfers_df = pd.DataFrame(
        columns=["Player", "Transfer From", "Transfer To", "Date", "Fee"]
    )

scraped_players = set(transfers_df["Player"].unique())


# Utility: Clean transfer fee string
def parse_fee(price_raw):
    fee_raw = price_raw.lower()
    if "loan" in fee_raw:
        return "Loan"
    elif "free" in fee_raw:
        return "Free"
    else:
        try:
            return int(
                fee_raw.replace("€", "")
                .replace("m", "000000")
                .replace("k", "000")
                .replace(".", "")
                .replace(",", "")
                .strip()
            )
        except:
            return "N/A"


# 🧵 Worker function for scraping a single player
def scrape_player(url):
    full_url = url + "/transfer-history"
    for attempt in range(3):
        try:
            response = requests.get(
                full_url, headers={"User-Agent": "Mozilla/5.0"}, verify=False
            )
            if response.status_code == 200:
                break
            time.sleep(2**attempt)
        except Exception:
            time.sleep(2**attempt)
    else:
        return []

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        player_name = (
            soup.find("h1").get_text(strip=True)
            if soup.find("h1")
            else url.split("/")[-1]
        )

        if player_name in scraped_players:
            return []

        transfer_rows = soup.select("table tbody tr")
        player_transfers = []

        for row in transfer_rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            date = cells[0].get_text(strip=True)
            clubs = cells[1].select(".transfer-club__name")
            from_club = clubs[0].get_text(strip=True) if len(clubs) > 0 else "N/A"
            to_club = clubs[1].get_text(strip=True) if len(clubs) > 1 else "N/A"
            price_raw = cells[2].get_text(strip=True)
            fee = parse_fee(price_raw)

            player_transfers.append(
                {
                    "Player": player_name,
                    "Transfer From": from_club,
                    "Transfer To": to_club,
                    "Date": date,
                    "Fee": fee,
                }
            )

        return player_transfers

    except Exception:
        return []


# 🧵 Run scraping in parallel using 5 threads
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(scrape_player, url): url for url in df["Player URL"]}
    for future in tqdm(
        as_completed(futures), total=len(futures), desc="Scraping with concurrency"
    ):
        data = future.result()
        if data:
            pd.DataFrame(data).to_csv(
                "compiled_transfers.csv", mode="a", index=False, header=False
            )
            scraped_players.add(data[0]["Player"])

print("🏁 Done! All new transfers saved to compiled_transfers.csv.")
