import os
import time
import warnings

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter("ignore", InsecureRequestWarning)

# Load and sort input CSV by index
df = pd.read_csv("all_players_ratings_original.csv")
df = df.sort_index()

# Output file path
compiled_path = "compiled_transfers.csv"


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


# Worker function for scraping and writing a single player
def scrape_and_write(index, url):
    full_url = url + "/transfer-history"
    for attempt in range(3):
        try:
            print(f"Fetching: {full_url} (Attempt {attempt + 1})")
            response = requests.get(
                full_url, headers={"User-Agent": "Mozilla/5.0"}, verify=False
            )
            print(f"Status code: {response.status_code}")
            if response.status_code == 200:
                break
            time.sleep(2**attempt)
        except Exception as e:
            print(f"Request error: {e}")
            time.sleep(2**attempt)
    else:
        print(f"Failed to fetch after retries: {full_url}")
        return

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        player_name = (
            soup.find("h1").get_text(strip=True)
            if soup.find("h1")
            else url.split("/")[-1]
        )

        transfer_rows = soup.select("table tbody tr")
        print(f"{player_name}: Found {len(transfer_rows)} transfer rows")

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
                    "Player Index": index,
                    "Player": player_name,
                    "Transfer From": from_club,
                    "Transfer To": to_club,
                    "Date": date,
                    "Fee": fee,
                }
            )

        if player_transfers:
            new_df = pd.DataFrame(player_transfers)
            header = not os.path.exists(compiled_path)
            new_df.to_csv(compiled_path, mode="a", index=False, header=header)
            print(
                f"Written {len(player_transfers)} transfers for {player_name} (Index {index})"
            )

    except Exception as e:
        print(f"Parsing error for {full_url}: {e}")


# 🐢 Run scraping sequentially without concurrency
for idx, url in tqdm(
    df["Player URL"].items(), total=len(df), desc="Scraping sequentially"
):
    scrape_and_write(idx, url)

print(" Done! All transfers saved to compiled_transfers.csv.")
