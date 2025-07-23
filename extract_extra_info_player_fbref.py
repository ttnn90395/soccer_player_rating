import json
import logging
import os
import time

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from tqdm import tqdm

# --- Setup ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(filename="scraping_errors.log", level=logging.ERROR)

# Path to your CSV
csv_path = "C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_original_updated.csv"

# Checkpoint tracking
checkpoint_path = "scraping_checkpoint.txt"
processed_indices = set()

# Load any previously processed indices
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        processed_indices = set(
            int(line.strip()) for line in f if line.strip().isdigit()
        )

# Map keys to DataFrame columns
key_to_column = {
    "height": "Height",
    "weight": "Weight",
    "position": "Positions_fbref",
    "club": "Club",
    "birth_date": "Birth_date",
    "birth_place": "Birth_place",
    "weekly_wage": "Weekly_wage",
    "instagram": "Instagram",
    "recognitions": "Recognitions",
}


# --- Function to extract player info from FBref profile ---
def extract_player_info(url):
    try:
        response = requests.get(url, verify=False, timeout=1000)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Request failed for {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    player = {key: "" for key in key_to_column}
    player["recognitions"] = []

    # JSON-LD structured data
    script_tag = soup.find("script", type="application/ld+json")
    if script_tag:
        try:
            data = json.loads(script_tag.string)
            player["height"] = (
                data.get("height", {}).get("value", "")
                if isinstance(data.get("height"), dict)
                else data.get("height", "")
            )
            player["weight"] = (
                data.get("weight", {}).get("value", "")
                if isinstance(data.get("weight"), dict)
                else data.get("weight", "")
            )
            member_of = data.get("memberOf", {})
            if isinstance(member_of, dict):
                player["club"] = member_of.get("name", "")
        except json.JSONDecodeError:
            logging.error(f"JSON decode failed for {url}")

    # Birth info
    birth_tag = soup.find("span", id="necro-birth")
    if birth_tag:
        player["birth_date"] = birth_tag.text.strip()
        next_span = birth_tag.find_next("span").find_next("span")
        if next_span:
            player["birth_place"] = next_span.text.strip()

    # Weekly wage
    wage_tag = soup.find("span", style=lambda s: s and "color:#932a12" in s)
    if wage_tag:
        player["weekly_wage"] = wage_tag.text.strip()

    # Instagram
    insta_tag = soup.find("a", href=lambda href: href and "instagram.com" in href)
    if insta_tag:
        player["instagram"] = insta_tag["href"]

    # Recognitions
    bling_tag = soup.find("span", id="bling-alt-text")
    if bling_tag and bling_tag.string:
        player["recognitions"] = [
            line.strip("* ").strip()
            for line in bling_tag.string.strip().split("\n")
            if line.strip()
        ]

    # Position
    position_tag = soup.find(string=lambda text: text and "Position:" in text)
    if position_tag:
        player["position"] = position_tag.split("Position:")[-1].strip()

    return player


# --- Load CSV and ensure required columns ---
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV not found: {csv_path}")

df = pd.read_csv(csv_path, encoding="utf-8-sig")

for col in key_to_column.values():
    if col not in df.columns:
        df[col] = ""

# --- Scraping loop with resume support ---
for index, row in tqdm(df.iterrows(), total=len(df), desc="Resumable FBref Scraping"):
    if index in processed_indices:
        continue

    url = row.get("fbref_url", "")
    if not url:
        print(f"No URL for index {index}")
        continue

    try:
        info = extract_player_info(url)
        time.sleep(2.5)
        for key, value in info.items():
            column = key_to_column.get(key)
            if not column:
                continue
            value_str = "; ".join(value) if isinstance(value, list) else value
            df.at[index, column] = value_str

        # Save progress checkpoint
        with open(checkpoint_path, "a") as f:
            f.write(f"{index}\n")

        df.to_csv(csv_path, index=False)

    except Exception as e:
        logging.error(f"Error updating index {index} for {url}: {e}")

# --- Final save (just in case) ---
df.to_csv(csv_path, index=False)
