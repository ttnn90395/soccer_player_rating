import json
import time

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def extract_player_info(url):
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")

    player = {
        "height": "",
        "weight": "",
        "position": "",
        "club": "",
        "birth_date": "",
        "birth_place": "",
        "weekly_wage": "",
        "instagram": "",
        "recognitions": [],
    }

    # Extract from JSON-LD script
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
            pass

    # Birth info
    birth_tag = soup.find("span", id="necro-birth")
    if birth_tag:
        player["birth_date"] = birth_tag.text.strip()
        next_span = birth_tag.find_next("span").find_next("span")
        player["birth_place"] = next_span.text.strip() if next_span else ""

    # Weekly wage
    wage_tag = soup.find("span", style=lambda value: value and "color:#932a12" in value)
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


# --- Load CSV and update ---
csv_path = "C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_original_updated.csv"
df = pd.read_csv(csv_path)

# Create columns if they don't exist
columns_to_create = [
    "Height",
    "Weight",
    "Positions_fbref",
    "Club",
    "Birth_date",
    "Birth_place",
    "Weekly_wage",
    "Instagram",
    "Recognitions",
]
for col in columns_to_create:
    if col not in df.columns:
        df[col] = ""

# Loop through each player URL
for index, row in df.iterrows():
    url = row.get("fbref_url", "")
    if not url:
        continue
    try:
        info = extract_player_info(url)
        for key in info:
            value = "; ".join(info[key]) if isinstance(info[key], list) else info[key]
            df.at[
                index, "Positions_fbref" if key == "position" else key.capitalize()
            ] = value if value not in ["N/A", None] else ""
        time.sleep(2)  # Prevent hammering the site
    except Exception as e:
        print(f"Error processing {url}: {e}")

# Save updated data
df.to_csv(csv_path, index=False)
