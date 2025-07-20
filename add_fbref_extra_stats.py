import json
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Load the original CSV
csv_path = r"C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_original_updated.csv"
df = pd.read_csv(csv_path)

# New columns to add
columns_to_add = [
    "fbref_name",
    "Height",
    "Weight",
    "Position_fbref",
    "Instagram",
    "Weekly_wage",
    "Birth_place",
    "Recognitions",
]

for col in columns_to_add:
    df[col] = ""


# Function to extract player data from FBref
def get_fbref_data(url):
    result = {
        "fbref_name": "N/A",
        "Height": "N/A",
        "Weight": "N/A",
        "Position_fbref": "N/A",
        "Instagram": "N/A",
        "Weekly_wage": "N/A",
        "Birth_place": "N/A",
        "Recognitions": [],
    }

    try:
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")

        # JSON-LD block
        script_tag = soup.find("script", type="application/ld+json")
        if script_tag:
            data = json.loads(script_tag.string)

            # Height & Weight
            if isinstance(data.get("height"), dict):
                result["Height"] = data["height"].get("value", "N/A")
            elif isinstance(data.get("height"), str):
                result["Height"] = data.get("height", "N/A")

            if isinstance(data.get("weight"), dict):
                result["Weight"] = data["weight"].get("value", "N/A")
            elif isinstance(data.get("weight"), str):
                result["Weight"] = data.get("weight", "N/A")

        # Name
        name_tag = soup.find("h1")
        result["fbref_name"] = name_tag.find("span").text if name_tag else "N/A"

        # Position
        position_tag = soup.find(string=lambda text: text and "Position:" in text)
        if position_tag:
            result["Position_fbref"] = position_tag.split("Position:")[-1].strip()

        # Birth place
        birth_tag = soup.find("span", id="necro-birth")
        if birth_tag:
            next_span = birth_tag.find_next("span").find_next("span")
            result["Birth_place"] = next_span.text.strip() if next_span else "N/A"

        # Instagram
        insta_tag = soup.find("a", href=lambda href: href and "instagram.com" in href)
        result["Instagram"] = insta_tag["href"] if insta_tag else "N/A"

        # Weekly wage
        wage_tag = soup.find(
            "span", style=lambda value: value and "color:#932a12" in value
        )
        result["Weekly_wage"] = wage_tag.text.strip() if wage_tag else "N/A"

        # Recognitions
        bling_section = soup.find("span", id="bling-alt-text")
        if bling_section and bling_section.string:
            result["Recognitions"] = [
                line.strip("* ").strip()
                for line in bling_section.string.strip().split("\n")
                if line.strip()
            ]
    except Exception as e:
        print(f"Error processing {url}: {e}")

    return result


# Loop over each player and update their data
for index, row in df.iterrows():
    fbref_url = row.get("fbref_url", "")
    if fbref_url and pd.notna(fbref_url):
        print(f"Processing {fbref_url}")
        data = get_fbref_data(fbref_url)
        for col in columns_to_add:
            df.at[index, col] = (
                data[col] if col != "recognitions" else "; ".join(data[col])
            )
        time.sleep(1)  # Gentle delay to avoid hammering FBref

# Save enriched data to a new file
output_path = r"C:/Users/L1160681/OneDrive - TotalEnergies/Documents/Projet/SP/all_players_ratings_enriched.csv"
df.to_csv(output_path, index=False)
print(f"New enriched file saved to: {output_path}")
