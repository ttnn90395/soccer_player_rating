import re

import pandas as pd

# Load your CSV
df = pd.read_csv("all_players_ratings_original.csv")

# Drop missing fbref_url entries
urls = df["fbref_url"].fillna("")


# Function to extract player_id
def extract_player_id(url):
    match = re.search(r"/players/([a-z0-9]+)", url)
    return match.group(1) if match else ""


# Function to extract player_name from last segment
def extract_clean_name_for_url(url):
    last_segment = url.rstrip("/").split("/")[-1]
    if last_segment.lower() in [
        "goallogs",
        "matchlogs",
        "scout",
        "all_comps",
        "summary",
    ]:
        return ""
    name_parts = last_segment.split("-")
    return "-".join(name_parts[:2]) if len(name_parts) >= 2 else last_segment


# Generate new column
fbref_alltimestat = []
for url in urls:
    player_id = extract_player_id(url)
    player_name = extract_clean_name_for_url(url)
    if player_id and player_name:
        stat_url = f"https://fbref.com/en/players/{player_id}/all_comps/{player_name}-Stats---All-Competitions"
    else:
        stat_url = ""
    fbref_alltimestat.append(stat_url)

# Add to DataFrame
df["fbref_alltimestat"] = fbref_alltimestat

# Save updated CSV
df.to_csv("all_players_ratings_original_updated.csv", index=False)
