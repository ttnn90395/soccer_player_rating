import re

import pandas as pd

# Load your CSV
df = pd.read_csv("all_players_ratings_original_updated.csv")

# Drop missing fbref_url entries
urls = df["fbref_url"].fillna("")


# Function to extract player_id
def extract_player_id(url):
    match = re.search(r"/players/([a-z0-9]+)", url)
    return match.group(1) if match else ""


# Generate new column
fbref_player_id = []
for url in urls:
    player_id = extract_player_id(url)

    fbref_player_id.append(player_id)

# Add to DataFrame
df["player_id"] = fbref_player_id

# Save updated CSV
df.to_csv("all_players_ratings_original_updated.csv", index=False)
