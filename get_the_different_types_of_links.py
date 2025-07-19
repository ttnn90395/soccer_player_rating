from urllib.parse import urlparse

import pandas as pd

# Load your CSV
df = pd.read_csv("all_players_ratings_original.csv")

# Extract and clean fbref URLs
urls = df["fbref_url"].dropna().unique()


# Function to classify URL type based on path
def classify_fbref_url(url):
    path = urlparse(url).path
    if "goallogs" in path:
        return "Goal Log"
    elif path.count("/") == 3:
        return "Player Profile"
    else:
        return "Other"


# Create a dictionary grouping URLs by type
url_types = {}
for url in urls:
    url_type = classify_fbref_url(url)
    url_types.setdefault(url_type, []).append(url)

# Print categorized URLs
for category, links in url_types.items():
    print(f"\n🔗 {category} URLs:")
    for link in links:
        print(link)
