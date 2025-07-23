import pandas as pd

# Load the CSV
df = pd.read_csv(
    r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_ratings_original_updated.csv"
)

# Define the columns to remove
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

# Drop the columns
df.drop(columns=key_to_column.values(), inplace=True)
df.to_csv(
    r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_ratings_original_updated.csv",
    index=False,
)
