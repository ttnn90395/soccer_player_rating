import pandas as pd

# Load the CSV file
file_path = r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_ratings.csv"
df = pd.read_csv(file_path)

# Sort by the 'Value' column (descending order)
df_sorted = df.sort_values(by="Value", ascending=False)

# Overwrite the original file with the sorted data
df_sorted.to_csv(file_path, index=False)
