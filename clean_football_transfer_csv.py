import csv

import pandas as pd

fixed_rows = []
expected_columns = 10

with open("most_valuable_players_fast.csv", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()

        # If entire line is wrapped in quotes, unwrap it first
        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]

        # Fix escaped quotes like ""M, AM (R)"" to "M, AM (R)"
        line = line.replace('""', '"')

        # Parse line using csv.reader for proper quote handling
        parsed = next(csv.reader([line], quotechar='"', skipinitialspace=True))

        # If the row looks good, keep it
        if len(parsed) == expected_columns:
            fixed_rows.append(parsed)
        elif len(parsed) > expected_columns:
            # Try merging overflow into Positions field
            repaired = parsed[:5] + [", ".join(parsed[5:-4])] + parsed[-4:]
            if len(repaired) == expected_columns:
                fixed_rows.append(repaired)
            else:
                print(f"Could not fix row: {parsed}")

# Load into DataFrame
columns = [
    "Name",
    "Player URL",
    "Age",
    "Team",
    "Team Link",
    "Positions",
    "Nationality",
    "Rating",
    "Potential",
    "Value",
]
df = pd.DataFrame(fixed_rows, columns=columns)

# Convert numeric columns
for col in ["Age", "Rating", "Potential", "Value"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.drop(index=0).reset_index(drop=True)
# Define the redundant prefix
prefix = "https://www.footballtransfers.com"

# Fix repeated prefix in all relevant URL columns
for col in ["Player URL", "Team Link"]:
    if col in df.columns:
        df[col] = df[col].str.replace(f"{prefix}{prefix}", prefix, regex=False)

df.to_csv("all_players_ratings_original.csv")
