import os
import shutil

# Define source and destination folders
source_folder = r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_fbref_tables"
destination_folder = r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_fbref_tables_zip"

# Ensure the destination folder exists
os.makedirs(destination_folder, exist_ok=True)

# Move all .zip files
for filename in os.listdir(source_folder):
    if filename.endswith(".zip"):
        src_path = os.path.join(source_folder, filename)
        dst_path = os.path.join(destination_folder, filename)
        shutil.move(src_path, dst_path)

print("All zip files have been moved.")
