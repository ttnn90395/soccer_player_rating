import os
import threading
import zipfile

# Define the path to the parent directory containing the folders
parent_dir = r"C:\Users\L1160681\OneDrive - TotalEnergies\Documents\Projet\SP\all_players_fbref_tables"

# Set the maximum number of concurrent threads
max_threads = 7
semaphore = threading.Semaphore(max_threads)


def zip_folder(folder_name):
    with semaphore:
        folder_path = os.path.join(parent_dir, folder_name)
        zip_path = os.path.join(parent_dir, f"{folder_name}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, start=folder_path)
                    zipf.write(full_path, arcname)
        print(f"Zipped: {folder_name}")


# Create and start threads
threads = []
for folder_name in os.listdir(parent_dir):
    folder_path = os.path.join(parent_dir, folder_name)
    if os.path.isdir(folder_path):
        t = threading.Thread(target=zip_folder, args=(folder_name,))
        t.start()
        threads.append(t)

# Wait for all threads to finish
for t in threads:
    t.join()

print("All folders have been zipped.")
