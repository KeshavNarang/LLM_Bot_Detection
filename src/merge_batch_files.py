import os
import pandas as pd
import shutil

# === FILE PATHS ===
SRC_DIR = 'data/raw'  # Location where batch files are currently stored
DEST_DIR = '../data/raw'      # Final destination directory
os.makedirs(DEST_DIR, exist_ok=True)

# === FUNCTION TO MERGE CSV BATCHES ===
def merge_batches(batch_prefix, output_filename):
    """Merge all batch CSV files into a single CSV."""
    batch_files = [f for f in os.listdir(SRC_DIR) if f.startswith(batch_prefix) and f.endswith('.csv')]
    batch_files.sort()  # Ensure proper order

    if not batch_files:
        print(f"No batch files found for {batch_prefix}")
        return

    print(f"Merging {len(batch_files)} batch files for {batch_prefix}...")

    # Load and concatenate all batch CSVs
    dfs = [pd.read_csv(os.path.join(SRC_DIR, batch)) for batch in batch_files]
    merged_df = pd.concat(dfs, ignore_index=True)

    # Save the merged CSV in the destination directory
    output_path = os.path.join(DEST_DIR, output_filename)
    merged_df.to_csv(output_path, index=False)
    print(f"Saved merged file to {output_path}")

# === MERGE TOP AND HOT BATCHES ===
merge_batches('top', 'top100.csv')
merge_batches('hot', 'hot100.csv')

# === CLEANUP: REMOVE BATCH FILES ===
print("Cleaning up batch files...")
for batch_file in os.listdir(SRC_DIR):
    if batch_file.startswith('top') or batch_file.startswith('hot'):
        os.remove(os.path.join(SRC_DIR, batch_file))
        print(f"Deleted {batch_file}")

# === REMOVE THE TEMPORARY DATA FOLDER IN SRC ===
if not os.listdir(SRC_DIR):
    print(f"Removing temporary batch directory: {SRC_DIR}")
    shutil.rmtree(SRC_DIR)

print("Batch merging and cleanup complete.")
