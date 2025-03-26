import os
import pandas as pd

# === CONFIGURATION ===
BATCH_DIR = '../data/raw'
OUTPUT_FILE = '../data/raw/politics_comments.csv'

# === MERGE FUNCTION ===
def merge_batches(batch_dir, output_file, prefix="politics_hot"):
    """Merge all batch CSVs into a single file."""
    batch_files = [f for f in os.listdir(batch_dir) if f.startswith(prefix) and f.endswith('.csv')]

    if not batch_files:
        print("No batch files found.")
        return

    print(f"Found {len(batch_files)} batch files. Merging...")

    merged_df = pd.concat(
        [pd.read_csv(os.path.join(batch_dir, batch_file)) for batch_file in batch_files],
        ignore_index=True
    )

    # Save merged file
    merged_df.to_csv(output_file, index=False)
    print(f"Saved merged data to {output_file} with {len(merged_df)} rows.")

# === RUN SCRIPT ===
merge_batches(BATCH_DIR, OUTPUT_FILE)

print("Merging complete.")
