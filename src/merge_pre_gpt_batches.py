import os
import pandas as pd

# === CONFIGURATION ===
BATCH_DIR = '../data/pre_gpt'
OUTPUT_FILE = '../data/archive/pre_GPT_comments.csv'

# === MERGE BATCH FILES ===
def merge_batches(batch_dir, output_file):
    """Merge all batch CSVs into a single CSV."""
    
    # Collect all batch files
    batch_files = [f for f in os.listdir(batch_dir) if f.startswith("pre_gpt_batch") and f.endswith(".csv")]
    batch_files.sort()  # Ensure files are merged in order

    if not batch_files:
        print("No batch files found.")
        return

    print(f"Found {len(batch_files)} batch files. Merging...")

    # Load and append all batch files
    merged_df = pd.concat(
        (pd.read_csv(os.path.join(batch_dir, batch_file)) for batch_file in batch_files),
        ignore_index=True
    )

    # Save the merged DataFrame to a single CSV file
    merged_df.to_csv(output_file, index=False)
    print(f"Successfully merged {len(batch_files)} batches into {output_file} with {len(merged_df)} rows!")

# === RUN SCRIPT ===
if __name__ == "__main__":
    merge_batches(BATCH_DIR, OUTPUT_FILE)
