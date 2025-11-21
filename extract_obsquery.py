"""Data extraction script for CAOM ObsQuery view.

This script queries the ObsQuery view in chunks and saves the results as
CSV files in the data/raw directory.
"""

import os
import glob
import pandas as pd
import db_connection

# Configuration
LIMIT = 100000000
CHUNK_SIZE = 100000
OUTPUT_DIR = os.path.join("data", "raw")
TABLE_NAME = "ObsQuery"  # Or the specific view name if different
MISSION = "TESS"


def get_next_chunk_number(output_dir):
    """Get the next available chunk number by checking existing files."""
    pattern = os.path.join(output_dir, "obsquery_chunk_*.csv")
    existing_files = glob.glob(pattern)
    
    if not existing_files:
        return 0
    
    # Extract chunk numbers from filenames
    chunk_numbers = []
    for file in existing_files:
        filename = os.path.basename(file)
        # Extract number from "obsquery_chunk_XXXXX.csv"
        try:
            chunk_num = int(filename.split("_")[-1].split(".")[0])
            chunk_numbers.append(chunk_num)
        except (ValueError, IndexError):
            continue
    
    if chunk_numbers:
        return max(chunk_numbers) + 1
    return 0


def extract_data():
    """Extract data from ObsQuery view in chunks."""
    print(f"Starting extraction from {TABLE_NAME}...")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Get the next available chunk number to avoid overwriting existing files
    start_chunk_num = get_next_chunk_number(OUTPUT_DIR)
    print(f"Starting chunk numbering from {start_chunk_num}")

    engine = db_connection.get_engine()

    # Query to select all columns
    query = f"SELECT TOP ({LIMIT}) * FROM {TABLE_NAME} WHERE obs_collection = '{MISSION}'"

    # Use pandas read_sql with chunksize
    # This returns an iterator
    chunks = pd.read_sql(query, engine, chunksize=CHUNK_SIZE)

    total_rows = 0
    chunk_count = 0

    for i, chunk in enumerate(chunks):
        chunk_count += 1
        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk

        chunk_num = start_chunk_num + i
        output_file = os.path.join(OUTPUT_DIR, f"obsquery_chunk_{chunk_num:05d}.csv")

        # Save to CSV
        # index=False to avoid saving the pandas index
        chunk.to_csv(output_file, index=False)

        print(f"Processed chunk {chunk_num}: {rows_in_chunk} rows. Saved to {output_file}")

    print(f"Extraction complete. Total rows: {total_rows}. Total chunks: {chunk_count}")


if __name__ == "__main__":
    extract_data()
