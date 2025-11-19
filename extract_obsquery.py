"""Data extraction script for CAOM ObsQuery view.

This script queries the ObsQuery view in chunks and saves the results as
CSV files in the data/raw directory.
"""

import os
import pandas as pd
import db_connection

# Configuration
LIMIT = 1000000
CHUNK_SIZE = 100000
OUTPUT_DIR = os.path.join("data", "raw")
TABLE_NAME = "ObsQuery"  # Or the specific view name if different


def extract_data():
    """Extract data from ObsQuery view in chunks."""
    print(f"Starting extraction from {TABLE_NAME}...")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    engine = db_connection.get_engine()

    # Query to select all columns
    query = f"SELECT TOP ({LIMIT}) * FROM {TABLE_NAME}"

    # Use pandas read_sql with chunksize
    # This returns an iterator
    chunks = pd.read_sql(query, engine, chunksize=CHUNK_SIZE)

    total_rows = 0
    chunk_count = 0

    for i, chunk in enumerate(chunks):
        chunk_count += 1
        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk

        output_file = os.path.join(OUTPUT_DIR, f"obsquery_chunk_{i:05d}.csv")

        # Save to CSV
        # index=False to avoid saving the pandas index
        chunk.to_csv(output_file, index=False)

        print(f"Processed chunk {i}: {rows_in_chunk} rows. Saved to {output_file}")

    print(f"Extraction complete. Total rows: {total_rows}. Total chunks: {chunk_count}")


if __name__ == "__main__":
    extract_data()
