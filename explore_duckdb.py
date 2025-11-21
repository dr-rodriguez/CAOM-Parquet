# Script to explore the CAOM database using DuckDB

import duckdb

FILE_PATH = "data/hats/caom/dataset/Norder=*/Dir=*/Npix=*.parquet"

# Explore the database
print("Exploring the database...")
x = duckdb.query(f"SELECT * FROM '{FILE_PATH}'")
print(x)
x = duckdb.query(f"SELECT count(*) FROM '{FILE_PATH}'")
print(x)

x = duckdb.query(f"SELECT obs_collection, count(*) FROM '{FILE_PATH}' GROUP BY obs_collection")
print(x)