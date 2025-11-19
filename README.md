# CAOM-Parquet
Hack-day exploration of Parquet files for the astronomical database of Observations, CAOM.   
Column fields at https://mast.stsci.edu/api/v0/_c_a_o_mfields.html 

## Setup
- `uv sync`
- Installs dependencies:
  - `sqlalchemy` (MSSQL driver)
  - `pyodbc` (MSSQL connection)
  - `pandas` (dataframe operations)
  - `pyarrow` (Parquet support)
  - `lsdb` (HATS exploration)
  - `hats-import` (HATS format conversion)

## Database Configuration
- Update `config.py` file for database details.

## Data Extraction
- Run `extract_obsquery.py` to extract data from the database.
- Saves intermediate csv files in `data/raw` directory.

## HATS Format Conversion
- Run `hats-load.py` to convert extracted data to HATS format.
- Saves HATS-formatted files in `data/hats` directory.

## Exploration with LSDB
- Run `explore_lsdb.py` to explore the HATS-formatted data.

