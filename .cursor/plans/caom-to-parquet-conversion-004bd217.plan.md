<!-- 004bd217-130a-4341-8d12-52339b1c41d5 6e5c9c10-ea39-4e9e-8d4c-2662d2333f5c -->
# CAOM to Parquet Conversion Plan

## Overview

Convert the CAOM MSSQL database's ObsQuery view (~300M rows) into HATS-formatted Parquet files for exploration with LSDB. This plan assumes a hack-day timeframe with focus on getting a working pipeline.

## Prerequisites & Setup

### 1. Environment Setup

- Initialize `uv` project: `uv init` or `uv venv`
- Install dependencies:
  - `sqlalchemy` (MSSQL driver)
  - `pyodbc` (MSSQL connection)
  - `pandas` (dataframe operations)
  - `pyarrow` (Parquet support)
  - `lsdb` (HATS exploration)
  - `hats-import` (HATS format conversion)
- Create `requirements.txt` or `pyproject.toml` with pinned versions

### 2. Database Configuration

- Create `config.py` or `.env` file for database credentials:
  - Server/host
  - Database name
  - Authentication (Windows/username-password)
  - Connection string template
- Test database connection with a simple query

## Data Extraction Pipeline

### 3. Database Connection Module

- Create `db_connection.py`:
  - SQLAlchemy engine setup for MSSQL
  - Connection pooling for large queries
  - Error handling and retry logic

### 4. Data Extraction Script

- Create `extract_obsquery.py`:
  - Query ObsQuery view in batches (chunked reads to handle 300M rows)
  - Use `pd.read_sql()` with `chunksize` parameter (e.g., 100K-1M rows per chunk)
  - Identify spatial columns (RA, Dec, or similar) for HATS tiling
  - Handle data types and nulls appropriately
  - Save intermediate Parquet files per chunk (optional, for recovery)

### 5. Data Validation

- Verify column names and types
- Check spatial coordinate ranges
- Sample data quality checks
- Estimate total size and processing time

## HATS Format Conversion

### 6. HATS Import Setup

- Review `hats-import` documentation for spatial column requirements
- Identify spatial columns in ObsQuery (likely RA/Dec or similar)
- Determine appropriate HATS parameters (tile size, depth, etc.)

### 7. Convert to HATS Format

- Use `hats-import` CLI or Python API to convert extracted data
- Process in batches if needed (hats-import may handle chunking)
- Output to `./data/hats/` directory
- Verify HATS structure (tiles, metadata files)

## Exploration with LSDB

### 8. LSDB Integration

- Create `explore_lsdb.py`:
  - Load HATS dataset using LSDB
  - Test basic queries (spatial searches, filtering)
  - Demonstrate cross-matching or spatial joins
  - Create simple visualization/analysis examples

## Hack-Day Considerations

### 9. Optimization & Testing

- Start with sample (1-10M rows) to validate pipeline
- Monitor memory usage during extraction
- Test HATS conversion on sample before full run
- Create progress logging for long-running operations

### 10. Documentation

- Document connection setup
- Note any issues encountered
- Record processing times and resource usage
- Create example LSDB queries

## File Structure

```
.
├── config.py (or .env)
├── db_connection.py
├── extract_obsquery.py
├── explore_lsdb.py
├── requirements.txt (or pyproject.toml)
├── data/
│   ├── raw/ (intermediate parquet files, optional)
│   └── hats/ (final HATS-formatted output)
└── README.md (usage instructions)
```

## Key Challenges to Address

- Memory management for 300M row extraction (chunked processing)
- Spatial column identification and validation
- HATS parameter tuning for optimal performance
- Database query performance (indexes, query optimization)
- Processing time estimation and progress tracking

### To-dos

- [ ] Set up Python environment with uv, install dependencies (sqlalchemy, pyodbc, pandas, pyarrow, lsdb, hats-import)
- [ ] Create database configuration module with connection string and credentials management
- [ ] Implement database connection module using SQLAlchemy with MSSQL driver and connection pooling
- [ ] Create data extraction script that queries ObsQuery view in chunks and saves to intermediate Parquet files
- [ ] Add data validation to verify column types, spatial coordinates, and data quality
- [ ] Use hats-import to convert extracted Parquet files to HATS format, identifying spatial columns
- [ ] Create LSDB exploration script to load HATS dataset and demonstrate spatial queries
- [ ] Test entire pipeline on sample data (1-10M rows) before processing full dataset