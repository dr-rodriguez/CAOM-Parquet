#caom #hack-ideas 

We want to learn more about parquet by converting the CAOM (Common Archive Observation Model) MSSQL database into Parquet.

For some context, CAOM contains a number of tables and views, including:
- CaomObservation - main observation data
- CaomPlane - spatial, spectral, wavelength information
- CaomArtifact - file information
- CaomMembers - relationships between observations
- ObsQuery - view that joins Observation and Plane information

We want to utilize the HATS (Hierarchical Adaptive Tiling Scheme) format, which is used for spatial exploring databases stored as parquet files.   
We then want to use the LSDB python package to explore this dataset.   
This page describes how to use LSDB and HATS: https://docs.lsdb.io/en/latest/tutorials/import_catalogs.html   
We may also need to use the `hats-import` tool to better handle the large number of entries in CAOM: https://hats-import.readthedocs.io/en/latest/index.html   
We likely want to focus on just the ObsQuery view as it condenses the information, but it does have about 300 million rows.

Other considerations:
- uv for package/environment management
- SQLAlchemy and/or pyodbc for database access
- Pandas for dataframe management
- lsdb for HATS format
- hats-import for creating parquet files in HATS format

Make a plan outlining the steps we would need to take to carry this out in a hack-day scenario.
