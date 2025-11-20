import lsdb
import pandas as pd
import matplotlib.pyplot as plt
import os

# Placeholder path to the HATS catalog
# In a real scenario, this would be the path to the directory containing the HATS data
HATS_PATH = os.path.join("data", "hats", "caom")


def main():
    print(f"Attempting to load HATS catalog from {HATS_PATH}...")

    # Check if directory exists to avoid immediate crash if run without data
    if not os.path.exists(HATS_PATH):
        print(f"WARNING: Path {HATS_PATH} does not exist.")
        print("This script is running in DEMO mode with placeholder comments.")
        print(
            "To run with real data, ensure 'data/hats' contains a valid HATS catalog."
        )
        return

    try:
        # 1. Load HATS dataset using LSDB
        # lsdb.read_hats() reads the metadata and prepares the catalog for lazy loading
        catalog = lsdb.read_hats(HATS_PATH)
        print("Catalog loaded successfully!")
        print(catalog)

        # Display basic info
        print("\nCatalog info:")
        print(f"  Name: {catalog.name}")
        print(f"  Total rows (estimated): {catalog.count()}")

        execute_queries(catalog)

    except Exception as e:
        print(f"Could not load catalog: {e}")
        print(
            "Ensure the directory contains a valid dataset_properties file and partition info."
        )


def execute_queries(catalog):
    """Executes actual LSDB queries on the loaded catalog."""

    # 2. Test basic queries (spatial searches, filtering)
    print("\n--- Basic Queries ---")

    # Example: Cone Search
    # Find objects within 1 degree of a specific RA/Dec
    # CAOM uses 's_ra' and 's_dec' for coordinates
    ra_center = 210.8  # Example: M101
    dec_center = 54.3
    radius_deg = 1.0

    print(
        f"Performing cone search at RA={ra_center}, Dec={dec_center}, radius={radius_deg} deg..."
    )
    try:
        # lsdb automatically detects spatial columns if properly defined in HATS metadata
        # otherwise we might need to specify them, but HATS usually handles this.
        cone_search_result = catalog.cone_search(
            ra=ra_center, dec=dec_center, radius=radius_deg
        )
        count = cone_search_result.count()
        print(f"Found {count} objects in cone.")
    except Exception as e:
        print(f"Cone search failed: {e}")

    # Example: Filtering
    # Filter by a column value
    # Common CAOM fields: 'obs_collection', 'instrument_name', 'target_name', 't_exptime'
    try:
        print("Filtering for objects (example query)...")
        # Example: Filter for HST observations
        filtered_catalog = catalog.query("obs_collection == 'HST'")
        print(f"Found {filtered_catalog.count()} HST observations.")

        # Example: Filter by exposure time
        filtered_catalog = catalog.query("t_exptime > 1000")
        print(f"Found {filtered_catalog.count()} observations with t_exptime > 1000.")
    except Exception as e:
        print(f"Filtering example skipped: {e}")

    # 3. Demonstrate cross-matching or spatial joins
    print("\n--- Cross-matching ---")
    print("Cross-matching requires a second catalog. Skipping for single catalog demo.")
    # xmatch_result = catalog.crossmatch(other_catalog, radius_arcsec=1.0)

    # 4. Create simple visualization/analysis examples
    print("\n--- Visualization ---")

    try:
        # compute() triggers the actual data loading
        print("Loading top 1000 rows for visualization...")
        df = catalog.head(1000).compute()

        # CAOM spatial columns
        ra_col = "s_ra"
        dec_col = "s_dec"

        # Fallback check if columns exist (in case of variation)
        if ra_col not in df.columns and "RA" in df.columns:
            ra_col = "RA"
        if dec_col not in df.columns and "Dec" in df.columns:
            dec_col = "Dec"

        if not df.empty and ra_col in df.columns and dec_col in df.columns:
            plt.figure(figsize=(10, 6))
            plt.scatter(df[ra_col], df[dec_col], s=5, alpha=0.5)
            plt.xlabel(f"Right Ascension ({ra_col})")
            plt.ylabel(f"Declination ({dec_col})")
            plt.title("Spatial Distribution of Sample Objects")
            plt.grid(True)

            output_plot = "spatial_distribution_example.png"
            plt.savefig(output_plot)
            print(f"Saved visualization to {output_plot}")
        else:
            print(
                f"Skipping visualization. Expected columns '{ra_col}', '{dec_col}' not found. Available: {df.columns}"
            )
    except Exception as e:
        print(f"Visualization failed: {e}")


if __name__ == "__main__":
    main()
