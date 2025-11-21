import os
import glob
from hats_import.pipeline import pipeline
from hats_import.catalog.arguments import ImportArguments
from hats_import.catalog.file_readers.csv import CsvReader
import config


class FilteredCsvReader(CsvReader):
    """
    Custom CSV reader that filters out rows with invalid coordinates.
    
    Filters rows where:
    - Declination is outside [-90, 90] degrees
    - Declination is NaN or None
    - RA is NaN or None (RA can wrap, so we only check for NaN)
    """
    
    def __init__(self, ra_column="s_ra", dec_column="s_dec", **kwargs):
        super().__init__(**kwargs)
        self.ra_column = ra_column
        self.dec_column = dec_column
    
    def read(self, input_file, read_columns=None):
        """
        Read CSV file and filter out invalid coordinates.
        """
        for chunk_df in super().read(input_file, read_columns=read_columns):
            # Filter out rows with invalid coordinates
            filtered_chunk = chunk_df
            if self.ra_column in chunk_df.columns and self.dec_column in chunk_df.columns:
                # Create mask for valid coordinates
                valid_mask = (
                    chunk_df[self.dec_column].notna() &
                    (chunk_df[self.dec_column] >= -90.0) &
                    (chunk_df[self.dec_column] <= 90.0) &
                    chunk_df[self.ra_column].notna()
                )
                
                # Count filtered rows for logging
                # filtered_count = (~valid_mask).sum()
                # if filtered_count > 0:
                #     print(f"Warning: Filtered {filtered_count} rows with invalid coordinates from {input_file}")
                
                # Return only valid rows
                filtered_chunk = chunk_df[valid_mask]
            
            # Only yield non-empty chunks
            if len(filtered_chunk) > 0:
                yield filtered_chunk


def get_caom_type_map():
    """
    Create a type mapping based on CAOM field descriptions.
    See https://mast.stsci.edu/api/v0/_c_a_o_mfields.html
    
    All mappings verified against the official CAOM field specifications.
    """
    return {
        # String fields (per CAOM spec: string data type)
        "dataproduct_type": "string",  # CAOM: Product Type (IMAGE, SPECTRUM, SED, etc.)
        "obs_collection": "string",  # CAOM: Mission/Collection (SWIFT, PS1, HST, IUE)
        "obs_id": "string",  # CAOM: Observation ID (string, e.g., "U24Z0101T", "N4QF18030")
        "obs_publisher_did": "string",  # Not in CAOM docs, inferred as string
        "obsType": "string",  # Not in CAOM docs, inferred as string
        "intentType": "string",  # CAOM: Observation Type (science, calibration)
        "target_name": "string",  # CAOM: Target Name
        "wavelength_region": "string",  # CAOM: Waveband (EUV, XRAY, OPTICAL)
        "instrument_name": "string",  # CAOM: Instrument Name (e.g., WFPC2/WFC, UVOT)
        "filters": "string",  # CAOM: Filters (multi-value separator ';')
        "pol_states": "string",  # Not in CAOM docs, inferred as string
        "target_classification": "string",  # CAOM: Target Classification (multi-value separator ';')
        "obs_title": "string",  # CAOM: Observation Title
        "facility_name": "string",  # Not in CAOM docs, inferred as string
        "proposal_pi": "string",  # CAOM: Principal Investigator (last name)
        "proposal_id": "string",  # CAOM: Proposal ID (e.g., "EGCJC", "11360")
        "proposal_type": "string",  # CAOM: Proposal Type (3PI, GO, GO/DD, HLA, etc.)
        "provenance_name": "string",  # CAOM: Provenance Name (e.g., TASOC, CALSTIS, PS1)
        "proposal_project": "string",  # Not in CAOM docs, inferred as string
        "s_region": "string",  # CAOM: STC/S Footprint (ICRS circle or polygon)
        "previewURI": "string",  # Not in CAOM docs (similar to jpegURL), inferred as string
        "productURI": "string",  # Not in CAOM docs, inferred as string
        "dataRights": "string",  # CAOM: Data Rights (public, exclusive_access, restricted)
        "metaDataRights": "string",  # Not in CAOM docs, inferred as string
        # Integer fields (per CAOM spec: integer data type)
        "calib_level": "int64",  # CAOM: Calibration Level (0-4: raw to contributed science product)
        "max_calib_level": "int64",  # Not in CAOM docs, inferred as integer (similar to calib_level)
        "sequence_number": "int64",  # CAOM: Sequence Number (e.g., Kepler quarter, TESS sector)
        "obsid": "int64",  # CAOM: Product Group ID (Long integer, e.g., 2007590987)
        "objID": "int64",  # CAOM: Object ID (Long integer, e.g., 2012969445)
        # Float fields (per CAOM spec: float data type)
        "s_ra": "float64",  # CAOM: RA (decimal degrees)
        "s_dec": "float64",  # CAOM: Dec (decimal degrees)
        "t_min": "float64",  # CAOM: Start Time (MJD)
        "t_max": "float64",  # CAOM: End Time (MJD)
        "t_exptime": "float64",  # CAOM: Exposure Length (seconds)
        "em_min": "float64",  # CAOM: Min. Wavelength (nm)
        "em_max": "float64",  # CAOM: Max. Wavelength (nm)
        "t_obs_release": "float64",  # CAOM: Release Date (MJD)
        "srcDen": "float64",  # CAOM: Number of Catalog Objects
        # Boolean fields (per CAOM spec: boolean, but can be empty/null)
        # Using string to handle empty values that would cause type conversion issues
        "mtFlag": "string",  # CAOM: Moving Target Flag (boolean, but can be absent/empty)
    }


def main():
    # Define input files
    input_dir = os.path.join("data", "raw")
    input_files = glob.glob(os.path.join(input_dir, "**", "*.csv"), recursive=True)

    if not input_files:
        print(f"No CSV files found in {input_dir}")
        return

    print(f"Found {len(input_files)} CSV files to process.")

    # Define output path
    output_path = os.path.join("data", "hats")
    output_artifact_name = "caom"

    # Create filtered CSV reader with type mapping and low_memory=False to avoid mixed type warnings
    # This reader will filter out rows with invalid coordinates before HEALPix conversion
    csv_reader = FilteredCsvReader(
        ra_column=config.RA_COLUMN,
        dec_column=config.DEC_COLUMN,
        type_map=get_caom_type_map(),
        chunksize=500000,
        header="infer",
        low_memory=False,  # Pass to pandas to avoid mixed type warnings
    )

    # Configure arguments
    args = ImportArguments(
        output_path=output_path,
        output_artifact_name=output_artifact_name,
        ra_column=config.RA_COLUMN,
        dec_column=config.DEC_COLUMN,
        sort_columns=config.SORT_COLUMN,
        progress_bar=True,
        file_reader=csv_reader,
        input_file_list=input_files,
    )

    # Run the pipeline
    pipeline(args)

    print("HATS import completed successfully.")


if __name__ == "__main__":
    main()
