import os
import glob
from hats_import.pipeline import pipeline
from hats_import.catalog.arguments import ImportArguments
import config


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

    # Configure arguments
    args = ImportArguments(
        output_path=output_path,
        output_artifact_name=output_artifact_name,
        ra_column=config.RA_COLUMN,
        dec_column=config.DEC_COLUMN,
        sort_columns=config.SORT_COLUMN,
        progress_bar=True,
        file_reader="csv",
        input_file_list=input_files,
    )

    # Run the pipeline
    pipeline(args)

    print("HATS import completed successfully.")


if __name__ == "__main__":
    main()
