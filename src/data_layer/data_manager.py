from pathlib import Path
import pandas as pd

# Import the functions from other modules in the data layer
from .data_loader import load_csv_data
from .data_validator import validate_data
from .resampler import resample_timeframe

def process_and_save_data(raw_file_path: Path, processed_dir: Path):
    """
    Orchestrates the full data processing pipeline:
    1. Loads raw M1 data from CSV.
    2. Validates the data.
    3. Resamples it to multiple target timeframes.
    4. Saves the processed dataframes as Parquet files.
    """
    print("--- Starting Data Processing Pipeline ---")

    # 1. Load Data
    m1_df = load_csv_data(raw_file_path)
    if m1_df.empty:
        print("Pipeline stopped: Could not load data.")
        return

    # 2. Validate Data
    if not validate_data(m1_df):
        print("Pipeline stopped: Data validation failed.")
        return

    # 3. Create processed data directory
    processed_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created/ensured output directory exists at {processed_dir}")

    # 4. Resample and Save
    target_timeframes = ['5min', '15min', '1h', '4h']
    # Extracts 'XAUUSD' from a filename like 'XAUUSD_M1_4M_clean'
    base_symbol = raw_file_path.stem.split('_')[0]

    for tf in target_timeframes:
        print(f"Processing timeframe: {tf}...")
        resampled_df = resample_timeframe(m1_df.copy(), tf)

        if not resampled_df.empty:
            output_filename = f"{base_symbol}_{tf}.parquet"
            output_path = processed_dir / output_filename

            try:
                resampled_df.to_parquet(output_path, engine='pyarrow')
                print(f"Successfully saved {output_path}")
            except Exception as e:
                print(f"Error saving file to {output_path}: {e}")
        else:
            print(f"Skipping empty resampled dataframe for timeframe {tf}.")

    print("\n--- Data Processing Pipeline Finished ---")


if __name__ == '__main__':
    # Define file paths relative to the project root
    RAW_DATA_FILE = Path('XAUUSD_M1_4M_clean.csv')
    PROCESSED_DATA_DIR = Path('data/processed')

    process_and_save_data(RAW_DATA_FILE, PROCESSED_DATA_DIR)
