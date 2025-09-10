import sys
import shutil
from pathlib import Path
import pandas as pd

# Add src to the Python path to allow importing our modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

from data_layer.data_manager import process_and_save_data

# Define paths for the test
RAW_DATA_FILE = Path('XAUUSD_M1_4M_clean.csv')
PROCESSED_DATA_DIR = Path('data/processed_test') # Use a separate dir for testing
EXPECTED_FILES = [
    PROCESSED_DATA_DIR / 'XAUUSD_5min.parquet',
    PROCESSED_DATA_DIR / 'XAUUSD_15min.parquet',
    PROCESSED_DATA_DIR / 'XAUUSD_1h.parquet',
    PROCESSED_DATA_DIR / 'XAUUSD_4h.parquet',
]

def setup_function():
    """Clean up before each test."""
    if PROCESSED_DATA_DIR.exists():
        shutil.rmtree(PROCESSED_DATA_DIR)

def teardown_function():
    """Clean up after each test."""
    if PROCESSED_DATA_DIR.exists():
        shutil.rmtree(PROCESSED_DATA_DIR)

def test_full_data_pipeline():
    """
    End-to-end test for the data processing pipeline.
    """
    # Arrange: Ensure the environment is clean before the test
    setup_function()

    # Act: Run the main data processing function
    process_and_save_data(RAW_DATA_FILE, PROCESSED_DATA_DIR)

    # Assert: Check that the outcomes are as expected

    # 1. Check that the directory was created
    assert PROCESSED_DATA_DIR.exists()
    assert PROCESSED_DATA_DIR.is_dir()

    # 2. Check that all expected files were created
    for f_path in EXPECTED_FILES:
        assert f_path.exists()
        assert f_path.is_file()

    # 3. Load one of the files and check its content
    df_h1 = pd.read_parquet(PROCESSED_DATA_DIR / 'XAUUSD_1h.parquet')

    # Check it's a DataFrame and is not empty
    assert isinstance(df_h1, pd.DataFrame)
    assert not df_h1.empty

    # Check for expected columns
    expected_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in expected_cols:
        assert col in df_h1.columns

    # Check that the index is a DatetimeIndex with UTC timezone
    assert isinstance(df_h1.index, pd.DatetimeIndex)
    assert str(df_h1.index.tz) == 'UTC'

    # Clean up after the test
    teardown_function()
