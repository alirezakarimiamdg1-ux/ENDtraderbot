import pandas as pd
from pathlib import Path

def load_csv_data(file_path: Path) -> pd.DataFrame:
    """
    Loads M1 financial data from a CSV file into a pandas DataFrame.

    The function expects the CSV to have the following columns:
    'time', 'open', 'high', 'low', 'close', 'volume'.
    Additional columns will be kept.

    Args:
        file_path: The path to the CSV file.

    Returns:
        A pandas DataFrame with the time column set as a datetime index.
        Returns an empty DataFrame if the file is not found or is empty.
    """
    if not file_path.is_file():
        print(f"Error: File not found at {file_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(
            file_path,
            parse_dates=['time'],
            index_col='time',
            skipinitialspace=True
        )

        if df.empty:
            print(f"Warning: CSV file at {file_path} is empty.")
            return pd.DataFrame()

        print(f"Successfully loaded {len(df)} rows from {file_path}")
        return df

    except Exception as e:
        print(f"Error loading CSV file at {file_path}: {e}")
        return pd.DataFrame()

if __name__ == '__main__':
    # Example usage:
    # Assuming the script is run from the root of the project.
    data_file = Path('XAUUSD_M1_4M_clean.csv')

    df = load_csv_data(data_file)

    if not df.empty:
        print("\nData loaded successfully. Here are the first 5 rows:")
        print(df.head())
        print("\nDataFrame Info:")
        df.info()
