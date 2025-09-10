import pandas as pd

def validate_data(df: pd.DataFrame) -> bool:
    """
    Validates the raw financial data DataFrame.

    Checks for:
    1. Presence of required columns.
    2. Missing (NaN) values in key columns.
    3. Data integrity (e.g., low <= high).

    Args:
        df: The pandas DataFrame to validate.

    Returns:
        True if the data is valid, False otherwise.
    """
    if df.empty:
        print("Validation Error: DataFrame is empty.")
        return False

    # 1. Check for required columns
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Validation Error: Missing required columns: {missing_columns}")
        return False

    # 2. Check for missing values in key columns
    if df[required_columns].isnull().values.any():
        print("Validation Error: Missing values found in required columns.")
        # Optional: print details of rows with missing values
        # print(df[df[required_columns].isnull().any(axis=1)])
        return False

    # 3. Check for data integrity
    # low should be the minimum of the candle, high should be the maximum
    if not (df['low'] <= df['open']).all() or \
       not (df['low'] <= df['high']).all() or \
       not (df['low'] <= df['close']).all():
        print("Validation Error: Found rows where 'low' is not the minimum price.")
        # Optional: print problematic rows
        # print(df[~((df['low'] <= df['open']) & (df['low'] <= df['high']) & (df['low'] <= df['close']))])
        return False

    if not (df['high'] >= df['open']).all() or \
       not (df['high'] >= df['low']).all() or \
       not (df['high'] >= df['close']).all():
        print("Validation Error: Found rows where 'high' is not the maximum price.")
        # Optional: print problematic rows
        # print(df[~((df['high'] >= df['open']) & (df['high'] >= df['low']) & (df['high'] >= df['close']))])
        return False

    print("Data validation successful: All checks passed.")
    return True

if __name__ == '__main__':
    from pathlib import Path
    from .data_loader import load_csv_data

    # Load the data first
    data_file = Path('XAUUSD_M1_4M_clean.csv')
    df_loaded = load_csv_data(data_file)

    if not df_loaded.empty:
        print("\n--- Running Validation ---")
        is_valid = validate_data(df_loaded)
        print(f"Validation result: {'OK' if is_valid else 'Failed'}")

        # Example of invalid data for testing purposes
        print("\n--- Testing with invalid data ---")
        df_invalid = df_loaded.copy()
        # Introduce an error
        df_invalid.loc[df_invalid.index[5], 'high'] = df_invalid.loc[df_invalid.index[5], 'low'] - 1
        is_valid_test = validate_data(df_invalid)
        print(f"Validation result for intentionally broken data: {'OK' if is_valid_test else 'Failed'}")
