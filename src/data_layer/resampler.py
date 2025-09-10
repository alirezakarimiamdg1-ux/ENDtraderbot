import pandas as pd

def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resamples a DataFrame to a higher timeframe.

    Args:
        df: The input DataFrame with a DatetimeIndex (should be M1 data).
        timeframe: The target timeframe string (e.g., '5min', '15min', '1h', '4h').

    Returns:
        A new DataFrame resampled to the target timeframe.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Input DataFrame must have a DatetimeIndex.")

    # Ensure timezone-awareness (localize to UTC if naive)
    if df.index.tz is None:
        df = df.tz_localize('UTC')
    else:
        df = df.tz_convert('UTC')

    aggregation_rules = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    # We only want to resample columns that are present in the dataframe and in the rules
    cols_to_resample = {k: v for k, v in aggregation_rules.items() if k in df.columns}

    resampled_df = df.resample(timeframe, label='right', closed='right').agg(cols_to_resample)

    # Drop rows where all values are NaN, which happens for periods with no trades
    resampled_df.dropna(how='all', inplace=True)

    return resampled_df

if __name__ == '__main__':
    from pathlib import Path
    # Use relative imports for sibling modules
    from .data_loader import load_csv_data
    from .data_validator import validate_data

    # Load and validate the M1 data
    data_file = Path('XAUUSD_M1_4M_clean.csv')
    m1_df = load_csv_data(data_file)

    if not m1_df.empty and validate_data(m1_df):
        print("\n--- Resampling M1 data ---")

        # Resample to 5 minutes
        print("\nResampling to 5 Minutes (5min)...")
        m5_df = resample_timeframe(m1_df.copy(), '5min')
        print("Original M1 rows for first 5min candle:\n", m1_df.head(5))
        print("\nResulting M5 candle:\n", m5_df.head(1))
        print(f"Shape of M5 DataFrame: {m5_df.shape}")

        # Resample to 1 hour
        print("\nResampling to 1 Hour (1h)...")
        h1_df = resample_timeframe(m1_df.copy(), '1h')
        print("\nResulting H1 candle (first one):")
        print(h1_df.head(1))
        print(f"Shape of H1 DataFrame: {h1_df.shape}")

        # Resample to 4 hours
        print("\nResampling to 4 Hours (4h)...")
        h4_df = resample_timeframe(m1_df.copy(), '4h')
        print("\nResulting H4 candle (first one):")
        print(h4_df.head(1))
        print(f"Shape of H4 DataFrame: {h4_df.shape}")
