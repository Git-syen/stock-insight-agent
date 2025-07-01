import pandas as pd
import numpy as np

def run_accumulation_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Prevent divide-by-zero
    range_diff = df['High'] - df['Low']
    range_diff = range_diff.replace(0, np.nan)

    csv = df['Volume'] * (df['Close'] - df['Open']) / range_diff
    csv_ma = csv.rolling(window=14).mean()
    csv_std = (csv - csv_ma).rolling(window=14).std()

    df['out'] = np.where(
        csv_std != 0,
        ((csv - csv_ma) / csv_std) * (range_diff.rolling(window=14).std()),
        0
    )

    df['MI'] = np.where(df['out'] > 0, df['High'] + df['out'], df['Low'] + df['out'])
    mid_price = (df['High'] + df['Low']) / 2.0
    df['dMI'] = (100 * (df['MI'] - mid_price) / mid_price).round(2)

    df['Date'] = pd.to_datetime(df['Timestamp']).dt.normalize()

    # One dMI per Symbol-Date
    daily_dmi = (
        df.sort_values('Timestamp')
        .groupby(['Symbol', 'Date'], as_index=False)
        .last()[['Symbol', 'Date', 'dMI']]
    )

    # Get last 10 dMI per symbol
    recent_dmi = (
        daily_dmi
        .sort_values(['Symbol', 'Date'])
        .groupby('Symbol')
        .tail(10)
        .pivot(index='Symbol', columns='Date', values='dMI')
        .reset_index()
    )

    # Format column names
    recent_dmi.columns = ['Symbol'] + [pd.to_datetime(c).strftime('%d-%b') for c in recent_dmi.columns[1:]]

    # Keep only last 10 columns
    recent_dmi = recent_dmi[['Symbol'] + recent_dmi.columns[-10:].tolist()]

    # Filter: keep only symbols where latest dMI > 0
    last_date_col = recent_dmi.columns[-1]
    filtered = recent_dmi[recent_dmi[last_date_col] > 0]

    return filtered
