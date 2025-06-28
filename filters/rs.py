import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()
    df['RS'] = df['Close'] / index_df['Close']
    df['RS_SMA'] = df['RS'].rolling(window=20).mean()
    df['RS_Ratio'] = df['RS'] / df['RS_SMA']
    return df[df['RS_Ratio'] > 1.05].copy()