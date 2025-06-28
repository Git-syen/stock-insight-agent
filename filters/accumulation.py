import pandas as pd
import ta

def run_accumulation_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['VPT'] = ta.volume.volume_price_trend(df['Close'], df['Volume'])
    df['Volume_MA'] = df['Volume'].rolling(window=20).mean()

    return df[
        (df['VPT'].diff() > 0) &
        (df['Volume'] > df['Volume_MA'])
    ].copy()