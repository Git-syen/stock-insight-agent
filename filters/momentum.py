import pandas as pd
import ta

def run_momentum_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['EMA_20'] = ta.trend.ema_indicator(df['Close'], window=20)
    df['EMA_50'] = ta.trend.ema_indicator(df['Close'], window=50)
    df['ADX'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['RSI'] = ta.momentum.rsi(df['Close'], window=14)

    return df[
        (df['EMA_20'] > df['EMA_50']) &
        (df['ADX'] > 20) &
        (df['RSI'] > 50) & (df['RSI'] < 70)
    ].copy()