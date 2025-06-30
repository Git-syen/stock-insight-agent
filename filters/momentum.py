import pandas as pd
import ta

def run_momentum_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['EMA_21'] = ta.trend.ema_indicator(df['Close'], window=21)
    df['EMA_50'] = ta.trend.ema_indicator(df['Close'], window=50)
    df['ADX'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14)
    df['RSI'] = ta.momentum.rsi(df['Close'], window=14)

    # Apply the filter
    filtered = df[
        (df['EMA_21'] > df['EMA_50']) &
        (df['ADX'] > 20) &
        (df['RSI'] > 50) & (df['RSI'] < 70)
    ]

    # Return only unique symbols that passed the filter
    return pd.DataFrame(filtered['Symbol'].unique(), columns=["Symbol"])