import pandas as pd
import ta

def run_momentum_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Calculate indicators
    df['EMA_21'] = ta.trend.ema_indicator(df['Close'], window=21).round(2)
    df['EMA_50'] = ta.trend.ema_indicator(df['Close'], window=50).round(2)
    df['ADX'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14).round(2)
    df['RSI'] = ta.momentum.rsi(df['Close'], window=14).round(2)

    # Apply the filter
    filtered = df[
        (df['EMA_21'] > df['EMA_50']) &
        (df['ADX'] > 20) &
        (df['RSI'] > 50) & (df['RSI'] < 70)
    ]

    # Return the relevant columns for shortlisted stocks
    return filtered[['Symbol', 'EMA_21', 'EMA_50', 'ADX', 'RSI']].drop_duplicates(subset='Symbol').reset_index(drop=True)
