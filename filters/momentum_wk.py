import pandas as pd
import ta

def run_weekly_momentum_filter(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Sort for consistent indicator calculation
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # Calculate momentum indicators on weekly data
    df['EMA_21'] = ta.trend.ema_indicator(df['Close'], window=21).round(2)
    df['EMA_50'] = ta.trend.ema_indicator(df['Close'], window=50).round(2)
    df['ADX'] = ta.trend.adx(df['High'], df['Low'], df['Close'], window=14).round(2)
    df['RSI'] = ta.momentum.rsi(df['Close'], window=14).round(2)

    # Get latest data per symbol (last row per group)
    latest_df = (
        df.groupby("Symbol")
        .tail(1)
        .copy()
    )

    # Apply filter on latest values
    filtered = latest_df[
        (latest_df['EMA_21'] > latest_df['EMA_50']) &
        (latest_df['ADX'] > 20) &
        (latest_df['RSI'] > 50) & (latest_df['RSI'] < 70)
    ]

    # Return filtered stocks with relevant indicator values
    return filtered[['Symbol', 'EMA_21', 'EMA_50', 'ADX', 'RSI']].reset_index(drop=True)
