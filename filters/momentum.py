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
    ].copy()

    # Drop duplicates to keep latest per Symbol
    filtered = filtered.sort_values("Timestamp").drop_duplicates(subset="Symbol", keep="last")

    # Merge Sector and Mktcap from reference Excel
    try:
        ref_df = pd.read_excel("data_ref/NSE_Stocks.xlsx", sheet_name=0)
        ref_df = ref_df[["Symbol", "Sector", "Mktcap"]].drop_duplicates()
        filtered = filtered.merge(ref_df, on="Symbol", how="left")
    except Exception as e:
        print("⚠️ Sector/Mktcap merge failed:", e)

    # Return final columns
    return filtered[["Symbol", "Sector", "Mktcap", "EMA_21", "EMA_50", "ADX", "RSI"]].reset_index(drop=True)
