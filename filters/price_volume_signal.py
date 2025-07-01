import pandas as pd
import numpy as np

def run_price_volume_filter(df: pd.DataFrame, lookback: int = 20, vol_avg_period: int = 14, vol_multiplier: float = 1.5) -> pd.DataFrame:
    df = df.copy()

    # Ensure timestamp is datetime and sorted
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # Calculate moving average of volume
    df["VolumeAvg"] = df.groupby("Symbol")["Volume"].transform(lambda x: x.rolling(vol_avg_period).mean())

    # Calculate volume spike condition
    df["VolSpike"] = df["Volume"] > (df["VolumeAvg"] * vol_multiplier)

    # Calculate rolling breakout/breakdown
    df["HighClose"] = df.groupby("Symbol")["Close"].transform(lambda x: x.shift(1).rolling(lookback).max())
    df["LowClose"] = df.groupby("Symbol")["Close"].transform(lambda x: x.shift(1).rolling(lookback).min())

    df["Breakout"] = (df["Close"] > df["HighClose"]) & df["VolSpike"]
    df["Breakdown"] = (df["Close"] < df["LowClose"]) & df["VolSpike"]

    df["Date"] = df["Timestamp"].dt.normalize()

    # Label signal type
    df["Signal"] = np.select(
      [df["Breakout"], df["Breakdown"]],
      ["Breakout", "Breakdown"],
      default=None
  ).astype(object)

    # Filter for signals only
    signal_df = df[df["Signal"].notna()].copy()

    # One signal per Symbol-Date
    signal_df = (
        signal_df.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Get last 10 signals per symbol
    recent_signals = (
        signal_df
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="Signal")
        .reset_index()
    )

    # Format columns
    recent_signals.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_signals.columns[1:]]

    # Keep only last 10 date columns
    recent_signals = recent_signals[["Symbol"] + recent_signals.columns[-10:].tolist()]

    return recent_signals
