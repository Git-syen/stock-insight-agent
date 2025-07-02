import pandas as pd
import numpy as np


def run_price_volume_filter(df: pd.DataFrame, lookback: int = 14, vol_avg_period: int = 14, vol_multiplier: float = 1.5) -> pd.DataFrame:
    df = df.copy()

    # Ensure Timestamp is datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # Volume spike condition (per symbol)
    df["VolumeAvg"] = df.groupby("Symbol")["Volume"].transform(lambda x: x.rolling(window=vol_avg_period).mean())
    df["VolSpike"] = df["Volume"] > (df["VolumeAvg"] * vol_multiplier)

    # Use pandas rolling max/min
    df["HighClose"] = df.groupby("Symbol")["Close"].transform(lambda x: x.shift(1).rolling(lookback).max())
    df["LowClose"] = df.groupby("Symbol")["Close"].transform(lambda x: x.shift(1).rolling(lookback).min())
   

    df["Breakout"] = (df["Close"] > df["HighClose"]) & df["VolSpike"]
    df["Breakdown"] = (df["Close"] < df["LowClose"]) & df["VolSpike"]

    df["Date"] = df["Timestamp"].dt.normalize()

    # Assign signal
    df["Signal"] = np.select(
        [df["Breakout"], df["Breakdown"]],
        ["Breakout", "Breakdown"],
        default="Other"
    ).astype(object)

    signal_df = df[df["Signal"].notna()].copy()

    # Step 1: Define priority
    priority = {"Breakout": 2, "Breakdown": 1, "Other": 0}

    # Step 2: Map priority
    signal_df["Priority"] = signal_df["Signal"].map(priority).fillna(0).astype(int)

    # Step 3: Sort and keep highest priority signal per Symbol-Date
    signal_df = (
        signal_df.sort_values(["Symbol", "Date", "Priority"], ascending=[True, True, False])
                .drop_duplicates(subset=["Symbol", "Date"], keep="first")
                .drop(columns="Priority")
    )


    # Step 1: Sort and take last 10 signals per symbol
    recent_signals = (
        signal_df
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)  # last 10 signals per Symbol
        .pivot(index="Symbol", columns="Date", values="Signal")
        .reset_index()
    )

    # Step 2: Format column names: keep 'Symbol' and convert Dates to dd-Mon format
    recent_signals.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_signals.columns[1:]]

    # Step 3: Identify only signal columns (excluding Symbol)
    signal_only_cols = recent_signals.columns[1:]

    # Step 4: Count how many of the last 10 signals are 'Breakout'
    recent_signals["BreakoutCount"] = recent_signals[signal_only_cols].apply(
        lambda row: sum(x == "Breakout" for x in row), axis=1
    )

    # Step 5: Keep only rows (symbols) with at least 1 breakout
    recent_signals = recent_signals[recent_signals["BreakoutCount"] > 0]

    return recent_signals

