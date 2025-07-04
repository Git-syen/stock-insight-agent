import pandas as pd
import numpy as np

def run_weekly_price_volume_filter(
    df: pd.DataFrame,
    lookback: int = 10,
    vol_avg_period: int = 10,
    vol_multiplier: float = 1.5
) -> pd.DataFrame:
    df = df.copy()

    # Ensure Timestamp is datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # Calculate rolling average volume per symbol
    df["VolumeAvg"] = df.groupby("Symbol")["Volume"].transform(
        lambda x: x.rolling(window=vol_avg_period).mean()
    )

    # Volume spike condition
    df["VolSpike"] = df["Volume"] > (df["VolumeAvg"] * vol_multiplier)

    # Lookback high and low of previous close
    df["HighClose"] = df.groupby("Symbol")["Close"].transform(
        lambda x: x.shift(1).rolling(lookback).max()
    )
    df["LowClose"] = df.groupby("Symbol")["Close"].transform(
        lambda x: x.shift(1).rolling(lookback).min()
    )

    # Breakout/Breakdown logic
    df["Breakout"] = (df["Close"] > df["HighClose"]) & df["VolSpike"]
    df["Breakdown"] = (df["Close"] < df["LowClose"]) & df["VolSpike"]

    # Clean Date for grouping
    df["Date"] = df["Timestamp"].dt.normalize()

    # Assign signal
    df["Signal"] = np.select(
        [df["Breakout"], df["Breakdown"]],
        ["Breakout", "Breakdown"],
        default="Other"
    ).astype(object)

    # Filter signals only
    signal_df = df[df["Signal"].notna()].copy()

    # Signal priority
    priority = {"Breakout": 2, "Breakdown": 1, "Other": 0}
    signal_df["Priority"] = signal_df["Signal"].map(priority).fillna(0).astype(int)

    # Keep top priority signal per Symbol-Date
    signal_df = (
        signal_df.sort_values(["Symbol", "Date", "Priority"], ascending=[True, True, False])
                 .drop_duplicates(subset=["Symbol", "Date"], keep="first")
                 .drop(columns="Priority")
    )


    # Last 10 signals per symbol
    recent_signals = (
        signal_df
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="Signal")
        .reset_index()
    )

    # Format column names
    recent_signals.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_signals.columns[1:]]

    # Count breakout signals
    signal_cols = recent_signals.columns[1:]
    recent_signals["BreakoutCount"] = recent_signals[signal_cols].apply(
        lambda row: sum(x == "Breakout" for x in row), axis=1
    )

    # Keep only symbols with at least 1 breakout
    recent_signals = recent_signals[recent_signals["BreakoutCount"] > 0]

    return recent_signals
