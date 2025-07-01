import pandas as pd
import numpy as np

def run_accumulation_filter(df: pd.DataFrame, cmf_period: int = 20, high_period: int = 52) -> pd.DataFrame:
    df = df.copy()

    # Calculate CMF
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # Avoid divide-by-zero
    range_diff = df["High"] - df["Low"]
    range_diff = range_diff.replace(0, np.nan)

    mf_multiplier = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / range_diff
    mf_volume = mf_multiplier * df["Volume"]

    df["CMF"] = mf_volume.rolling(window=cmf_period).sum() / df["Volume"].rolling(window=cmf_period).sum().round(2)
    df["Date"] = df["Timestamp"].dt.normalize()

    # 52-day CMF high per symbol
    df["CMF_52D_High"] = (
        df.sort_values(["Symbol", "Date"])
        .groupby("Symbol")["CMF"]
        .transform(lambda x: x.rolling(high_period, min_periods=1).max())
    )

    # Latest CMF value per symbol
    latest_df = (
        df.sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(1)[["Symbol", "CMF", "CMF_52D_High"]]
    )

    # Filter: latest CMF > 0
    strong_symbols = latest_df[latest_df["CMF"] > 0]["Symbol"]
    filtered = df[df["Symbol"].isin(strong_symbols)].copy()

    # One CMF per Symbol-Date
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Last 10 CMF values per symbol
    recent_cmf = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="CMF")
        .reset_index()
    )

    # Format column names
    recent_cmf.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_cmf.columns[1:]]
    recent_cmf = recent_cmf[["Symbol"] + recent_cmf.columns[-10:].tolist()]

    # Add CMF_52D_High column
    recent_cmf = recent_cmf.merge(
        latest_df[["Symbol", "CMF_52D_High"]],
        on="Symbol", how="left"
    )

    return recent_cmf
