import pandas as pd
import numpy as np

def run_monthly_accumulation_filter(df: pd.DataFrame, cmf_period: int = 6, high_period: int = 12) -> pd.DataFrame:
    df = df.copy()

    # Ensure proper datetime format and sorting
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values(["Symbol", "Timestamp"])

    # CMF Calculation (assumes monthly data is already provided)
    range_diff = df["High"] - df["Low"]
    range_diff = range_diff.replace(0, np.nan)

    mf_multiplier = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / range_diff
    mf_volume = mf_multiplier * df["Volume"]

    df["CMF"] = (
        mf_volume.rolling(window=cmf_period).sum() /
        df["Volume"].rolling(window=cmf_period).sum()
    ).round(2)

    # Normalize to month start
    df["Date"] = df["Timestamp"].dt.normalize()

    # 12-month CMF high per symbol
    df["CMF_12M_High"] = (
        df.groupby("Symbol")["CMF"]
        .transform(lambda x: x.rolling(high_period, min_periods=1).max())
    )

    # Latest CMF value per symbol
    latest_df = (
        df.groupby("Symbol")
        .tail(1)[["Symbol", "CMF", "CMF_12M_High"]]
    )

    # Filter: CMF > 0
    strong_symbols = latest_df[latest_df["CMF"] > 0]["Symbol"]
    filtered = df[df["Symbol"].isin(strong_symbols)].copy()

    # One CMF per Symbol-Date
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Last 10 monthly CMF values per symbol
    recent_cmf = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="CMF")
        .reset_index()
    )

    # Format column names
    recent_cmf.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%b-%Y") for c in recent_cmf.columns[1:]]

    # Limit to last 10 columns + CMF_12M_High
    recent_cmf = recent_cmf[["Symbol"] + recent_cmf.columns[-10:].tolist()]

    # Add CMF_12M_High
    recent_cmf = recent_cmf.merge(
        latest_df[["Symbol", "CMF_12M_High"]],
        on="Symbol", how="left"
    )

    # Add Sector & Mktcap info
    try:
        ref_df = pd.read_excel("data_ref/NSE_Stocks.xlsx", sheet_name=0)
        ref_df = ref_df[["Symbol", "Sector", "Mktcap"]].drop_duplicates()
        recent_cmf = recent_cmf.merge(ref_df, on="Symbol", how="left")
    except Exception as e:
        print("⚠️ Sector/Mktcap merge failed:", e)

    # Reorder columns: Symbol, Sector, Mktcap, Dates..., CMF_12M_High
    cols = recent_cmf.columns.tolist()
    ordered_cols = ["Symbol", "Sector", "Mktcap"] + [c for c in cols if c not in ("Symbol", "Sector", "Mktcap", "CMF_12M_High")] + ["CMF_12M_High"]
    recent_cmf = recent_cmf[ordered_cols]

    return recent_cmf
