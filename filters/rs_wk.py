import pandas as pd

def run_weekly_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 52) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Ensure datetime and sorting
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    index_df["Timestamp"] = pd.to_datetime(index_df["Timestamp"])

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge Benchmark Weekly Close
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Compute RS (Relative Strength)
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    ).round(2)

    df["Date"] = df["Timestamp"].dt.normalize()

    # 52-week RS high per symbol
    df["RS_52W_High"] = (
        df.groupby("Symbol")["RS"]
        .transform(lambda x: x.rolling(rs_period, min_periods=1).max())
    )

    # Latest RS per symbol
    latest_df = (
        df.groupby("Symbol")
        .tail(1)[["Symbol", "RS", "RS_52W_High"]]
    )

    # Filter: RS > 105
    strong_symbols = latest_df[latest_df["RS"] > 105]["Symbol"]
    filtered = df[df["Symbol"].isin(strong_symbols)].copy()

    # Keep only one row per Symbol-Date
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Last 10 RS values per symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="RS")
        .reset_index()
    )

    # Format columns
    recent_rs.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_rs.columns[1:]]
    recent_rs = recent_rs[["Symbol"] + recent_rs.columns[-10:].tolist()]

    # Add RS_52W_High
    recent_rs = recent_rs.merge(
        latest_df[["Symbol", "RS_52W_High"]],
        on="Symbol", how="left"
    )

    return recent_rs
