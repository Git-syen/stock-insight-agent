import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Ensure Timestamps are datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    index_df["Timestamp"] = pd.to_datetime(index_df["Timestamp"])

    # Sort
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge benchmark index
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # RS Calculation
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    # Filter for strong RS
    # filtered = df[df["RS"] > 105].copy()
    filtered = df

    # Drop duplicate Symbol-Date combos (e.g., intraday issues)
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Timestamp"], as_index=False)
        .last()
    )

    # Round timestamp to date only
    filtered["Date"] = filtered["Timestamp"].dt.date

    # Keep only latest 10 dates per symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="RS")
        .reset_index()
    )

    # Format date columns
    recent_rs.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_rs.columns[1:]]

    # Limit to last 10 columns
    recent_rs = recent_rs[["Symbol"] + recent_rs.columns[1:][-10:].tolist()]

    return recent_rs
