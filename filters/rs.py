import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    index_df["Timestamp"] = pd.to_datetime(index_df["Timestamp"])

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge Nifty as benchmark
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Compute RS
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    # Filter stocks with RS > 105
    filtered = df[df["RS"] > 105].copy()
    filtered["Date"] = filtered["Timestamp"].dt.normalize()

    # One RS per Symbol per Date
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Get last 10 RS values per symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="RS")
        .reset_index()
    )

    # Rename columns (after pivoting)
    recent_rs.columns = ["Symbol"] + [d.strftime("%d-%b") for d in recent_rs.columns[1:]]

    # Keep only last 10 dates
    recent_rs = recent_rs[["Symbol"] + recent_rs.columns[-10:].tolist()]

    return recent_rs
