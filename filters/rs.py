import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    index_df["Timestamp"] = pd.to_datetime(index_df["Timestamp"])

    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Merge Benchmark
    df = df.merge(
        index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}),
        on="Timestamp", how="left"
    )

    # Calculate RS (standard formula)
    df["RS"] = (
        100 * (1 + (df["Close"] / df["Close"].shift(rs_period) - 1)) /
        (1 + (df["Benchmark_Close"] / df["Benchmark_Close"].shift(rs_period) - 1))
    )

    df["Date"] = df["Timestamp"].dt.normalize()

    # Get last RS value per Symbol
    latest_rs = (
        df
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(1)[["Symbol", "RS"]]
    )

    # Filter by latest RS > 105
    strong_symbols = latest_rs[latest_rs["RS"] > 105]["Symbol"]

    # Now go back and collect last 10 RS values only for those symbols
    filtered = df[df["Symbol"].isin(strong_symbols)].copy()

    # One RS per Symbol-Date
    filtered = (
        filtered.sort_values("Timestamp")
        .groupby(["Symbol", "Date"], as_index=False)
        .last()
    )

    # Last 10 RS values per Symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Date"])
        .groupby("Symbol")
        .tail(10)
        .pivot(index="Symbol", columns="Date", values="RS")
        .reset_index()
    )

    # Format column names
    recent_rs.columns = ["Symbol"] + [pd.to_datetime(c).strftime("%d-%b") for c in recent_rs.columns[1:]]

    # Keep only last 10 columns (in case more exist)
    recent_rs = recent_rs[["Symbol"] + recent_rs.columns[-10:].tolist()]

    return recent_rs
