import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Sort both DataFrames
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Calculate RS and RS Ratio
    # df["RS"] = df["Close"] / index_df["Close"]
    df = df.merge(index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}), on="Timestamp", how="left")
    df["RS"] = df["Close"] / df["Benchmark_Close"]


    # Filter strong RS symbols
    filtered = df[df["RS"] > 0].copy()

    # Drop duplicates to avoid unstack error
    filtered = filtered.drop_duplicates(subset=["Symbol", "Timestamp"])

    # Get last 10 RS values per symbol
    recent_rs = (
        filtered
        .sort_values(["Symbol", "Timestamp"])
        .groupby("Symbol")
        .tail(10)
        .set_index(["Symbol", "Timestamp"])["RS"]
        .unstack()
        .reset_index()
    )

    # Rename columns using actual dates
    recent_rs.columns = ["Symbol"] + [ts.strftime("%d-%b") for ts in recent_rs.columns[1:]]

    return recent_rs
