import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame, rs_period: int = 252) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Sort both DataFrames
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Calculate RS and RS Ratio
    #df["RS"] = df["Close"] / index_df["Close"]
    df = df.merge(index_df[["Timestamp", "Close"]].rename(columns={"Close": "Benchmark_Close"}), on="Timestamp", how="left")
    df["RS"] = 100 * (1+(df["Close"] / df["Benchmark_Close"]-1))/(1+(df["Close"].shift(rs_period) / df["Benchmark_Close"].shift(rs_period)-1))


    # Filter strong RS symbols
    #filtered = df[df["RS"] > 0].copy()
    filtered = df

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
    # After you create `recent_rs` as final DataFrame:
    rs_cols = recent_rs.columns[1:]  # skip 'Symbol'
    last_10_cols = rs_cols[-10:]
    recent_rs = recent_rs[["Symbol"] + list(last_10_cols)]


    return recent_rs
