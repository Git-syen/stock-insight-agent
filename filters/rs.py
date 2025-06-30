import pandas as pd

def run_rs_filter(df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    index_df = index_df.copy()

    # Sort both DataFrames
    df = df.sort_values(by=["Symbol", "Timestamp"])
    index_df = index_df.sort_values(by="Timestamp")

    # Assume index_df has unique Timestamp values
    index_close_map = dict(zip(index_df["Timestamp"], index_df["Close"]))

    # Map benchmark Close to stock df
    df["Benchmark_Close"] = df["Timestamp"].map(index_close_map)

    # Calculate RS and RS Ratio
    df["RS"] = df["Close"] / df["Benchmark_Close"]
    df["RS_SMA"] = df.groupby("Symbol")["RS"].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    df["RS_Ratio"] = df["RS"] / df["RS_SMA"]

    # Filter strong RS symbols
    filtered = df[df["RS_Ratio"] > 1.05].copy()

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
